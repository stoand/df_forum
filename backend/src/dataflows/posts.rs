use crate::forum_minimal::{
    try_recv_contains, Persisted, QueryResult, QueryResultSender, ScopeCollection, POSTS_PER_PAGE,
};

use timely::dataflow::operators::Map;

use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;

pub fn posts_dataflow<'a>(
    manages_sess: &ScopeCollection<'a>,
    query_result_sender: QueryResultSender,
) {
    let manages = manages_sess.map(|(_addr, (id, persisted))| (id, persisted));

    let sessions = manages_sess
        .map(|(addr, (_id, _persisted))| addr)
        .consolidate();

    let view_posts = manages_sess.flat_map(|(addr, (_id, persisted))| {
        if let Persisted::ViewPosts = persisted {
            vec![(addr, 0)]
        } else {
            vec![]
        }
    });

    let view_posts_page = manages_sess.flat_map(|(addr, (_id, persisted))| {
        if let Persisted::ViewPostsPage(page) = persisted {
            vec![(addr, page)]
        } else {
            vec![]
        }
    });

    let sessions_view_posts = view_posts
        .join_map(&sessions.map(|v| (v, v)), |_key, &a, &b| (a, b))
        .map(|(_v0, v1)| (v1, None::<u64>));
    // .inspect(|v| println!("1 -- {:?}", v));

    let sessions_view_posts_page = view_posts_page
        .join_map(&sessions.map(|v| (v, v)), |_key, &a, &b| (a, b))
        .map(|(v0, v1)| (v1, Some(v0)));
    // .inspect(|v| println!("2 -- {:?}", v));

    let sessions_current_page = sessions_view_posts
        .concat(&sessions_view_posts_page)
        .reduce(|_key, inputs, outputs| {
            let mut final_page = None::<u64>;
            let mut found: bool = false;

            for (page, diff) in inputs {
                if *diff > 0 {
                    if let Some(page0) = page {
                        final_page = Some(*page0);
                    }
                    found = true;
                }
            }
            // println!(
            //     "key = {:?}, input = {:?}, output = {:?}",
            //     key, inputs, outputs
            // );

            if found {
                outputs.push((final_page.unwrap_or(0), 1));
            }
        });
    // .inspect(|v| println!("1 -- {:?}", v));

    let post_ids = manages
        .inner
        .map(|((id, persisted), time, diff)| ((time, id, persisted), time, diff))
        .as_collection()
        .flat_map(|(time, id, persisted)| match persisted {
            Persisted::PostTitle(_) => vec![(0, (id, time))],
            _ => vec![],
        });
    // .inspect(|v| println!("2 -- {:?}", v));

    // todo - join all posts for every user+current_page
    let pages_with_zero = sessions_current_page
        .map(|(_user_id, page)| (0, page))
        .consolidate();
    // .inspect(|v| println!("3 -- {:?}", v));

    let page_ids_with_all_post_ids = pages_with_zero
        .join(&post_ids)
        .map(|(_discarded_zero, (page_id, (post_id, post_time)))| (page_id, (post_id, post_time)));
    // .inspect(|v| println!("4 -- {:?}", v));

    let page_ids_with_relevant_post_ids =
        page_ids_with_all_post_ids.reduce(move |page_id, inputs, outputs| {
            // println!(
            //     "key = {:?}, input = {:?}, output = {:?}",
            //     page_id, inputs, outputs
            // );

            let mut sorted = inputs.to_vec();
            sorted.sort_by_key(|((_post_id, time), _diff)| -(*time as isize));
            let items: Vec<u64> = sorted
                .iter()
                .skip((*page_id) as usize * POSTS_PER_PAGE)
                .take(POSTS_PER_PAGE)
                .filter(|((_id, _time), diff)| *diff > 0)
                .map(|((id, _time), _diff)| *id)
                .collect();

            outputs.push((items, 1));
        });
    // .inspect(|v| println!("4.1 -- {:?}", v));

    let page_ids_with_relevant_posts = page_ids_with_relevant_post_ids
        .flat_map(|(page_id, post_ids)| post_ids.into_iter().map(move |post_id| (page_id, post_id)))
        .map(|(page_id, post_id)| (post_id, page_id));
    // .inspect(|v| println!("5.1 -- {:?}", v));

    let post_titles = manages.flat_map(|(id, persisted)| {
        if let Persisted::PostTitle(title) = persisted {
            vec![(id, title)]
        } else {
            vec![]
        }
    });
    // .inspect(|v| println!("title -- {:?}", v));

    let post_bodies = manages.flat_map(|(id, persisted)| {
        if let Persisted::PostBody(body) = persisted {
            vec![(id, body)]
        } else {
            vec![]
        }
    });

    let posts = post_titles.join(&post_bodies);
    // .inspect(|v| println!("5.2 -- {:?}", v));

    let page_posts = page_ids_with_relevant_posts
        .join(&posts)
        .map(|(post_id, (page_id, post))| (page_id, (post_id, post)));

    let _user_posts = sessions_current_page
        .map(|(addr, page_id)| (page_id, addr))
        .join(&page_posts)
        // .inspect(|v| println!("5.3 -- {:?}", v))
        .inspect(
            move |((_page_id, (addr, (post_id, (post_title, post_body)))), _time, diff)| {
                let query_result = if *diff > 0 {
                    QueryResult::AddPost(*post_id, post_title.clone(), post_body.clone())
                } else {
                    QueryResult::DeletePost(*post_id)
                };

                // println!(
                //     "send Query::Posts -- {:?} (addr = {:?})",
                //     query_result, addr
                // );

                query_result_sender
                    .clone()
                    .send((*addr, vec![query_result]))
                    .unwrap();
            },
        );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forum_minimal::ForumMinimal;
    use std::net::SocketAddr;
    use tokio::sync::broadcast;

    #[tokio::test]
    pub async fn test_posts() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new_with_dataflows(
            persisted_sender.clone(),
            query_result_sender,
            posts_dataflow,
        );

        persisted_sender
            .send((
                addr,
                vec![
                    (55, Persisted::ViewPosts, 1),
                    (5, Persisted::PostTitle("Zerg".into()), 1),
                    (5, Persisted::PostBody("Zerg Info".into()), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;
        assert!(try_recv_contains(
            &mut query_result_receiver,
            (addr, vec![QueryResult::PostTitle("Zerg".into())])
        ));
    }
}
