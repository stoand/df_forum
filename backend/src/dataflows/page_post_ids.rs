use crate::forum_minimal::{
    Persisted, QueryResult, QueryResultSender, ScopeCollection, POSTS_PER_PAGE,
};
use log::debug;
use std::collections::HashMap;
use std::net::SocketAddr;

use timely::dataflow::operators::Map;

use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;

pub fn posts_post_ids_dataflow<'a>(
    collection: &ScopeCollection<'a>,
    query_result_sender: QueryResultSender,
) {
    let session_pages = collection
        .reduce(|_addr, inputs, outputs| {
            // debug!(
            //     "addr(key) = {:?}, input = {:?}, output = {:?}",
            //     addr, inputs, outputs
            // );

            let mut page = None;

            for ((_id, persisted), diff) in inputs {
                if *diff > 0 {
                    match persisted {
                        Persisted::ViewPosts => {
                            if page == None {
                                page = Some(0);
                            }
                        }
                        Persisted::ViewPostsPage(view_page) => {
                            page = Some(*view_page);
                        }
                        _ => {}
                    }
                }
            }

            if let Some(page) = page {
                outputs.push((page, 1));
            }
        })
        .inspect(|v| debug!("session pages -- {:?}", v));

    let post_ids_with_time = collection
        .inner
        .map(|((addr, (id, persisted)), time, diff)| ((time, addr, id, persisted), time, diff))
        .as_collection()
        .flat_map(|(time, addr, id, persisted)| match persisted {
            Persisted::Post => vec![(0, (addr, id, time))],
            _ => vec![],
        });

    // we don't care who created the posts
    let page_posts = post_ids_with_time
        .reduce(|_discarded_zero, inputs, outputs| {
            debug!("input = {:?}, output = {:?}", inputs, outputs);

            let mut vals = inputs
                .to_vec()
                .into_iter()
                .filter(|(_, diff)| *diff > 0)
                .collect::<Vec<_>>();

            vals.sort_by_key(|((_addr, _id, time), _diff)| -(*time as isize));

            let mut page_item_index: u64 = 0;
            let mut page = 0;

            for ((addr, id, creation_time), _diff) in vals {
                outputs.push(((*addr, *id, page, *creation_time), 1));
                page_item_index += 1;
                if page_item_index >= POSTS_PER_PAGE as u64 {
                    page_item_index = 0;
                    page += 1;
                }
            }
        })
        .map(|(_discarded_zero, (addr, id, page, creation_time))| (page, (addr, id, creation_time)))
        .inspect(|v| debug!("page posts -- {:?}", v));

    let session_posts = session_pages
        .map(|(addr, page)| (page, addr))
        .join::<_, isize>(&page_posts);

    let session_post_results = session_posts
        .inner
        .map(
            move |((page, (session_addr, (_addr, id, creation_time))), time, diff)| {
                let query_result = if diff > 0 {
                    vec![(session_addr, QueryResult::PagePost(id, page, creation_time))]
                } else {
                    vec![(session_addr, QueryResult::DeletePost(id))]
                };
                debug!("session posts -- {:?}", query_result);

                (query_result, time, diff)
            },
        )
        .as_collection();

    let session_post_ids =
        session_posts.map(|(_page, (session_addr, (_addr, id, _time)))| (id, session_addr));

    let session_post_field_results = collection
        .map(|(creator_addr, (id, persisted))| (id, (creator_addr, persisted)))
        .join::<_, isize>(&session_post_ids)
        .inner
        .map(
            move |((id, ((_creator_addr, persisted), session_addr)), time, diff)| {
                if diff > 0 {
                    let query_result = match persisted {
                        Persisted::PostTitle(title) => {
                            vec![(session_addr, QueryResult::PostTitle(id, title.clone()))]
                        }
                        Persisted::PostBody(body) => {
                            vec![(session_addr, QueryResult::PostBody(id, body.clone()))]
                        }
                        _ => vec![],
                    };

                    (query_result, time, diff)

                    // if let Some(query_result) = query_result {
                    // }
                } else {
                    (vec![], time, diff)
                }
            },
        )
        .as_collection()
        .inspect(|v| debug!("session post fields -- {:?}", v));

    // Send everything at once to prevent flickering (but still split by session)
    let _batch_output = session_post_field_results
        .concat(&session_post_results)
        .consolidate()
        .inspect_batch(move |_time, query_results_aug| {
            let mut sessions: HashMap<SocketAddr, Vec<QueryResult>> = HashMap::new();

            let query_results = query_results_aug
                .to_vec()
                .into_iter()
                .map(|(qr, _time, _diff)| qr)
                .flatten()
                .collect::<Vec<_>>();

            // Break apart query_results by session

            for (session_addr, query_result) in query_results {
                if None == sessions.get(&session_addr) {
                    sessions.insert(session_addr, Vec::new());
                }
                sessions
                    .get_mut(&session_addr)
                    .expect("session not found")
                    .push(query_result);
            }

            for (session_addr, query_results) in sessions.iter() {
                query_result_sender
                    .clone()
                    .send((*session_addr, query_results.clone()))
                    .unwrap();
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forum_minimal::ForumMinimal;
    use std::net::SocketAddr;
    use tokio::sync::broadcast;

    #[tokio::test]
    pub async fn test_page_post_ids() {
        crate::init_logger();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new_with_dataflows(
            persisted_sender.clone(),
            query_result_sender,
            posts_post_ids_dataflow,
        );

        persisted_sender
            .send((
                addr,
                vec![
                    (55, Persisted::ViewPostsPage(1), 1),
                    (5, Persisted::Post, 1),
                    (5, Persisted::PostTitle("Zerg".into()), 1),
                    (5, Persisted::PostBody("Zerg Info".into()), 1),
                    (6, Persisted::Post, 1),
                    (6, Persisted::PostTitle("Terran".into()), 1),
                    (6, Persisted::PostBody("Terran Info".into()), 1),
                    (7, Persisted::Post, 1),
                    (7, Persisted::PostTitle("Protoss".into()), 1),
                    (7, Persisted::PostBody("Protoss Info".into()), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::PagePost(7, 1, 0),
                    QueryResult::PostTitle(7, "Protoss".into()),
                    QueryResult::PostBody(7, "Protoss Info".into()),
                ]
            ))
        );

        persisted_sender
            .send((
                addr,
                vec![
                    (55, Persisted::ViewPostsPage(1), -1),
                    (55, Persisted::ViewPostsPage(0), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::DeletePost(7),
                    QueryResult::PagePost(5, 0, 0),
                    QueryResult::PagePost(6, 0, 0),
                    QueryResult::PostTitle(5, "Zerg".into()),
                    QueryResult::PostTitle(6, "Terran".into()),
                    QueryResult::PostBody(5, "Zerg Info".into()),
                    QueryResult::PostBody(6, "Terran Info".into()),
                ]
            ))
        );
    }
}
