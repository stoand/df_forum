use crate::forum_minimal::{
    Persisted, QueryResult, QueryResultSender, ScopeCollection, POSTS_PER_PAGE,
};
use log::debug;

use timely::dataflow::operators::Map;

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
            Persisted::PostTitle(_) => vec![(0, (addr, id, time))],
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

            for ((addr, id, _time), _diff) in vals {
                outputs.push(((*addr, *id, page), 1));
                page_item_index += 1;
                if page_item_index >= POSTS_PER_PAGE as u64 {
                    page_item_index = 0;
                    page += 1;
                }
            }
        })
        .map(|(_discarded_zero, (addr, id, page))| {
            (page, (addr, id))
        })
        .inspect(|v| debug!("page posts -- {:?}", v));

    let query_result_sender0 = query_result_sender.clone();

    let session_posts = session_pages
        .map(|(addr, page)| (page, addr))
        .join::<_, isize>(&page_posts)
        .inspect(
            move |((page, (session_addr, (_addr, id))), time, diff)| {
                let query_result = if *diff > 0 {
                    QueryResult::PagePost(*id, *page, *time)
                } else {
                    QueryResult::DeletePost(*id)
                };
                debug!("session posts -- {:?}", query_result);

                query_result_sender0
                    .clone()
                    .send((*session_addr, vec![query_result]))
                    .unwrap();
            },
        );

    let session_post_ids = session_posts
        .map(|(_page, (session_addr, (_addr, id)))| (id, session_addr));
    
    let deleted_posts = session_post_ids
        .reduce(|_id, inputs, outputs| {
            let mut all_inputs_remove = true;

            for (addr, diff) in inputs {
                if *diff > 0 {
                    all_inputs_remove = false;
                }
            }

            if all_inputs_remove {
                for (&addr, _diff) in inputs {
                    outputs.push((addr, -1));
                }
            }
        })
        .inspect(|v| debug!("deleted_posts -- {:?}", v));

    let query_result_sender1 = query_result_sender.clone();

    let _session_post_fields = collection
        .map(|(creator_addr, (id, persisted))| (id, (creator_addr, persisted)))
        .join::<_, isize>(&session_post_ids)
        .inspect(
            move |((id, ((_creator_addr, persisted), session_addr)), _time, diff)| {
                if *diff > 0 {
                    let query_result = match persisted {
                        Persisted::PostTitle(title) => {
                            Some(QueryResult::PostTitle(*id, title.clone()))
                        }
                        Persisted::PostBody(body) => Some(QueryResult::PostBody(*id, body.clone())),
                        _ => None,
                    };

                    if let Some(query_result) = query_result {
                        query_result_sender1
                            .clone()
                            .send((*session_addr, vec![query_result]))
                            .unwrap();
                    }
                }
            },
        )
        .inspect(|v| debug!("session post fields -- {:?}", v));
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

        let mut tr = || query_result_receiver.try_recv();

        assert_eq!(tr(), Ok((addr, vec![QueryResult::PagePost(7, 1, 0)])));

        assert_eq!(
            tr(),
            Ok((addr, vec![QueryResult::PostTitle(7, "Protoss".into())]))
        );

        assert_eq!(
            tr(),
            Ok((addr, vec![QueryResult::PostBody(7, "Protoss Info".into())]))
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

        assert_eq!(tr(), Ok((addr, vec![QueryResult::PagePost(5, 0, 2)])));

        assert_eq!(tr(), Ok((addr, vec![QueryResult::PagePost(6, 0, 2)])));

        assert_eq!(tr(), Ok((addr, vec![QueryResult::DeletePost(7)])));

        assert_eq!(
            tr(),
            Ok((addr, vec![QueryResult::PostTitle(5, "Zerg".into())]))
        );
        assert_eq!(
            tr(),
            Ok((addr, vec![QueryResult::PostBody(5, "Zerg Info".into())]))
        );

        assert_eq!(
            tr(),
            Ok((addr, vec![QueryResult::PostTitle(6, "Terran".into())]))
        );
        assert_eq!(
            tr(),
            Ok((addr, vec![QueryResult::PostBody(6, "Terran Info".into())]))
        );
    }
}
