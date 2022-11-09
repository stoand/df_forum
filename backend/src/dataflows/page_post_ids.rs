use crate::forum_minimal::{
    Persisted, QueryResult, QueryResultSender, ScopeCollection, POSTS_PER_PAGE,
};
use log::debug;

use timely::dataflow::operators::Map;

use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;

pub fn posts_post_ids_dataflow<'a>(
    collection: &ScopeCollection<'a>,
    query_result_sender: QueryResultSender,
) {
    let sessions = collection
        .map(|(addr, (_id, _persisted))| addr)
        .consolidate();

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

            // let pages = HashMap::new();

            let (_, pages) = inputs[0];

            let mut vals = inputs
                .to_vec()
                .into_iter()
                .filter(|(_, diff)| *diff > 0)
                .collect::<Vec<_>>();
            // .sort_by_key(|((addr, id, time), diff)| -(*time as isize));
            vals.sort_by_key(|((_addr, _id, time), _diff)| -(*time as isize));

            let mut page_item_index: u64 = 0;
            let mut page = 0;

            for ((addr, id, _time), _diff) in vals {
                outputs.push(((*addr, *id, page, page_item_index), 1));
                page_item_index += 1;
                if page_item_index >= POSTS_PER_PAGE as u64 {
                    page_item_index = 0;
                    page += 1;
                }
            }
        })
        .map(|(_discarded_zero, (addr, id, page, page_item_index))| {
            (page, (addr, id, page_item_index))
        })
        .inspect(|v| debug!("page posts -- {:?}", v));

    let query_result_sender0 = query_result_sender.clone();

    let session_posts = session_pages
        .map(|(addr, page)| (page, addr))
        .join::<_, isize>(&page_posts)
        .inspect(
            move |((page, (session_addr, (_addr, id, page_item_index))), _time, _diff)| {
                query_result_sender0
                    .clone()
                    .send((
                        *session_addr,
                        vec![QueryResult::PagePost(*id, *page, *page_item_index)],
                    ))
                    .unwrap();
            },
        )
        .inspect(|v| debug!("session posts -- {:?}", v));

    let session_post_ids = session_posts
        .map(|(_page, (session_addr, (_addr, id, _page_item_index)))| (id, session_addr));

    let query_result_sender1 = query_result_sender.clone();

    let session_post_fields = collection
        .map(|(creator_addr, (id, persisted))| (id, (creator_addr, persisted)))
        .join::<_, isize>(&session_post_ids)
        .map(move |(id, ((creator_addr, persisted), session_addr))| {
            let query_result = match persisted {
                Persisted::PostTitle(title) => Some(QueryResult::PostTitle(id, title)),
                Persisted::PostBody(body) => Some(QueryResult::PostBody(id, body)),
                _ => None,
            };

            if let Some(query_result) = query_result {
                query_result_sender1
                    .clone()
                    .send((session_addr, vec![query_result]))
                    .unwrap();
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forum_minimal::{try_recv_contains, ForumMinimal};
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

        assert!(try_recv_contains(
            &mut query_result_receiver,
            (
                addr,
                vec![
                    QueryResult::PagePost(7, 1, 0),
                    // QueryResult::PostTitle(7, "Protoss".into()),
                    // QueryResult::PostBody(7, "Protoss Info".into()),
                ]
            )
        ));

        // persisted_sender
        //     .send((
        //         addr,
        //         vec![
        //             (55, Persisted::ViewPostsPage(1), -1),
        //             (55, Persisted::ViewPostsPage(0), 1),
        //         ],
        //     ))
        //     .unwrap();

        // forum_minimal.advance_dataflow_computation_once().await;
    }
}
