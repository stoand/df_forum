use crate::forum_minimal::{
    Persisted, QueryResult, QueryResultSender, ScopeCollection, POSTS_PER_PAGE,
};
use log::debug;

use std::collections::HashMap;
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
        .reduce(|addr, inputs, outputs| {
            debug!(
                "addr(key) = {:?}, input = {:?}, output = {:?}",
                addr, inputs, outputs
            );

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
        .map(|(_addr, page)| (0, page))
        .reduce(|_discarded_zero, inputs, outputs| {
            debug!(
                "session pages - input = {:?}, output = {:?}",
                inputs, outputs
            );

            let out = inputs
                .iter()
                .filter(|(_, diff)| *diff > 0)
                .map(|(page, _diff)| **page)
                .collect::<Vec<_>>();

            let mut copy = Vec::new();
            for item in out {
                copy.push(item.clone());
            }
            outputs.push((copy, 1));
        })
        // .map(|(_discarded_zero, pages)| pages)
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
        .join(&session_pages)
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
            vals.sort_by_key(|(((_addr, _id, time), _), _diff)| -(*time as isize));

            // for input in inputs {
            //     if let ((addr, id, time), diff) = input {
            //         if *diff > 0 {
            //         }
            //     }
            // }

            let mut out = Vec::new();

            for (((addr, id, time), pages), _diff) in inputs {
                out.push((addr.clone(), id.clone(), time.clone()));
            }

            outputs.push((out, 1));
        })
        .map(|(_discarded_zero, items)| items)
        .inspect(|v| debug!("page posts -- {:?}", v));
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
                vec![QueryResult::PagePostIds {
                    post_ids: vec![7, 6],
                    page_count: 2
                }]
            )
        ));

        // TODO: ViewPostsPage
    }
}
