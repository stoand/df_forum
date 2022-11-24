use crate::forum_minimal::{
    batch_send, Persisted, QueryResult, QueryResultSender, ScopeCollection, POSTS_PER_PAGE,
};
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Count;
use differential_dataflow::operators::Join;
use differential_dataflow::AsCollection;
use log::debug;
use timely::dataflow::operators::Map;

pub fn post_aggr_dataflow<'a>(
    manages_sess: &ScopeCollection<'a>,
    query_result_sender: QueryResultSender,
) {
    let manages = manages_sess.map(|(_addr, (id, persisted))| (id, persisted));

    let sessions_with_zero = manages_sess
        .filter(|(_addr, (_, persisted))| {
            if let Persisted::ViewPostsPage(_) = persisted {
                true
            } else {
                false
            }
        })
        .map(|(addr, _)| (0, addr))
        .consolidate();

    let post_aggregates_result = manages
        .flat_map(|(_id, persisted)| {
            if let Persisted::Post = persisted {
                vec![0]
            } else {
                // add an additional count so that counting to zero is possible
                if let Persisted::ViewPostsPage(_) = persisted {
                    vec![0]
                } else {
                    vec![]
                }
            }
        })
        .count()
        .inspect(|v| debug!("val {:?}", v))
        .map(|v| (0, v))
        .join(&sessions_with_zero)
        .inner
        .map(|((_zero, ((_, count), addr)), time, diff)| {
            debug!("count {:?}", count);
            let actual_count = count - 1;
            let mut page_count = ((actual_count as f64) / (POSTS_PER_PAGE as f64)).ceil() as u64;
            if page_count < 1 {
                page_count = 1;
            }

            let result = if diff > 0 {
                vec![(
                    addr,
                    QueryResult::PostAggregates(actual_count as u64, page_count as u64),
                )]
            } else {
                vec![]
            };

            (result, time, diff)
        })
        .as_collection();

    // .inspect_batch(move |_time, items| {
    //     let mut addrs = Vec::new();
    //     let mut final_count = 0;

    //     for (((_discarded_zero, count), viewer_addr), _time, diff) in items {
    //         addrs.push(viewer_addr);
    //         if *diff > 0 {
    //             final_count = *count as u64;
    //         }
    //     }

    //     let mut page_count = ((final_count as f64) / (POSTS_PER_PAGE as f64)).ceil() as u64;
    //     if page_count < 1 {
    //         page_count = 1;
    //     }

    //     for addr in addrs {
    //         query_result_sender_loop
    //             .clone()
    //             .send((
    //                 *addr,
    //                 vec![QueryResult::PostAggregates(final_count, page_count)],
    //             ))
    //             .unwrap();
    //     }
    // });

    let _batch_output = post_aggregates_result
        .inspect_batch(move |_time, aug| batch_send(aug, &query_result_sender));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forum_minimal::ForumMinimal;
    use std::net::SocketAddr;
    use tokio::sync::broadcast;

    #[tokio::test]
    pub async fn test_post_aggr_total() {
        crate::init_logger();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new_with_dataflows(
            persisted_sender.clone(),
            query_result_sender,
            post_aggr_dataflow,
        );

        persisted_sender
            .send((
                addr,
                vec![
                    (55, Persisted::ViewPostsPage(0), 1),
                    (5, Persisted::Post, 1),
                    (6, Persisted::Post, 1),
                    (7, Persisted::Post, 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr, vec![QueryResult::PostAggregates(3, 2)]))
        );

        persisted_sender
            .send((addr, vec![(5, Persisted::Post, -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr, vec![QueryResult::PostAggregates(2, 1)]))
        );

        persisted_sender
            .send((
                addr,
                vec![(6, Persisted::Post, -1), (7, Persisted::Post, -1)],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr, vec![QueryResult::PostAggregates(0, 1)]))
        );
    }

    // #[tokio::test]
    // pub async fn test_post_aggr_likes() {
    //     crate::init_logger();
    //     let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    //     let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
    //     let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

    //     let mut forum_minimal = ForumMinimal::new_with_dataflows(
    //         persisted_sender.clone(),
    //         query_result_sender,
    //         post_aggr_dataflow,
    //     );

    //     persisted_sender
    //         .send((
    //             addr,
    //             vec![
    //                 (55, Persisted::ViewPostsPage(0), 1),
    //                 (5, Persisted::Post, 1),
    //                 (6, Persisted::Post, 1),
    //                 (7, Persisted::Post, 1),
    //             ],
    //         ))
    //         .unwrap();

    //     forum_minimal.advance_dataflow_computation_once().await;

    //     assert_eq!(
    //         query_result_receiver.try_recv(),
    //         Ok((addr, vec![QueryResult::PostAggregates(3, 2)]))
    //     );
    // }
}
