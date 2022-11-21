use crate::forum_minimal::{
    Persisted, QueryResult, QueryResultSender, ScopeCollection, POSTS_PER_PAGE,
};
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Count;
use differential_dataflow::operators::Join;

pub fn post_aggr_dataflow<'a>(
    manages_sess: &ScopeCollection<'a>,
    query_result_sender: QueryResultSender,
) {
    let manages = manages_sess.map(|(_addr, (id, persisted))| (id, persisted));
    let query_result_sender_loop = query_result_sender.clone();

    let sessions_with_zero = manages_sess
        .filter(|(_addr, (_, persisted))| {
            if let Persisted::Session(_) = persisted {
                true
            } else {
                false
            }
        })
        .map(|(addr, _)| (0, addr))
        .consolidate();
    let _post_aggregates = manages
        .flat_map(|(_id, persisted)| {
            if let Persisted::Post = persisted {
                vec![0]
            } else {
                vec![]
            }
        })
        .count()
        .map(|v| (0, v))
        .join(&sessions_with_zero)
        .map(|(_zero, v)| v)
        .inspect_batch(move |_time, items| {
            let mut addrs = Vec::new();
            let mut final_count = 0;

            for (((_discarded_zero, count), viewer_addr), _time, diff) in items {
                addrs.push(viewer_addr);
                if *diff > 0 {
                    final_count = *count as u64;
                }
            }

            let mut page_count = ((final_count as f64) / (POSTS_PER_PAGE as f64)).ceil() as u64;
            if page_count < 1 {
                page_count = 1;
            }

            for addr in addrs {
                query_result_sender_loop
                    .clone()
                    .send((
                        *addr,
                        vec![QueryResult::PostAggregates(final_count, page_count)],
                    ))
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
    pub async fn test_post_aggr() {
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
            .send((
                addr,
                vec![
                    (5, Persisted::Post, -1),
                    (6, Persisted::Post, -1),
                    (7, Persisted::Post, -1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr, vec![QueryResult::PostAggregates(0, 1)]))
        );
    }
}
