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

    let manages = manages_sess.map(|(_addr, (id, persisted))| (id, persisted));
    let query_result_sender_loop = query_result_sender.clone();

    let sessions_with_zero = manages_sess.map(|(addr, _)| (0, addr)).consolidate();
    let _post_aggregates = manages
        .flat_map(|(_id, persisted)| {
            if let Persisted::PostTitle(_) = persisted {
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

            let page_count = ((final_count as f64) / (POSTS_PER_PAGE as f64)).ceil() as u64;

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
