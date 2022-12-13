use crate::forum_minimal::{OutputScopeCollection, Persisted, QueryResult, ScopeCollection};
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use timely::dataflow::operators::Filter;
use timely::dataflow::operators::Map;

use crate::dataflows::shared_post_pages;
use log::debug;

pub fn post_total_likes_dataflow<'a>(
    collection: &ScopeCollection<'a>,
) -> OutputScopeCollection<'a> {
    let post_pages = shared_post_pages(&collection)
        .map(|(_creator_addr, post_id, page, _position)| (page, post_id));

    let page_to_viewer_addr = collection
        .flat_map(|(viewer_addr, (_user_id, persisted))| {
            if let Persisted::ViewPostsPage(page) = persisted {
                vec![(page, viewer_addr)]
            } else {
                vec![]
            }
        })
        .inner
        .filter(|(_, _time, diff)| *diff > 0)
        .as_collection()
        .inspect(|v| debug!("current page -- {:?}", v));

    let post_like_counts = collection
        .flat_map(|(_addr, (_user_id, persisted))| {
            // TODO
            if let Persisted::PostLike(post_id, liked) = persisted {
                vec![(post_id, liked)]
            // add an additional count so that counting to zero is possible
            // } else if Persisted::PlusOneDummy == persisted {
            //     vec![(0, ())]
            } else {
                vec![]
            }
        })
        .reduce(|post_id, inputs, outputs| {
            debug!("post_id: {}, inputs: {:?}", post_id, inputs);

            let result = if inputs.len() == 1 {
                inputs[0].1
            } else {
                // [(false, n), (true, m)
                inputs[1].1 - inputs[0].1
            };
            // outputs.push((inputs[0].1 - 1, 1));
            outputs.push((result, 1));
        })
        // .filter(|(post_id, _count)| *post_id != 0)
        .inspect(|v| debug!("like_counts -- {:?}", v));

    let result = post_pages
        .join(&page_to_viewer_addr)
        .map(|(page, (post_id, viewer_addr))| (post_id, (page, viewer_addr)))
        .inspect(|v| debug!("map -- {:?}", v))
        .join(&post_like_counts)
        .inner
        .map(|((post_id, ((_page, addr), count)), time, diff)| {
            let result = if diff > 0 {
                vec![(addr, QueryResult::PostTotalLikes(post_id, count as u64))]
            } else {
                vec![]
            };

            (result, time, diff)
        })
        .as_collection();

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forum_minimal::ForumMinimal;
    use std::net::SocketAddr;
    use tokio::sync::broadcast;

    #[tokio::test]
    pub async fn test_post_total_likes() {
        crate::init_logger();
        let addr0: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        // let addr1: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new_with_dataflows(
            persisted_sender.clone(),
            query_result_sender,
            post_total_likes_dataflow,
        );

        persisted_sender
            .send((
                addr0,
                vec![
                    (55, Persisted::ViewPostsPage(0), 1),
                    (5, Persisted::Post, 1),
                    (6, Persisted::Post, 1),
                    (55, Persisted::PostLike(5, true), 1),
                    (56, Persisted::PostLike(5, true), 1),
                    (57, Persisted::PostLike(5, true), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr0, vec![QueryResult::PostTotalLikes(5, 3)])),
        );
    }
}
