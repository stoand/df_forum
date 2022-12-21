use crate::forum_minimal::{OutputScopeCollection, Persisted, QueryResult, ScopeCollection};
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use log::debug;
use timely::dataflow::operators::Map;

pub fn user_like_count_dataflow<'a>(collection: &ScopeCollection<'a>) -> OutputScopeCollection<'a> {
    let likes_plus_one = collection.flat_map(|(_addr, (user_id, persisted))| {
        if let Persisted::PostLike(post_id, true) = persisted {
            vec![(post_id, user_id)]
        } else if Persisted::Session == persisted {
            vec![(0, user_id)]
        } else {
            vec![]
        }
    });

    let posts = collection.flat_map(|(_addr, (post_id, persisted))| {
        if Persisted::Post == persisted {
            vec![(post_id, ())]
        } else if Persisted::Session == persisted {
            vec![(0, ())]
        } else {
            vec![]
        }
    });    

    let session_addr_to_user = collection.flat_map(|(addr, (user_id, persisted))| {
        if Persisted::Session == persisted {
            vec![(addr, user_id)]
        } else {
            vec![]
        }
    });

    let session_user_to_addr = session_addr_to_user.map(|(addr, user_id)| (user_id, addr));

    let results = likes_plus_one
        .join(&posts)
        .map(|(post_id, (user_id, ()))| (user_id, post_id))
        .reduce(|user_id, inputs, outputs| {
            debug!("user id: {}, inputs: {:?}", user_id, inputs);

            outputs.push((inputs.len() - 1, 1));
        })
        .join(&session_user_to_addr)
        .inspect(|v| debug!("v : {:?}", v))
        .inner
        .map(|((_user_id, (count, addr)), time, diff)| {
            let result = if diff > 0 {
                vec![(addr, QueryResult::UserLikeCount(count as u64))]
            } else {
                vec![]
            };

            (result, time, diff)
        })
        .as_collection();

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forum_minimal::ForumMinimal;
    use std::net::SocketAddr;
    use tokio::sync::broadcast;

    #[tokio::test]
    pub async fn test_user_like_count() {
        crate::init_logger();
        let addr0: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        // let addr1: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new_with_dataflows(
            persisted_sender.clone(),
            query_result_sender,
            user_like_count_dataflow,
        );

        persisted_sender
            .send((
                addr0,
                vec![
                    (55, Persisted::Session, 1),
                    (5, Persisted::Post, 1),
                    (55, Persisted::PostLike(5, true), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr0, vec![QueryResult::UserLikeCount(1)])),
        );

        persisted_sender
            .send((
                addr0,
                vec![
                    (5, Persisted::Post, -1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr0, vec![QueryResult::UserLikeCount(0)])),
        );
    }
}
