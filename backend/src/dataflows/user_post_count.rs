use crate::forum_minimal::{OutputScopeCollection, Persisted, QueryResult, ScopeCollection};
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use log::debug;
use timely::dataflow::operators::Filter;
use timely::dataflow::operators::Map;

pub fn user_post_count_dataflow<'a>(collection: &ScopeCollection<'a>) -> OutputScopeCollection<'a> {
    // get addrs with same user id as current session addr

    let posts_plus_one = collection.flat_map(|(addr, (post_id, persisted))| {
        if Persisted::Post == persisted {
            vec![(addr, post_id)]
        } else if Persisted::Session == persisted {
            vec![(addr, 0)]
        } else {
            vec![]
        }
    });
    // .inner
    // .filter(|(_, _time, diff)| *diff > 0)
    // .as_collection();

    let session_addr_to_user = collection.flat_map(|(addr, (user_id, persisted))| {
        if Persisted::Session == persisted {
            vec![(addr, user_id)]
        } else {
            vec![]
        }
    });

    let session_user_to_addr = session_addr_to_user.map(|(addr, user_id)| (user_id, addr));

    let posts = collection.flat_map(|(addr, (post_id, persisted))| {
        if Persisted::Post == persisted {
            vec![(addr, post_id)]
        } else {
            vec![]
        }
    });

    let posts_and_creator_user_ids = posts
        .join(&session_addr_to_user)
        .map(|(addr, (post_id, user_id))| (post_id, (user_id, addr)))
        .reduce(|post_id, inputs, output| {
            debug!("post_id: {}, v : {:?}", post_id, inputs);

            let mut found_removal = false;

            'outer: for ((_rm_user_id, _rm_addr), rm_diff) in inputs {
                if *rm_diff < 0 {
                    found_removal = true;
                    for ((add_user_id, add_addr), add_diff) in inputs {
                        if *add_diff > 0 {
                            output.push(((*add_user_id), -1));
                            continue 'outer;
                        }
                    }
                }
            }

            if !found_removal {
                for ((add_user_id, add_addr), add_diff) in inputs {
                    if *add_diff > 0 {
                        output.push(((*add_user_id), 1));
                    }
                }
            }
        })
        .inspect(|v| debug!("v : {:?}", v));

    let results = posts_plus_one
        .join(&session_addr_to_user)
        .map(|(_addr, (post_id, user_id))| (user_id, post_id))
        .reduce(|user_id, inputs, outputs| {
            // debug!("user id: {}, inputs: {:?}", user_id, inputs);

            outputs.push((inputs.len() - 1, 1));
        })
        .join(&session_user_to_addr)
        // .inspect(|v| debug!("v : {:?}", v))
        .inner
        .map(|((_user_id, (count, addr)), time, diff)| {
            let result = if diff > 0 {
                vec![(addr, QueryResult::UserPostCount(count as u64))]
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
    pub async fn test_user_post_count() {
        crate::init_logger();
        let addr0: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr1: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new_with_dataflows(
            persisted_sender.clone(),
            query_result_sender,
            user_post_count_dataflow,
        );

        persisted_sender
            .send((
                addr0,
                vec![
                    (55, Persisted::Session, 1),
                    (5, Persisted::Post, 1),
                    (6, Persisted::Post, 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr0, vec![QueryResult::UserPostCount(2)])),
        );

        persisted_sender
            .send((addr2, vec![(55, Persisted::Session, 1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr2, vec![QueryResult::UserPostCount(2)])),
        );

        persisted_sender
            .send((
                addr1,
                vec![
                    (56, Persisted::Session, 1),
                    (5, Persisted::Post, -1),
                    // (6, Persisted::Post, -1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr1, vec![QueryResult::UserPostCount(0)])),
        );
    }
}
