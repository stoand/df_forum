use crate::forum_minimal::{OutputScopeCollection, Persisted, QueryResult, ScopeCollection};
use differential_dataflow::operators::Join;
use differential_dataflow::AsCollection;
use log::debug;
use timely::dataflow::operators::Map;

pub fn post_liked_by_user_dataflow<'a>(
    collection: &ScopeCollection<'a>,
) -> OutputScopeCollection<'a> {
    let session_name_to_addr = collection.flat_map(|(addr, (_id, persisted))| {
        if let Persisted::Session(session_name) = persisted {
            vec![(session_name, addr)]
        } else {
            vec![]
        }
    });
    let session_addr_to_name = collection.flat_map(|(addr, (_id, persisted))| {
        if let Persisted::Session(session_name) = persisted {
            vec![(addr, session_name)]
        } else {
            vec![]
        }
    });

    let collection_with_session_name = collection
        .join(&session_addr_to_name)
        .map(|(addr, ((_id, persisted), session_name))| (session_name, (persisted, addr)))
        .join(&session_name_to_addr)
        .inspect(|v| debug!("val: {:?}", v));

    let result = collection_with_session_name
        .inner
        .map(
            move |((_session_name, ((persisted, _addr), session_addr)), time, diff)| {
                let result = if let Persisted::PostLike(post_id) = persisted {
                    vec![(
                        session_addr,
                        QueryResult::PostLikedByUser(post_id, diff > 0),
                    )]
                } else {
                    vec![]
                };

                (result, time, diff)
            },
        )
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
    pub async fn test_post_liked_by_user() {
        crate::init_logger();
        let addr0: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr1: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new_with_dataflows(
            persisted_sender.clone(),
            query_result_sender,
            post_liked_by_user_dataflow,
        );

        persisted_sender
            .send((
                addr0,
                vec![
                    (55, Persisted::Session("asdf0".to_string()), 1),
                    (55, Persisted::ViewPostsPage(0), 1),
                    (5, Persisted::Post, 1),
                    (55, Persisted::PostLike(5), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr0, vec![QueryResult::PostLikedByUser(5, true)]))
        );

        persisted_sender
            .send((
                addr1,
                vec![
                    (56, Persisted::Session("asdf0".to_string()), 1),
                    (56, Persisted::ViewPostsPage(0), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr1, vec![QueryResult::PostLikedByUser(5, true)]))
        );
    }
}
