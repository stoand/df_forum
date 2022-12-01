use crate::forum_minimal::{
    OutputScopeCollection, Persisted, QueryResult, ScopeCollection, POSTS_PER_PAGE,
};
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use timely::dataflow::operators::Map;

use log::debug;

pub fn post_liked_by_user_dataflow<'a>(
    collection: &ScopeCollection<'a>,
) -> OutputScopeCollection<'a> {
    let user_id_to_page = collection.flat_map(|(_addr, (user_id, persisted))| {
        if let Persisted::ViewPostsPage(page) = persisted {
            vec![(user_id, page)]
        } else {
            vec![]
        }
    });

    let user_id_to_addr = collection.flat_map(|(addr, (user_id, persisted))| {
        if let Persisted::Session = persisted {
            vec![(user_id, addr)]
        } else {
            vec![]
        }
    });

    let post_likes = collection.flat_map(|(_addr, (user_id, persisted))| {
        if let Persisted::PostLike(post_id) = persisted {
            vec![(user_id, post_id)]
        } else {
            vec![]
        }
    });

    let post_pages = collection.map(|_| (1, 1));

    // TODO - only send likes for visible posts

    let result = user_id_to_addr
        .join(&post_likes)
        .join(&user_id_to_page)
        .map(|(_user_id, ((session_addr, post_id), visible_page))| (post_id, (session_addr, visible_page)))
        .join(&post_pages)
        .filter(|(_post_id, ((_session_addr, visible_page), post_page))| visible_page == post_page)
        .inner
        .map(|((post_id, ((session_addr, _visible_page), _post_page)), time, diff)| {
            (
                vec![(
                    session_addr,
                    QueryResult::PostLikedByUser(post_id, diff > 0),
                )],
                time,
                diff,
            )
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
                    (55, Persisted::Session, 1),
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
                    (55, Persisted::Session, 1),
                    (55, Persisted::ViewPostsPage(0), 1),
                    (55, Persisted::PostLike(5), -1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        let mut recv = Vec::new();

        recv.push(query_result_receiver.try_recv().unwrap());
        recv.push(query_result_receiver.try_recv().unwrap());
        recv.sort_by_key(|(addr, _)| *addr);

        assert_eq!(
            recv[0],
            (addr0, vec![QueryResult::PostLikedByUser(5, false)])
        );

        assert_eq!(
            recv[1],
            (addr1, vec![QueryResult::PostLikedByUser(5, false)])
        );

        // persisted_sender
        //     .send((addr1, vec![(56, Persisted::PostLike(5), -1)]))
        //     .unwrap();

        // forum_minimal.advance_dataflow_computation_once().await;

        // assert_eq!(
        //     query_result_receiver.try_recv(),
        //     Ok((addr1, vec![QueryResult::PostLikedByUser(5, false)]))
        // );

        // create, like, refresh, unlike, refresh
    }
}
