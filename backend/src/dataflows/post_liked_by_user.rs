use crate::forum_minimal::{
    OutputScopeCollection, Persisted, QueryResult, ScopeCollection, POSTS_PER_PAGE,
};
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use timely::dataflow::operators::Filter;
use timely::dataflow::operators::Map;

use log::debug;

pub fn post_liked_by_user_dataflow<'a>(
    collection: &ScopeCollection<'a>,
) -> OutputScopeCollection<'a> {
    let user_id_to_page = collection
        .flat_map(|(_addr, (user_id, persisted))| {
            if let Persisted::ViewPostsPage(page) = persisted {
                vec![(user_id, page)]
            } else {
                vec![]
            }
        })
        .inner
        .filter(|(_, _time, diff)| *diff > 0)
        .as_collection()
        .inspect(|v| debug!("current page -- {:?}", v));

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

    let post_pages = collection
        .flat_map(|(_addr, (post_id, persisted))| {
            if let Persisted::Post = persisted {
                vec![post_id]
            } else {
                vec![]
            }
        })
        .inner
        .map(|(post_id, time, diff)| (((), (-(time as i64), post_id)), time, diff))
        .as_collection()
        // reduce will automatically order by time
        .reduce(|_, inputs, outputs| {
            debug!("only inputs = {:?}", inputs);
            for (index, ((_time, post_id), diff)) in inputs.into_iter().enumerate() {
                if *diff > 0 {
                    outputs.push(((*post_id, index / POSTS_PER_PAGE), 1));
                }
                // outputs.push(((*post_id, index / POSTS_PER_PAGE), 1));
            }
        })
        .map(|((), val)| val)
        .inspect(|v| debug!("post pages -- {:?}", v));

    let result = user_id_to_addr
        .join(&post_likes)
        .join(&user_id_to_page)
        .map(|(_user_id, ((session_addr, post_id), visible_page))| {
            (post_id, (session_addr, visible_page))
        })
        .join(&post_pages)
        .inspect(|v| debug!("id and page -- {:?}", v))
        .filter(|(_post_id, ((_session_addr, visible_page), post_page))| {
            *visible_page == *post_page as u64
        })
        .inspect(|v| debug!("filtered id and page -- {:?}", v))
        .inner
        .map(
            |((post_id, ((session_addr, _visible_page), _post_page)), time, diff)| {
                (
                    vec![(
                        session_addr,
                        QueryResult::PostLikedByUser(post_id, diff > 0),
                    )],
                    time,
                    diff,
                )
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
                    (55, Persisted::Session, 1),
                    (55, Persisted::ViewPostsPage(1), 1),
                    (5, Persisted::Post, 1),
                    (6, Persisted::Post, 1),
                    (7, Persisted::Post, 1),
                    (55, Persisted::PostLike(7), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr0, vec![QueryResult::PostLikedByUser(7, true)]))
        );

        // persisted_sender
        //     .send((
        //         addr1,
        //         vec![
        //             (55, Persisted::Session, -1),
        //             (55, Persisted::ViewPostsPage(1), -1),
        //         ],
        //     ))
        //     .unwrap();

        // forum_minimal.advance_dataflow_computation_once().await;

        persisted_sender
            .send((
                addr1,
                vec![
                    (55, Persisted::Session, 1),
                    (55, Persisted::ViewPostsPage(0), 1),
                    (55, Persisted::PostLike(7), -1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty),
        );
    }
}
