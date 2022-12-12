use crate::forum_minimal::{OutputScopeCollection, Persisted, QueryResult, ScopeCollection};
use log::debug;

use timely::dataflow::operators::Filter;
use timely::dataflow::operators::Map;

// use differential_dataflow::operators::Consolidate;
// use differential_dataflow::operators::Count;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;

use crate::dataflows::shared_post_pages;

pub fn posts_post_ids_dataflow<'a>(collection: &ScopeCollection<'a>) -> OutputScopeCollection<'a> {
    let session_pages = collection
        .reduce(|_addr, inputs, outputs| {
            // debug!(
            //     "addr(key) = {:?}, input = {:?}, output = {:?}",
            //     addr, inputs, outputs
            // );

            let mut page = None;

            for ((_id, persisted), diff) in inputs {
                if *diff > 0 {
                    match persisted {
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
        .inspect(|v| debug!("session pages -- {:?}", v));

    let page_posts = shared_post_pages(&collection)
        .map(|(addr, post_id, page, position)| (page, (addr, post_id, position)));

    let session_posts = session_pages
        .map(|(addr, page)| (page, addr))
        .join::<_, isize>(&page_posts);

    let session_post_results = session_posts
        .inner
        .map(
            move |((page, (session_addr, (_addr, id, creation_time))), time, diff)| {
                let query_result = if diff > 0 {
                    vec![(session_addr, QueryResult::PagePost(id, page, creation_time))]
                } else {
                    vec![(session_addr, QueryResult::DeletePost(id))]
                };
                debug!("session posts -- {:?}", query_result);

                (query_result, time, diff)
            },
        )
        .as_collection();

    let session_post_ids =
        session_posts.map(|(_page, (session_addr, (_addr, id, _time)))| (id, session_addr));

    let session_post_field_results = collection
        .map(|(creator_addr, (id, persisted))| (id, (creator_addr, persisted)))
        .join::<_, isize>(&session_post_ids)
        .inner
        .map(
            move |((id, ((_creator_addr, persisted), session_addr)), time, diff)| {
                if diff > 0 {
                    let query_result = match persisted {
                        Persisted::PostTitle(title) => {
                            vec![(session_addr, QueryResult::PostTitle(id, title.clone()))]
                        }
                        Persisted::PostBody(body) => {
                            vec![(session_addr, QueryResult::PostBody(id, body.clone()))]
                        }
                        _ => vec![],
                    };

                    (query_result, time, diff)
                } else {
                    (vec![], time, diff)
                }
            },
        )
        .as_collection()
        .inspect(|v| debug!("session post fields -- {:?}", v));

    let post_creator_addrs = collection
        .flat_map(|(creator_addr, (post_id, persisted))| {
            if let Persisted::Post = persisted {
                vec![(creator_addr, post_id)]
            } else {
                vec![]
            }
        })
        .inner
        .filter(|(_, _time, diff)| *diff > 0)
        .as_collection();

    let user_id_addrs = collection.flat_map(|(creator_addr, (user_id, persisted))| {
        if let Persisted::Session = persisted {
            vec![(creator_addr, user_id)]
        } else {
            vec![]
        }
    });

    let post_creator_names_results = post_creator_addrs
        .join(&user_id_addrs)
        .map(|(creator_addr, (post_id, user_id))| (post_id, (creator_addr, user_id)))
        .join(&session_post_ids)
        .inner
        .map(
            |((post_id, ((_creator_addr, user_id), session_addr)), time, diff)| {
                let result = if diff > 0 {
                    vec![(
                        session_addr,
                        QueryResult::PostCreator(post_id, user_id.to_string()),
                    )]
                } else {
                    vec![]
                };

                (result, time, diff)
            },
        )
        .as_collection();

    session_post_field_results
        .concat(&session_post_results)
        .concat(&post_creator_names_results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forum_minimal::ForumMinimal;
    use std::net::SocketAddr;
    use tokio::sync::broadcast;
    use tokio::sync::broadcast::error::TryRecvError;

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

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::PagePost(7, 1, 0),
                    QueryResult::PostTitle(7, "Protoss".into()),
                    QueryResult::PostBody(7, "Protoss Info".into()),
                    // QueryResult::PostTotalLikes(7, 0),
                ]
            ))
        );

        persisted_sender
            .send((
                addr,
                vec![
                    (55, Persisted::ViewPostsPage(1), -1),
                    (55, Persisted::ViewPostsPage(0), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::DeletePost(7),
                    QueryResult::PagePost(5, 0, 0),
                    QueryResult::PagePost(6, 0, 0),
                    QueryResult::PostTitle(5, "Zerg".into()),
                    QueryResult::PostTitle(6, "Terran".into()),
                    QueryResult::PostBody(5, "Zerg Info".into()),
                    QueryResult::PostBody(6, "Terran Info".into()),
                    // QueryResult::PostTotalLikes(5, 0),
                    // QueryResult::PostTotalLikes(6, 0),
                ]
            ))
        );

        debug!("TESTING DELETION (0) ------------");

        persisted_sender
            .send((addr, vec![(7, Persisted::Post, -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        // when deleting a post that is not in view, nothing should happen
        assert_eq!(query_result_receiver.try_recv(), Err(TryRecvError::Empty));

        debug!("TESTING DELETION (1) ------------");

        persisted_sender
            .send((addr, vec![(6, Persisted::Post, -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        // when deleting a post that is not in view, nothing should happen
        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr, vec![(QueryResult::DeletePost(6))]))
        );
    }

    #[tokio::test]
    pub async fn test_page_post_deletion() {
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
            Ok((
                addr,
                vec![
                    QueryResult::PagePost(5, 0, 0),
                    QueryResult::PagePost(6, 0, 0),
                    // QueryResult::PostTotalLikes(5, 0),
                    // QueryResult::PostTotalLikes(6, 0),
                ]
            ))
        );

        persisted_sender
            .send((addr, vec![(6, Persisted::Post, -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::DeletePost(6),
                    QueryResult::PagePost(7, 0, 0),
                    // QueryResult::PostTotalLikes(7, 0),
                ]
            ))
        );
    }

    #[tokio::test]
    pub async fn test_page_post_username() {
        crate::init_logger();
        let addr0: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr1: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new_with_dataflows(
            persisted_sender.clone(),
            query_result_sender,
            posts_post_ids_dataflow,
        );

        persisted_sender
            .send((
                addr0,
                vec![
                    (55, Persisted::ViewPostsPage(0), 1),
                    (55, Persisted::Session, 1),
                    (5, Persisted::Post, 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        persisted_sender
            .send((
                addr1,
                vec![
                    (56, Persisted::ViewPostsPage(0), 1),
                    (56, Persisted::Session, 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr0,
                vec![
                    QueryResult::PagePost(5, 0, 0),
                    QueryResult::PostCreator(5, "55".to_string()),
                    // QueryResult::PostTotalLikes(5, 0),
                ]
            ))
        );

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr1,
                vec![
                    QueryResult::PagePost(5, 0, 0),
                    QueryResult::PostCreator(5, "55".to_string()),
                    // QueryResult::PostTotalLikes(5, 0),
                ]
            ))
        );

        persisted_sender
            .send((addr1, vec![(5, Persisted::Post, -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr0, vec![QueryResult::DeletePost(5),]))
        );

        // Warning - FLAKY
        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((addr1, vec![QueryResult::DeletePost(5),]))
        );
    }
    #[tokio::test]
    pub async fn test_page_post_likes() {
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
                    (55, Persisted::ViewPostsPage(0), 1),
                    (5, Persisted::Post, 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::PagePost(5, 0, 0),
                    // QueryResult::PostTotalLikes(5, 0),
                ]
            ))
        );

        persisted_sender
            .send((addr, vec![(55, Persisted::PostLike(5), 1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        persisted_sender
            .send((addr, vec![(55, Persisted::PostLike(5), -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        persisted_sender
            .send((
                addr,
                vec![
                    (55, Persisted::PostLike(6), 1),
                    (55, Persisted::PostLike(7), 1),
                    (56, Persisted::PostLike(7), 1),
                    (6, Persisted::Post, 1),
                    (7, Persisted::Post, 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::DeletePost(5),
                    QueryResult::PagePost(6, 0, 4),
                    QueryResult::PagePost(7, 0, 4),
                    // QueryResult::PostTotalLikes(6, 1),
                    // QueryResult::PostTotalLikes(7, 2),
                    // QueryResult::PostLikedByUser(6, true),
                    // QueryResult::PostLikedByUser(7, true),
                ]
            ))
        );

        persisted_sender
            .send((addr, vec![(55, Persisted::PostLike(7), -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;
    }

    // DISABLED
    // #[tokio::test]
    // pub async fn test_page_post_likes_multi_addr() {
    pub async fn _test_page_post_likes_multi_addr() {
        crate::init_logger();
        let addr0: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr1: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8082".parse().unwrap();
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new_with_dataflows(
            persisted_sender.clone(),
            query_result_sender,
            posts_post_ids_dataflow,
        );

        persisted_sender
            .send((
                addr0,
                vec![
                    (55, Persisted::ViewPostsPage(0), 1),
                    (55, Persisted::Session, 1),
                    (5, Persisted::Post, 1),
                    (6, Persisted::Post, 1),
                    (55, Persisted::PostLike(5), 1),
                    (55, Persisted::PostLike(6), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        persisted_sender
            .send((
                addr1,
                vec![
                    (56, Persisted::ViewPostsPage(0), 1),
                    (56, Persisted::Session, 1),
                    (56, Persisted::PostLike(5), 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr0,
                vec![
                    QueryResult::PagePost(5, 0, 0),
                    QueryResult::PagePost(6, 0, 0),
                    QueryResult::PostCreator(5, "55".to_string()),
                    QueryResult::PostCreator(6, "55".to_string()),
                    // QueryResult::PostTotalLikes(5, 1),
                    // QueryResult::PostTotalLikes(6, 1),
                    // QueryResult::PostLikedByUser(5, true),
                    // QueryResult::PostLikedByUser(6, true),
                ]
            ))
        );

        // what address is sent to first is non-deterministic
        let mut recv = Vec::new();

        recv.push(query_result_receiver.try_recv().unwrap());
        recv.push(query_result_receiver.try_recv().unwrap());
        recv.sort_by_key(|(addr, _)| *addr);

        assert_eq!(
            recv[0],
            (
                addr0,
                vec![
                    // QueryResult::PostTotalLikes(5, 2),
                    // QueryResult::PostLikedByUser(5, true),
                ]
            )
        );

        assert_eq!(
            recv[1],
            (
                addr1,
                vec![
                    QueryResult::PagePost(5, 0, 0),
                    QueryResult::PagePost(6, 0, 0),
                    QueryResult::PostCreator(5, "asdf0".to_string()),
                    QueryResult::PostCreator(6, "asdf0".to_string()),
                    // QueryResult::PostTotalLikes(5, 2),
                    // QueryResult::PostTotalLikes(6, 1),
                    // QueryResult::PostLikedByUser(5, true),
                    // QueryResult::PostLikedByUser(6, true),
                ]
            )
        );

        persisted_sender
            .send((addr1, vec![(55, Persisted::PostLike(6), -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        // what address is sent to first is non-deterministic
        let mut recv = Vec::new();

        recv.push(query_result_receiver.try_recv().unwrap());
        recv.push(query_result_receiver.try_recv().unwrap());
        recv.sort_by_key(|(addr, _)| *addr);

        // this test is flaky
        assert_eq!(
            recv[0],
            (
                addr0,
                vec![
                    // QueryResult::PostTotalLikes(6, 0),
                    // this test is flaky
                    // QueryResult::PostLikedByUser(6, false),
                ]
            )
        );

        assert_eq!(
            recv[1],
            (
                addr1,
                vec![
                    // QueryResult::PostTotalLikes(6, 0),
                    // this test is flaky
                    // QueryResult::PostLikedByUser(6, false),
                ]
            )
        );

        persisted_sender
            .send((
                addr2,
                vec![
                    (57, Persisted::ViewPostsPage(0), 1),
                    (57, Persisted::Session, 1),
                ],
            ))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr2,
                vec![
                    QueryResult::PagePost(5, 0, 0),
                    QueryResult::PagePost(6, 0, 0),
                    QueryResult::PostCreator(5, "55".to_string()),
                    QueryResult::PostCreator(6, "55".to_string()),
                    // QueryResult::PostTotalLikes(5, 2),
                    // QueryResult::(6, 0),
                    // QueryResult::PostLikedByUser(5, true),
                ]
            ))
        );
    }
}
