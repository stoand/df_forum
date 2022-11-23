use crate::forum_minimal::{
    Persisted, QueryResult, QueryResultSender, ScopeCollection, POSTS_PER_PAGE,
};
use log::debug;
use std::collections::HashMap;
use std::net::SocketAddr;

use timely::dataflow::operators::Map;

use differential_dataflow::operators::Consolidate;
// use differential_dataflow::operators::Count;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;

pub fn posts_post_ids_dataflow<'a>(
    collection: &ScopeCollection<'a>,
    query_result_sender: QueryResultSender,
) {
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

    let post_ids_with_time = collection
        .inner
        .map(|((addr, (id, persisted)), time, diff)| ((time, addr, id, persisted), time, diff))
        .as_collection()
        .flat_map(|(time, addr, id, persisted)| match persisted {
            Persisted::Post => vec![(0, (addr, id, time))],
            _ => vec![],
        });

    // we don't care who created the posts
    let page_posts = post_ids_with_time
        .reduce(|_discarded_zero, inputs, outputs| {
            debug!("input = {:?}, output = {:?}", inputs, outputs);

            let (mut added, mut removed): (Vec<_>, Vec<_>) =
                inputs.to_vec().into_iter().partition(|(_, diff)| *diff > 0);

            added.sort_by_key(|((_addr, _id, time), _diff)| -(*time as isize));
            removed.sort_by_key(|((_addr, _id, time), _diff)| -(*time as isize));

            let mut page_item_index: u64 = 0;
            let mut page = 0;

            for ((addr, id, creation_time), _diff) in added {
                outputs.push(((*addr, *id, page, *creation_time), 1));

                let dup_removed = removed
                    .iter()
                    .find(|((_addr, other_id, _creation_time), _diff)| id == other_id);
                if dup_removed != None {
                    outputs.push(((*addr, *id, page, *creation_time), -1));
                    continue;
                } else {
                    outputs.push(((*addr, *id, page, *creation_time), 1));
                }
                page_item_index += 1;
                if page_item_index >= POSTS_PER_PAGE as u64 {
                    page_item_index = 0;
                    page += 1;
                }
            }
        })
        .map(|(_discarded_zero, (addr, id, page, creation_time))| (page, (addr, id, creation_time)))
        .inspect(|v| debug!("page posts -- {:?}", v));

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

    let post_creator_addrs = collection.flat_map(|(creator_addr, (post_id, persisted))| {
        if let Persisted::Post = persisted {
            vec![(creator_addr, post_id)]
        } else {
            vec![]
        }
    });

    let session_name_addrs = collection.flat_map(|(creator_addr, (_session_id, persisted))| {
        if let Persisted::Session(session_name) = persisted {
            vec![(creator_addr, session_name)]
        } else {
            vec![]
        }
    });

    let post_creator_names_results = post_creator_addrs
        .join(&session_name_addrs)
        .map(|(creator_addr, (post_id, session_name))| (post_id, (creator_addr, session_name)))
        .join(&session_post_ids)
        .inner
        .map(
            |((post_id, ((_creator_addr, session_name), session_addr)), time, diff)| {
                let result = if diff > 0 {
                    vec![(
                        session_addr,
                        QueryResult::PostCreator(post_id, session_name),
                    )]
                } else {
                    vec![]
                };

                (result, time, diff)
            },
        )
        .as_collection();

    let posts_liked_by_user = collection.flat_map(|(addr, (_session_id, persisted))| {
        if let Persisted::PostLike(liked_post) = persisted {
            vec![(liked_post, addr)]
        } else {
            vec![]
        }
    });

    let posts_liked_by_user_result = session_post_ids
        .join(&posts_liked_by_user)
        .inner
        .map(|((post_id, (session_addr, addr)), time, diff)| {
            (
                if session_addr == addr {
                    vec![(
                        session_addr,
                        QueryResult::PostLikedByUser(post_id, diff > 0),
                    )]
                } else {
                    vec![]
                },
                time,
                diff,
            )
        })
        // .map(|((post_id, (session_addr, addr)), time, diff)| {
        //     (
        //         vec![(
        //             session_addr,
        //             QueryResult::PostLikedByUser(post_id, diff > 0),
        //         )],
        //         time,
        //         diff,
        //     )
        // })
        .as_collection()
        .inspect(|v| debug!("likes -- {:?}", v));

    let post_total_like_count_result = posts_liked_by_user
        .inspect(|v| debug!("liked 0 -- {:?}", v))
        .reduce(|_post_id, inputs, outputs| {
            outputs.push((inputs.len(), 1));
        })
        .inspect(|v| debug!("liked 1 -- {:?}", v))
        .map(|(post_id, count)| (post_id, count))
        .join(&session_post_ids)
        .inner
        .map(|((post_id, (count, session_addr)), time, diff)| {
            let result = if diff > 0 {
                vec![(
                    session_addr,
                    QueryResult::PostTotalLikes(post_id, count as u64),
                )]
            // } else if count == 1 {
            //     vec![(session_addr, QueryResult::PostTotalLikes(post_id, 0))]
            } else {
                vec![]
            };
            (result, time, diff)
        })
        .as_collection()
        .inspect(|v| debug!("like counts -- {:?}", v));

    // Send everything at once to prevent flickering (but still split by session)
    let _batch_output = session_post_field_results
        .concat(&session_post_results)
        .concat(&post_creator_names_results)
        .concat(&posts_liked_by_user_result)
        .concat(&post_total_like_count_result)
        .consolidate()
        .inspect_batch(move |_time, query_results_aug| {
            let mut sessions: HashMap<SocketAddr, Vec<QueryResult>> = HashMap::new();

            let query_results = query_results_aug
                .to_vec()
                .into_iter()
                .map(|(qr, _time, _diff)| qr)
                .flatten()
                .collect::<Vec<_>>();

            // Break apart query_results by session

            for (session_addr, query_result) in query_results {
                if None == sessions.get(&session_addr) {
                    sessions.insert(session_addr, Vec::new());
                }
                sessions
                    .get_mut(&session_addr)
                    .expect("session not found")
                    .push(query_result);
            }

            for (session_addr, query_results) in sessions.iter() {
                query_result_sender
                    .clone()
                    .send((*session_addr, query_results.clone()))
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
                vec![QueryResult::DeletePost(6), QueryResult::PagePost(7, 0, 0)]
            ))
        );
    }

    #[tokio::test]
    pub async fn test_page_post_username() {
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
                    (55, Persisted::Session("asdf".to_string()), 1),
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
                    QueryResult::PostCreator(5, "asdf".to_string()),
                ]
            ))
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
            Ok((addr, vec![QueryResult::PagePost(5, 0, 0),]))
        );

        persisted_sender
            .send((addr, vec![(55, Persisted::PostLike(5), 1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::PostTotalLikes(5, 1),
                    QueryResult::PostLikedByUser(5, true),
                ]
            ))
        );

        persisted_sender
            .send((addr, vec![(55, Persisted::PostLike(5), -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::PostTotalLikes(5, 0),
                    QueryResult::PostLikedByUser(5, false),
                ]
            ))
        );

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
                    QueryResult::PostTotalLikes(6, 1),
                    QueryResult::PostTotalLikes(7, 2),
                    QueryResult::PostLikedByUser(6, true),
                    QueryResult::PostLikedByUser(7, true),
                ]
            ))
        );

        persisted_sender
            .send((addr, vec![(55, Persisted::PostLike(7), -1)]))
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.try_recv(),
            Ok((
                addr,
                vec![
                    QueryResult::PostTotalLikes(7, 1),
                    QueryResult::PostLikedByUser(7, false)
                ]
            ))
        );
    }
    #[tokio::test]
    pub async fn test_page_post_likes_multi_addr() {
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
                    QueryResult::PostTotalLikes(5, 1),
                    QueryResult::PostTotalLikes(6, 1),
                    QueryResult::PostLikedByUser(5, true),
                    QueryResult::PostLikedByUser(6, true),
                ]
            ))
        );

        // what address is sent to first is non-deterministic
        let mut recv = Vec::new();

        recv.push(query_result_receiver.try_recv().unwrap());
        recv.push(query_result_receiver.try_recv().unwrap());
        recv.sort_by_key(|(addr, _)| *addr);

        assert_eq!(recv[0], (addr0, vec![QueryResult::PostTotalLikes(5, 2)]));

        assert_eq!(
            recv[1],
            (
                addr1,
                vec![
                    QueryResult::PagePost(5, 0, 0),
                    QueryResult::PagePost(6, 0, 0),
                    QueryResult::PostTotalLikes(5, 2),
                    QueryResult::PostTotalLikes(6, 1),
                    QueryResult::PostLikedByUser(5, true),
                ]
            )
        );
    }
}
