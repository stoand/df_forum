use df_forum_frontend::df_tuple_items::{Diff, Id, Time};
pub use df_forum_frontend::persisted::{Persisted, PersistedItems, Post};
use df_forum_frontend::query_result::QueryResult;
// use crate::operators::only_latest::OnlyLatest;

use std::cell::RefCell;
use std::rc::Rc;
use timely::communication::allocator::thread::Thread;
use timely::dataflow::operators::Map;
use timely::worker::Worker;
use timely::WorkerConfig;

use std::net::SocketAddr;
use tokio::sync::broadcast;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Count;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use differential_dataflow::{Collection, ExchangeData};

pub type PersistedInputSession = InputSession<Time, (SocketAddr, (Id, Persisted)), Diff>;

pub const POSTS_PER_PAGE: usize = 2;

pub struct ForumMinimal {
    pub input: Rc<RefCell<PersistedInputSession>>,
    pub worker: Rc<RefCell<Worker<timely::communication::allocator::Thread>>>,
    pub persisted_receiver: broadcast::Receiver<(SocketAddr, PersistedItems)>,
    pub dataflow_time: u64,
}

impl ForumMinimal {
    pub fn new(
        persisted_sender: broadcast::Sender<(SocketAddr, PersistedItems)>,
        query_result_sender: broadcast::Sender<(SocketAddr, Vec<QueryResult>)>,
    ) -> Self {
        let query_result_sender0 = query_result_sender.clone();
        let query_result_sender1 = query_result_sender.clone();
        let query_result_sender2 = query_result_sender.clone();

        let worker_fn = move |worker: &mut Worker<Thread>| {
            worker.dataflow(|scope| {
                let mut input: PersistedInputSession = InputSession::new();
                let manages_sess = input.to_collection(scope);
                let manages = manages_sess.map(|(addr, (id, persisted))| (id, persisted));

                let sessions = manages_sess
                    .map(|(addr, (id, persisted))| addr)
                    .consolidate();

                let view_posts = manages_sess.flat_map(|(addr, (id, persisted))| {
                    if let Persisted::ViewPosts(_) = persisted {
                        vec![(addr, 0)]
                    } else {
                        vec![]
                    }
                });

                let view_posts_page = manages_sess.flat_map(|(addr, (id, persisted))| {
                    if let Persisted::ViewPostsPage(_session, page) = persisted {
                        vec![(addr, page)]
                    } else {
                        vec![]
                    }
                });

                let sessions_view_posts = view_posts
                    .join_map(&sessions.map(|v| (v, v)), |_key, &a, &b| (a, b))
                    .map(|(v0, v1)| (v1, None::<u64>));
                // .inspect(|v| println!("1 -- {:?}", v));

                let sessions_view_posts_page = view_posts_page
                    .join_map(&sessions.map(|v| (v, v)), |_key, &a, &b| (a, b))
                    .map(|(v0, v1)| (v1, Some(v0)));
                // .inspect(|v| println!("2 -- {:?}", v));

                let sessions_current_page = sessions_view_posts
                    .concat(&sessions_view_posts_page)
                    .reduce(|key, inputs, outputs| {
                        let mut final_page = None::<u64>;
                        let mut found: bool = false;

                        for (page, diff) in inputs {
                            if *diff > 0 {
                                if let Some(page0) = page {
                                    final_page = Some(*page0);
                                }
                                found = true;
                            }
                        }
                        // println!(
                        //     "key = {:?}, input = {:?}, output = {:?}",
                        //     key, inputs, outputs
                        // );

                        if found {
                            outputs.push((final_page.unwrap_or(0), 1));
                        }
                    })
                    .inspect(|v| println!("1 -- {:?}", v));

                let query_result_sender_loop = query_result_sender1.clone();

                let post_ids = manages
                    .inner
                    .map(|((id, persisted), time, diff)| ((time, id, persisted), time, diff))
                    .as_collection()
                    .flat_map(|(time, id, persisted)| match persisted {
                        Persisted::PostTitle(_) => vec![(0, (id, time))],
                        _ => vec![],
                    })
                    .inspect(|v| println!("2 -- {:?}", v));

                // todo - join all posts for every user+current_page
                let pages_with_zero = sessions_current_page
                    .map(|(_user_id, page)| (0, page))
                    .consolidate()
                    .inspect(|v| println!("3 -- {:?}", v));

                let page_ids_with_all_post_ids = pages_with_zero.join(&post_ids).map(
                    |(_discarded_zero, (page_id, (post_id, post_time)))| {
                        (page_id, (post_id, post_time))
                    },
                );
                // .inspect(|v| println!("4 -- {:?}", v));

                let page_ids_with_relevant_post_ids =
                    page_ids_with_all_post_ids.reduce(move |page_id, inputs, outputs| {
                        println!(
                            "key = {:?}, input = {:?}, output = {:?}",
                            page_id, inputs, outputs
                        );

                        let mut sorted = inputs.to_vec();
                        sorted.sort_by_key(|((_post_id, time), _diff)| -(*time as isize));
                        let items: Vec<u64> = sorted
                            .iter()
                            .skip((*page_id) as usize * POSTS_PER_PAGE)
                            .take(POSTS_PER_PAGE)
                            .filter(|((_id, _time), diff)| *diff > 0)
                            .map(|((id, _time), _diff)| *id)
                            .collect();

                        outputs.push((items, 1));
                    });
                // .inspect(|v| println!("4.1 -- {:?}", v));

                let page_ids_with_relevant_posts = page_ids_with_relevant_post_ids
                    .flat_map(|(page_id, post_ids)| {
                        post_ids.into_iter().map(move |post_id| (page_id, post_id))
                    })
                    .map(|(page_id, post_id)| (post_id, page_id))
                    .inspect(|v| println!("5.1 -- {:?}", v));

                let post_titles = manages.flat_map(|(id, persisted)| {
                    if let Persisted::PostTitle(title) = persisted {
                        vec![(id, title)]
                    } else {
                        vec![]
                    }
                });

                let post_bodies = manages.flat_map(|(id, persisted)| {
                    if let Persisted::PostBody(body) = persisted {
                        vec![(id, body)]
                    } else {
                        vec![]
                    }
                });

                let posts = post_titles
                    .join(&post_bodies)
                    .inspect(|v| println!("5.2 -- {:?}", v));

                let page_posts = page_ids_with_relevant_posts
                    .join(&posts)
                    .map(|(post_id, (page_id, post))| (page_id, (post_id, post)));

                let _user_posts = sessions_current_page
                    .map(|(addr, page_id)| (page_id, addr))
                    .join(&page_posts)
                    .inspect(|v| println!("5.3 -- {:?}", v))
                    .inspect(
                        move |(
                            (_page_id, (addr, (post_id, (post_title, post_body)))),
                            _time,
                            diff,
                        )| {
                            // Todo only send this to the relevant addr
                            let _todo = addr;
                            let query_result = if *diff > 0 {
                                QueryResult::AddPost(
                                    *post_id,
                                    post_title.clone(),
                                    post_body.clone(),
                                )
                            } else {
                                QueryResult::DeletePost(*post_id)
                            };

                            println!("send Query::Posts -- {:?}", query_result);

                            query_result_sender0
                                .clone()
                                .send((*addr, vec![query_result]))
                                .unwrap();
                        },
                    );

                // TODO:
                // let query_result_sender_loop = query_result_sender1.clone();
                // 
                // let _post_aggregates = manages
                //     .flat_map(|(_id, persisted)| {
                //         if let Persisted::PostTitle(_) = persisted {
                //             vec![0]
                //         } else {
                //             vec![]
                //         }
                //     })
                //     .count()
                //     .inspect_batch(move |_time, items| {
                //         let mut final_count = 0;

                //         for ((_discarded_zero, count), _time, diff) in items {
                //             if *diff > 0 {
                //                 final_count = *count as u64;
                //             }
                //         }

                //         let page_count =
                //             ((final_count as f64) / (POSTS_PER_PAGE as f64)).ceil() as u64;
                //         query_result_sender_loop
                //             .clone()
                //             .send(vec![QueryResult::PostAggregates(final_count, page_count)])
                //             .unwrap();
                //     });

                input
            })
        };

        let alloc = Thread::new();
        let worker = Worker::new(WorkerConfig::default(), alloc);
        let worker0 = Rc::new(RefCell::new(worker.clone()));
        let worker1 = worker0.clone();
        let input = worker_fn(&mut worker1.borrow_mut());

        let input0: Rc<RefCell<InputSession<u64, (SocketAddr, (u64, Persisted)), isize>>> =
            Rc::new(RefCell::new(input));
        let input1 = input0.clone();

        ForumMinimal {
            input: input1,
            worker: worker0,
            persisted_receiver: persisted_sender.subscribe(),
            dataflow_time: 1,
        }
    }

    pub async fn advance_dataflow_computation_once(&mut self) {
        // TODO:
        let (addr, persisted_items) = self.persisted_receiver.recv().await.unwrap();

        self.dataflow_time += 1;

        for (id, item, diff) in persisted_items {
            if diff > 0 {
                self.input.borrow_mut().insert((addr, (id, item)));
            } else {
                self.input.borrow_mut().remove((addr, (id, item)));
            }
        }
        self.input.borrow_mut().advance_to(self.dataflow_time);

        for _ in 0..100 {
            self.input.borrow_mut().flush();
            self.worker.borrow_mut().step();
        }
    }
    pub async fn loop_advance_dataflow_computation(&mut self) {
        loop {
            self.advance_dataflow_computation_once().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_recv_contains<T: PartialEq + Clone>(
        reciever: &mut broadcast::Receiver<T>,
        values: T,
    ) -> bool {
        let mut success = false;

        while let Ok(val) = reciever.try_recv() {
            if val == values {
                success = true
            }
        }

        success
    }

    #[tokio::test]
    pub async fn test_basic() {
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new(persisted_sender.clone(), query_result_sender);

        let add_posts = vec![
            (10, Persisted::PostTitle("Zerg".into()), 1),
            (10, Persisted::PostBody("info about zerg".into()), 1),
            (10, Persisted::PostUserId(0), 1),
            (10, Persisted::PostLikes(0), 1),
            (20, Persisted::PostTitle("Terran".into()), 1),
            (20, Persisted::PostBody("info about terran".into()), 1),
            (20, Persisted::PostUserId(0), 1),
            (20, Persisted::PostLikes(0), 1),
        ];
        persisted_sender.clone().send(add_posts).unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert!(try_recv_contains(
            &mut query_result_receiver,
            vec![QueryResult::PostCount(2)]
        ));

        let remove_post = vec![
            (10, Persisted::PostTitle("Zerg".into()), -1),
            (10, Persisted::PostBody("info about zerg".into()), -1),
            (10, Persisted::PostUserId(0), -1),
            (10, Persisted::PostLikes(0), -1),
        ];

        persisted_sender.clone().send(remove_post).unwrap();
        forum_minimal.advance_dataflow_computation_once().await;

        assert!(try_recv_contains(
            &mut query_result_receiver,
            vec![QueryResult::PostCount(1)]
        ));
    }

    #[tokio::test]
    pub async fn test_pagination() {
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new(persisted_sender.clone(), query_result_sender);

        let mut found = false;
        persisted_sender
            .clone()
            .send(vec![
                (55, Persisted::Session, 1),
                (66, Persisted::ViewPosts(55), 1),
                (77, Persisted::ViewPostsPage(55, 1), 1),
            ])
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;
        persisted_sender
            .send(vec![
                (5, Persisted::PostTitle("Zerg".into()), 1),
                (5, Persisted::PostBody("Info about the Zerg".into()), 1),
            ])
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        persisted_sender
            .send(vec![
                (4, Persisted::PostTitle("Protoss".into()), 1),
                (4, Persisted::PostBody("Info about the Protoss".into()), 1),
                (6, Persisted::PostTitle("Terran".into()), 1),
                (6, Persisted::PostBody("Info about the Terran".into()), 1),
            ])
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        persisted_sender
            .clone()
            .send(vec![
                (77, Persisted::ViewPostsPage(55, 1), -1),
                (77, Persisted::ViewPostsPage(55, 0), 1),
            ])
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        // for i in 0..10 {
        //     persisted_sender
        //         .clone()
        //         .send(vec![(
        //             i * 100,
        //             Persisted::PostTitle("PostNum".to_string() + &i.to_string()),
        //             1,
        //         )])
        //         .unwrap();

        //     forum_minimal.advance_dataflow_computation_once().await;

        //     if !found {
        //         found = try_recv_contains(
        //             &mut query_result_receiver,
        //             vec![(
        //                 Query::PostsInPage(1),
        //                 QueryResult::PagePosts(vec![500, 600, 700, 800, 900]),
        //             )],
        //         );
        //     }
        // }

        // assert!(found);
    }
    #[tokio::test]
    pub async fn test_fields() {
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new(persisted_sender.clone(), query_result_sender);

        persisted_sender
            .send(vec![(5, Persisted::PostTitle("Zerg".into()), 1)])
            .unwrap();

        forum_minimal.advance_dataflow_computation_once().await;
        assert!(try_recv_contains(
            &mut query_result_receiver,
            vec![QueryResult::PostTitle("Zerg".into())]
        ));
    }
}
