use df_forum_frontend::df_tuple_items::{Diff, Id, Time};
pub use df_forum_frontend::persisted::{Persisted, PersistedItems, Post};
use df_forum_frontend::query_result::QueryResult;

// use crate::operators::only_latest::OnlyLatest;

use std::cell::RefCell;
use std::rc::Rc;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;

use tokio::sync::broadcast;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Count;

pub type PersistedInputSession = InputSession<Time, (Id, Persisted), Diff>;

pub struct ForumMinimal {
    pub input: Rc<RefCell<PersistedInputSession>>,
    pub worker: Rc<RefCell<Worker<timely::communication::allocator::Thread>>>,
    pub persisted_receiver: broadcast::Receiver<PersistedItems>,
    pub dataflow_time: u64,
}

impl ForumMinimal {
    pub fn new(
        persisted_sender: broadcast::Sender<PersistedItems>,
        query_result_sender: broadcast::Sender<Vec<QueryResult>>,
    ) -> Self {
        let query_result_sender0 = query_result_sender.clone();
        let query_result_sender1 = query_result_sender.clone();
        let query_result_sender2 = query_result_sender.clone();

        let worker_fn = move |worker: &mut Worker<Thread>| {
            worker.dataflow(|scope| {
                let mut input: PersistedInputSession = InputSession::new();
                let manages = input.to_collection(scope);

                let posts = manages.flat_map(move |(id, persisted)| match persisted {
                    Persisted::Post(post) => vec![(id, post)],
                    _ => vec![],
                });

                posts.inspect_batch(move |_time, items| {
                    let mut results: Vec<QueryResult> = Vec::new();

                    for ((id, persisted), _time, diff) in items {
                        if *diff > 0 {
                            results.push(QueryResult::AddPost(*id, persisted.clone()));
                        } else {
                            results.push(QueryResult::DeletePersisted(*id));
                        }
                    }

                    query_result_sender2.send(results).unwrap();
                });

                manages
                    .flat_map(|(_id, persisted)| {
                        if let Persisted::PostTitle(_) = persisted {
                            vec![0]
                        } else {
                            vec![]
                        }
                    })
                    .count()
                    .inspect_batch(move |_time, items| {
                        let mut final_count = 0;

                        for ((_discarded_zero, count), _time, diff) in items {
                            if *diff > 0 {
                                final_count = *count as u64;
                            }
                        }
                        query_result_sender0
                            .send(vec![QueryResult::PostCount(final_count)])
                            .unwrap();
                    });

                input
            })
        };

        let alloc = Thread::new();
        let worker = Worker::new(WorkerConfig::default(), alloc);
        let worker0 = Rc::new(RefCell::new(worker.clone()));
        let worker1 = worker0.clone();
        let input = worker_fn(&mut worker1.borrow_mut());

        let input0: Rc<RefCell<InputSession<u64, (u64, Persisted), isize>>> =
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
        let persisted_items = self.persisted_receiver.recv().await.unwrap();

        self.dataflow_time += 1;

        for (id, item, diff) in persisted_items {
            if diff > 0 {
                self.input.borrow_mut().insert((id, item));
            } else {
                self.input.borrow_mut().remove((id, item));
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

    // #[tokio::test]
    // pub async fn test_capture() {
    //     let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
    //     let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

    //     let mut forum_minimal = ForumMinimal::new(persisted_sender.clone(), query_result_sender);

    //     let gen_post = |id, title: &str, body: &str| {
    //         vec![
    //             (id, Persisted::PostTitle(title.into()), 1),
    //             (id, Persisted::PostBody(body.into()), 1),
    //             (id, Persisted::PostUserId(0), 1),
    //             (id, Persisted::PostLikes(0), 1),
    //         ]
    //     };

    //     let post0 = gen_post(10, "a", "b");

    //     persisted_sender.clone().send(post0).unwrap();

    //     forum_minimal.advance_dataflow_computation_once().await;

    //     // let handle = forum_minimal.handle0.borrow();

    //     // loop {
    //     //     if let Some(event_link) = handle.next {
    //     //     }
    //     // }
    //     // assert!(try_recv_contains(
    //     //     &mut query_result_receiver,
    //     //     vec![QueryResult::PostCount(0)]
    //     // ));
    // }
}
