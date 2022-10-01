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

use std::sync::{Arc, Mutex};
use timely::dataflow::operators::capture::{EventLink, Extract, Replay, EventWriter};
use timely::dataflow::operators::{Capture, Inspect, ToStream};
use timely::dataflow::Scope;


use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

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

                let path = Path::new("/tmp/asdf");

                let mut file = File::create(&path).unwrap();
                
                manages.inner.capture_into(EventWriter::new(file));

                let posts = manages.flat_map(move |(id, persisted)| match persisted {
                    Persisted::Post(post) => vec![(id, post)],
                    // _ => vec![],
                });

                posts.inspect(move |((id, persisted), _time, diff)| {
                    if *diff > 0 {
                        query_result_sender2
                            .send(vec![QueryResult::AddPost(*id, persisted.clone())])
                            .unwrap();
                    }
                });

                posts
                    .map(|(_id, _persisted)| 0)
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

                manages.inspect(move |((id, _persisted), _time, diff)| {
                    if *diff < 0 {
                        query_result_sender1
                            .send(vec![QueryResult::DeletePersisted(*id)])
                            .unwrap();
                    }
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

        let post0 = Persisted::Post(Post {
            title: "a".into(),
            body: "a".into(),
            user_id: 0,
            likes: 0,
        });

        let post1 = Persisted::Post(Post {
            title: "b".into(),
            body: "b".into(),
            user_id: 0,
            likes: 0,
        });

        let persisted_items = vec![(44, post0.clone(), 1), (45, post1.clone(), 1)];
        persisted_sender.clone().send(persisted_items).unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert!(try_recv_contains(
            &mut query_result_receiver,
            vec![QueryResult::PostCount(2)]
        ),);

        let remove_persisted_item = vec![(44, post0.clone(), -1), (45, post1.clone(), -1)];
        persisted_sender
            .clone()
            .send(remove_persisted_item)
            .unwrap();
        forum_minimal.advance_dataflow_computation_once().await;

        assert!(try_recv_contains(
            &mut query_result_receiver,
            vec![QueryResult::PostCount(0)]
        ));
    }

    #[tokio::test]
    pub async fn test_capture() {
        let (query_result_sender, mut query_result_receiver) = broadcast::channel(16);
        let (persisted_sender, _persisted_receiver) = broadcast::channel(16);

        let mut forum_minimal = ForumMinimal::new(persisted_sender.clone(), query_result_sender);

        let post0 = Persisted::Post(Post {
            title: "a".into(),
            body: "a".into(),
            user_id: 0,
            likes: 0,
        });

        let persisted_items = vec![(44, post0.clone(), 1)];
        persisted_sender.clone().send(persisted_items).unwrap();

        forum_minimal.advance_dataflow_computation_once().await;


        // let handle = forum_minimal.handle0.borrow();

        // loop {
        //     if let Some(event_link) = handle.next {
        //     }
        // }
    }
}
