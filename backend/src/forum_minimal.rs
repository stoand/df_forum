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

use differential_dataflow::AsCollection;
// use differential_dataflow::{Collection, ExchangeData};
// use timely::dataflow::operators::Filter;
use timely::dataflow::operators::Map;
// use timely::dataflow::*;

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

                // TODO: filter out deleted items
                let filter_out_deleted = manages.inspect(|_| {});

                let posts = filter_out_deleted.flat_map(move |(id, persisted)| {
                    match persisted {
                        Persisted::Post(post) => vec![(id, post)],
                        // WRONG
                        // TODO: do an antijoin on Delete elements with the id of a post
                        _ => vec![],
                    }
                });

                posts
                    // QueryResultAggregate::PostCount
                    .inspect(move |((id, persisted), _time, diff)| {
                        if *diff > 0 {
                            query_result_sender2
                                .send(vec![QueryResult::AddPost(*id, persisted.clone())])
                                .unwrap();
                        }
                    });

                posts
                    // .inspect(move |((_one, count), _time, diff)| {
                    //     println!("0. {:?}", ((_one, count), _time, diff));
                    // })
                    // .only_latest()
                    // .inspect(move |((_one, count), _time, diff)| {
                    //     println!("1. {:?}", ((_one, count), _time, diff));
                    // })
                    .inner
                    // .filter(|((_id, _persisted), time, diff)| *diff > 0)
                    // this de-dups multiple values, but sets the diff to
                    // to 2 or more if multiple duplicates are present
                    .map(|((_id, _persisted), time, diff)| (0, time, diff))
                    .as_collection()
                    .count()
                    .inspect(move |((_discarded_zero, count), _time, diff)| {
                        println!("inspect -- {:?}", ((0, count), _time, diff));

                        if *diff > 0 {
                            query_result_sender0
                                .send(vec![QueryResult::PostCount(*count as u64)])
                                .unwrap();
                        }
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

        for (id, item) in persisted_items {
            if item != Persisted::Deleted {
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

        let persisted_items = vec![(44, post0.clone()), (45, post1)];
        persisted_sender.clone().send(persisted_items).unwrap();

        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.recv().await.unwrap(),
            vec![QueryResult::PostCount(2)]
        );

        let remove_persisted_item = vec![(44, Persisted::Deleted)];
        persisted_sender
            .clone()
            .send(remove_persisted_item)
            .unwrap();
        forum_minimal.advance_dataflow_computation_once().await;

        assert_eq!(
            query_result_receiver.recv().await.unwrap(),
            vec![QueryResult::PostCount(1)]
        );
    }
}
