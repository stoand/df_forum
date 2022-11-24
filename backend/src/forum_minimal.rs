use df_forum_frontend::df_tuple_items::{Diff, Id, Time};
pub use df_forum_frontend::persisted::{Persisted, PersistedItems, Post};
pub use df_forum_frontend::query_result::QueryResult;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;

use log::debug;
use std::fmt::Debug;
use std::net::SocketAddr;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;

use differential_dataflow::input::InputSession;

use crate::dataflows::page_post_ids::posts_post_ids_dataflow;
use crate::dataflows::post_aggr::post_aggr_dataflow;

pub type InputFormat = (SocketAddr, (Id, Persisted));

pub type PersistedInputSession = InputSession<Time, InputFormat, Diff>;

pub const POSTS_PER_PAGE: usize = 2;

pub struct ForumMinimal {
    pub input: Rc<RefCell<PersistedInputSession>>,
    pub worker: Rc<RefCell<Worker<timely::communication::allocator::Thread>>>,
    pub persisted_receiver: broadcast::Receiver<(SocketAddr, PersistedItems)>,
    pub dataflow_time: u64,
}

type ScopeThread = timely::communication::allocator::Thread;
type ScopeWorker = timely::worker::Worker<ScopeThread>;
type ScopeChild<'a> = timely::dataflow::scopes::Child<'a, ScopeWorker, u64>;
pub type ScopeCollection<'a> = differential_dataflow::Collection<ScopeChild<'a>, InputFormat>;

pub type QueryResultSender = broadcast::Sender<(SocketAddr, Vec<QueryResult>)>;

pub fn default_dataflows<'a>(
    collection: &ScopeCollection<'a>,
    query_result_sender: QueryResultSender,
) {
    posts_post_ids_dataflow(collection, query_result_sender.clone());
    post_aggr_dataflow(collection, query_result_sender.clone());
}

impl ForumMinimal {
    pub fn new(
        persisted_sender: broadcast::Sender<(SocketAddr, PersistedItems)>,
        query_result_sender: broadcast::Sender<(SocketAddr, Vec<QueryResult>)>,
    ) -> Self {
        Self::new_with_dataflows(persisted_sender, query_result_sender, default_dataflows)
    }

    pub fn new_with_dataflows<F: for<'a> Fn(&ScopeCollection<'a>, QueryResultSender)>(
        persisted_sender: broadcast::Sender<(SocketAddr, PersistedItems)>,
        query_result_sender: broadcast::Sender<(SocketAddr, Vec<QueryResult>)>,
        init_dataflows: F,
    ) -> Self {
        let worker_fn = move |worker: &mut Worker<Thread>| {
            worker.dataflow(|scope| {
                let mut input: PersistedInputSession = InputSession::new();
                let collection = input.to_collection(scope);

                init_dataflows(&collection, query_result_sender.clone());

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

        for _ in 0..1000 {
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
pub fn batch_send(
    query_results_aug: &[(Vec<(SocketAddr, QueryResult)>, u64, isize)],
    query_result_sender: &Sender<(SocketAddr, Vec<QueryResult>)>,
) {
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
}
