use df_forum_frontend::persisted::Persisted;
use df_forum_frontend::query_result::QueryResult;

use std::cell::RefCell;
use std::rc::Rc;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Count;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForumMinimal {
    pub state: Vec<Persisted>,
    pub connection_count: u64,
}

impl ForumMinimal {
    pub fn new() -> Self {
        ForumMinimal {
            state: Vec::new(),
            connection_count: 0,
        }
    }
    pub fn say_hi(&mut self) {
        self.connection_count += 1;
        println!("forum_minimal says hi, count = {}", self.connection_count);
    }

    pub fn new_persisted_transaction(&mut self, persisted_items: Vec<Persisted>) {}

    pub fn compute_forum(
        &mut self,
        output0: Rc<RefCell<Vec<QueryResult>>>,
    ) -> Rc<RefCell<InputSession<u64, (u64, Persisted), isize>>> {
        let output1 = output0.clone();

        let worker_fn = move |worker: &mut Worker<Thread>| {
            worker.dataflow(|scope| {
                let mut input: InputSession<u64, (u64, Persisted), isize> = InputSession::new();
                let manages = input.to_collection(scope);

                manages
                    .filter(move |(_id, persisted)| {
                        if let Persisted::Post { .. } = persisted {
                            true
                        } else {
                            false
                        }
                    })
                    .count()
                    .inspect(move |(((_id, persisted), count), _time, _diff)| {
                        output1.borrow_mut().push(QueryResult::PostCount(*count as u64))
                    });

                input
            })
        };

        let alloc = Thread::new();
        let mut worker = Worker::new(WorkerConfig::default(), alloc);
        let input = worker_fn(&mut worker);

        let input0: Rc<RefCell<InputSession<u64, (u64, Persisted), isize>>> =
            Rc::new(RefCell::new(input));
        let input1 = input0.clone();

        input0.borrow_mut().insert((
            10u64,
            Persisted::Post {
                title: "asdf".into(),
                body: "body0".into(),
                user_id: 20,
                likes: 0,
            },
        ));
        input0.borrow_mut().advance_to(1u64);

        let mut go = move || {
            for _ in 0..10 {
                input0.borrow_mut().flush();
                worker.step();
            }
        };

        go();

        input1
    }

    // #SPC-forum_minimal.create_post
    // pub fn create_post(data: String) {
    // }
}

// // #SPC-forum_minimal.aggregates_global_post_count
// pub fn aggregates_global_post_count() {
// }

// #[test]
// pub fn test_aggregates_global_post_count() {
// }
