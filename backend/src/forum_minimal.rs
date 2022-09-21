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

#[derive(Clone)]
pub struct ForumMinimal {
    pub input: Rc<RefCell<InputSession<u64, (u64, Persisted), isize>>>,
    pub output: Rc<RefCell<Vec<QueryResult>>>,
    pub worker: Rc<RefCell<Worker<timely::communication::allocator::Thread>>>,
    time: u64,
}

impl ForumMinimal {
    pub fn new() -> Self {
        let output0 = Rc::new(RefCell::new(Vec::new()));
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
                        output1
                            .borrow_mut()
                            .push(QueryResult::PostCount(*count as u64))
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
            output: output0,
            time: 0,
            worker: worker0,
        }
    }

    pub fn submit_transaction(&mut self, persisted_items: Vec<Persisted>) {
        let input0 = self.input.clone();
        for item in persisted_items {
            self.input.borrow_mut().insert((10u64, item));
        }

        let mut this = &mut *self;
        this.time += 1;
        
        input0.borrow_mut().advance_to(self.time);

        for _ in 0..100 {
            input0.borrow_mut().flush();
            self.worker.borrow_mut().step();
        }
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
