use df_forum_frontend::persisted::Persisted;
use df_forum_frontend::query_result::QueryResult;

use std::cell::RefCell;
use std::rc::Rc;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;

use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Count;
// use differential_dataflow::operators::Join;
// use differential_dataflow::operators::Reduce;

#[derive(Clone)]
pub struct ForumMinimal {
    pub input: Rc<RefCell<InputSession<u64, (u64, Persisted), isize>>>,
    pub output: Rc<RefCell<Vec<QueryResult>>>,
    pub worker: Rc<RefCell<Worker<timely::communication::allocator::Thread>>>,
}

impl ForumMinimal {
    pub fn new(query_result_sender: broadcast::Sender<Vec<QueryResult>>) -> Self {
    // pub fn new() -> Self {
        let output0 = Rc::new(RefCell::new(Vec::new()));
        let output1 = output0.clone();
        let output2 = output0.clone();

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
                    .map(|_post| 1)
                    .count()
                    .inspect(move |((_one, count), _time, _diff)| {
                        println!("hello");
                        query_result_sender.send(vec![QueryResult::PostCount(*count as u64)]).unwrap();
                        // output1
                        //     .borrow_mut()
                        //     .push(QueryResult::PostCount(*count as u64))
                    });

                manages.inspect(move |((id, persisted), _time, _diff)| {
                    if let Persisted::Post {
                        title,
                        body,
                        user_id,
                        likes,
                    } = persisted
                    {
                        output2.borrow_mut().push(QueryResult::Post {
                            id: *id,
                            title: title.clone(),
                            body: body.clone(),
                            user_id: *user_id,
                            likes: *likes,
                        })
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
            output: output0,
            worker: worker0,
        }
    }

    pub fn submit_transaction(&mut self, persisted_items: Vec<(u64, Persisted)>) {
        let input0 = self.input.clone();
        let mut max_time = 0u64;
        for (time, item) in persisted_items {
            self.input.borrow_mut().insert((time, item));
            if time > max_time {
                max_time = time;
            }
        }
        input0.borrow_mut().advance_to(max_time);

        for _ in 0..100 {
            input0.borrow_mut().flush();
            self.worker.borrow_mut().step();
        }
    }
}

// #SPC-forum_minimal.aggregates_global_post_count
// #[test]
// pub fn aggregates_global_post_count() {
//     let mut forum_minimal = ForumMinimal::new();

//     let inputs = vec![
//         (
//             1u64,
//             Persisted::Post {
//                 title: "asdf".into(),
//                 body: "a".into(),
//                 user_id: 0,
//                 likes: 0,
//             },
//         ),
//         (
//             2u64,
//             Persisted::Post {
//                 title: "b".into(),
//                 body: "ba".into(),
//                 user_id: 0,
//                 likes: 0,
//             },
//         ),
//     ];

//     forum_minimal.submit_transaction(inputs);

//     let outputs: &Vec<QueryResult> = &*forum_minimal.output.borrow();
//     let outputs: Vec<&QueryResult> = outputs
//         .into_iter()
//         .filter(|output| {
//             if let QueryResult::PostCount(..) = output {
//                 true
//             } else {
//                 false
//             }
//         })
//         .collect();

//     assert_eq!(outputs, vec![&QueryResult::PostCount(2)]);
// }

#[tokio::test]
pub async fn test_channels() {
    let (tx, mut rx1) = broadcast::channel(16);

    let mut rx2: broadcast::Receiver<Vec<QueryResult>> = tx.subscribe();
    let mut forum_minimal = ForumMinimal::new(tx);
    // let mut forum_minimal = ForumMinimal::new();

    let inputs = vec![(
        1u64,
        Persisted::Post {
            title: "asdf".into(),
            body: "a".into(),
            user_id: 0,
            likes: 0,
        },
    )];

    forum_minimal.submit_transaction(inputs);

    tokio::spawn(async move {
        assert_eq!(rx2.recv().await.unwrap(), vec![QueryResult::PostCount(1)]);
    });

    sleep(Duration::from_millis(1)).await;
}
