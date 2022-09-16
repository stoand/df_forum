extern crate differential_dataflow;
extern crate serde;
extern crate timely;
#[macro_use]
extern crate serde_derive;
extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate df_forum_backend;

use df_forum_backend::operators::only_latest::OnlyLatest;

use std::cell::RefCell;
use std::rc::Rc;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Count;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;

#[test]
fn count_basic() {
    let output0 = Rc::new(RefCell::new(Vec::new()));
    let output1 = output0.clone();

    let worker_fn = move |worker: &mut Worker<Thread>| {
        worker.dataflow(|scope| {
            let mut input = InputSession::new();
            let manages = input.to_collection(scope);

            manages
                .count()
                .inspect(move |v| output0.borrow_mut().push(*v));

            input
        })
    };

    let alloc = Thread::new();
    let mut worker = Worker::new(WorkerConfig::default(), alloc);
    let input = worker_fn(&mut worker);

    let input0 = Rc::new(RefCell::new(input));
    let input1 = input0.clone();
    input0.borrow_mut().insert(80u32);
    input0.borrow_mut().advance_to(1u32);

    let mut go = move || {
        for _ in 0..10 {
            input0.borrow_mut().flush();
            worker.step();
        }
    };

    go();

    assert_eq!(*output1.borrow(), vec![((80, 1), 0, 1)]);
    input1.borrow_mut().insert(80u32);
    input1.borrow_mut().insert(70u32);
    input1.borrow_mut().advance_to(2u32);

    go();
    assert_eq!(
        *output1.borrow(),
        vec![
            ((80, 1), 0, 1),
            ((70, 1), 1, 1),
            ((80, 1), 1, -1),
            ((80, 2), 1, 1),
        ]
    );
}
#[test]
fn reduce_least() {
    let output0 = Rc::new(RefCell::new(Vec::new()));
    let output1 = output0.clone();

    let worker_fn = move |worker: &mut Worker<Thread>| {
        worker.dataflow(|scope| {
            let mut input = InputSession::new();
            let manages = input.to_collection(scope);

            manages
                // return least element
                .reduce(|_key, input, output| {
                    // log(&format!(
                    //     "key = {:?}, input = {:?}, output = {:?}",
                    //     key, input, output
                    // ));
                    output.push((*input[0].0, 1));
                })
                .inspect(move |v| output0.borrow_mut().push(*v));

            input
        })
    };

    let alloc = Thread::new();
    let mut worker = Worker::new(WorkerConfig::default(), alloc);
    let input = worker_fn(&mut worker);

    let input0 = Rc::new(RefCell::new(input));
    let input1 = input0.clone();
    input0.borrow_mut().insert((80, 5));
    input0.borrow_mut().insert((80, 3));
    input0.borrow_mut().insert((80, 8));
    input0.borrow_mut().insert((70, 90));
    input0.borrow_mut().advance_to(1u32);

    let mut go = move || {
        for _ in 0..10 {
            input0.borrow_mut().flush();
            worker.step();
        }
    };

    go();

    assert_eq!(*output1.borrow(), vec![((70, 90), 0, 1), ((80, 3), 0, 1)]);
    input1.borrow_mut().insert((80, 2));
    input1.borrow_mut().insert((70, 100));
    input1.borrow_mut().advance_to(2u32);

    go();
    assert_eq!(
        *output1.borrow(),
        vec![
            ((70, 90), 0, 1),
            ((80, 3), 0, 1),
            ((80, 2), 1, 1),
            ((80, 3), 1, -1),
        ]
    );
    input1.borrow_mut().remove((70, 90));
    input1.borrow_mut().advance_to(3u32);
    go();
    assert_eq!(
        *output1.borrow(),
        vec![
            ((70, 90), 0, 1),
            ((80, 3), 0, 1),
            ((80, 2), 1, 1),
            ((80, 3), 1, -1),
            ((70, 90), 2, -1),
            ((70, 100), 2, 1),
        ]
    );
}

// compute total post likes
#[test]
fn aggregation() {

    #[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    enum Persisted {
        Session {
            token: String,
            user_id: u64,
        },
        User {
            name: String,
        },
        Post {
            title: String,
            user_id: u64,
            likes: u64,
        },
        Deleted,
    }

    let output0 = Rc::new(RefCell::new(Vec::new()));
    let output1 = output0.clone();

    let session_token = "9al132kff";

    let worker_fn = move |worker: &mut Worker<Thread>| {
        worker.dataflow(|scope| {
            let mut input = InputSession::new();
            let manages = input.to_collection(scope);

            let current_session_user_id = manages.flat_map(move |(_id, persisted)| {
                if let Persisted::Session { user_id, token } = persisted {
                    if token == session_token {
                        vec![(user_id, ())]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            });
            let belonging_posts = manages.only_latest().flat_map(|(_id, persisted)| {
                if let Persisted::Post { user_id, .. } = persisted {
                    vec![(user_id, persisted)]
                } else {
                    vec![]
                }
            });

            let joined_user_session = current_session_user_id.join(&belonging_posts);

            let users_total_post_likes = joined_user_session.reduce(|_key, inputs, outputs| {
                // log(&format!(
                //     "key = {:?}, input = {:?}, output = {:?}",
                //     _key, inputs, outputs
                // ));
                let mut total_likes = 0;

                for (persisted, diff) in inputs {
                    if *diff > 0 {
                        if let Persisted::Post { likes, .. } = persisted.1 {
                            total_likes += likes;
                        }
                    }
                }

                outputs.push((total_likes, 1));
            });

            users_total_post_likes.inspect(move |v| output0.borrow_mut().push(*v));

            input
        })
    };

    let alloc = Thread::new();
    let mut worker = Worker::new(WorkerConfig::default(), alloc);
    let input = worker_fn(&mut worker);

    let input0 = Rc::new(RefCell::new(input));
    let input1 = input0.clone();

    input0.borrow_mut().insert((
        55,
        Persisted::Session {
            token: session_token.into(),
            user_id: 3,
        },
    ));
    input0.borrow_mut().insert((
        56,
        Persisted::Session {
            token: session_token.into(),
            user_id: 77,
        },
    ));
    input0
        .borrow_mut()
        .insert((3, Persisted::User { name: "Joe".into() }));
    input0.borrow_mut().insert((
        77,
        Persisted::User {
            name: "Frank".into(),
        },
    ));

    input0.borrow_mut().insert((
        29,
        Persisted::Post {
            title: "Protoss".into(),
            user_id: 77,
            likes: 81,
        },
    ));
    input0.borrow_mut().insert((
        10,
        Persisted::Post {
            title: "Terran".into(),
            user_id: 3,
            likes: 5,
        },
    ));
    input0.borrow_mut().insert((
        11,
        Persisted::Post {
            title: "Zerg".into(),
            user_id: 3,
            likes: 3,
        },
    ));
    input0.borrow_mut().insert((
        12,
        Persisted::Post {
            title: "Aliens".into(),
            user_id: 2,
            likes: 32,
        },
    ));
    input0.borrow_mut().advance_to(1u32);

    let mut go = move || {
        for _ in 0..10 {
            input0.borrow_mut().flush();
            worker.step();
        }
    };

    go();

    let user_id0 = 3;
    let total_likes0 = 8;

    let user_id1 = 77;
    let total_likes1 = 81;

    assert_eq!(
        *output1.borrow(),
        vec![
            ((user_id0, total_likes0), 0, 1),
            ((user_id1, total_likes1), 0, 1)
        ]
    );
    input1.borrow_mut().insert((
        11,
        Persisted::Post {
            title: "Zerg".into(),
            user_id: 3,
            likes: 9,
        },
    ));
    input1.borrow_mut().advance_to(2u32);

    go();
    assert_eq!(
        *output1.borrow(),
        vec![
            ((3, 8), 0, 1),
            ((77, 81), 0, 1),
            ((3, 8), 1, -1),
            ((3, 14), 1, 1),
        ]
    );
}

#[test]
fn aggregation_replacement() {

    #[derive(
        Hash,
        Clone,
        Copy,
        Debug,
        Serialize,
        Deserialize,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Abomonation,
    )]
    enum Persisted {
        Post { user_id: u64, likes: u64 },
        Deleted,
    }
    let output0_posts = Rc::new(RefCell::new(Vec::new()));
    let output1_posts = output0_posts.clone();
    let output0_like_totals = Rc::new(RefCell::new(Vec::new()));
    let output1_like_totals = output0_like_totals.clone();

    let worker_fn = move |worker: &mut Worker<Thread>| {
        worker.dataflow(|scope| {
            let mut input = InputSession::new();
            let manages = input.to_collection(scope);

            let filter_newest = manages
                .only_latest()
                // .inspect(|v| {
                //     log(&format!("v = {:?}", v));
                // });
                .inspect(move |v| output0_posts.borrow_mut().push(*v));

            let _total_likes = filter_newest
                .flat_map(|(_id, persisted)| {
                    if let Persisted::Post { user_id, likes } = persisted {
                        vec![(user_id, likes)]
                    } else {
                        vec![]
                    }
                })
                .reduce(|_key, inputs, outputs| {
                    // log(&format!(
                    //     "key = {:?}, input = {:?}, output = {:?}",
                    //     _key, inputs, outputs
                    // ));
                    let mut total_likes = 0;

                    for item in inputs {
                        total_likes += item.0;
                    }
                    outputs.push((total_likes, 1));
                })
                // .inspect(|v| {
                //     log(&format!("v = {:?}", v));
                // });
                .inspect(move |v| output0_like_totals.borrow_mut().push(*v));

            input
        })
    };

    let alloc = Thread::new();
    let mut worker = Worker::new(WorkerConfig::default(), alloc);
    let input = worker_fn(&mut worker);

    let input0 = Rc::new(RefCell::new(input));
    let input1 = input0.clone();

    let post10 = Persisted::Post {
        user_id: 20,
        likes: 8,
    };

    let post11 = Persisted::Post {
        user_id: 21,
        likes: 17,
    };

    input0.borrow_mut().insert((10, post10));
    input0.borrow_mut().insert((11, post11));
    input0.borrow_mut().advance_to(1u64);

    let mut go = move || {
        for _ in 0..10 {
            input0.borrow_mut().flush();
            worker.step();
        }
    };

    go();

    assert_eq!(
        *output1_posts.borrow(),
        vec![((10, post10), 0, 1), ((11, post11), 0, 1)]
    );
    assert_eq!(
        *output1_like_totals.borrow(),
        vec![((20, 8), 0, 1), ((21, 17), 0, 1)]
    );

    let post10_updated = Persisted::Post {
        user_id: 20,
        likes: 3,
    };

    input1.borrow_mut().insert((10, post10_updated));
    input1.borrow_mut().advance_to(2u64);

    go();
    assert_eq!(
        *output1_posts.borrow(),
        vec![
            ((10, post10), 0, 1),
            ((11, post11), 0, 1),
            ((10, post10), 1, -2),
            ((10, post10_updated), 1, 1),
        ]
    );

    assert_eq!(
        *output1_like_totals.borrow(),
        vec![
            ((20, 8), 0, 1),
            ((21, 17), 0, 1),
            ((20, 8), 1, -1),
            ((20, 11), 1, 1),
        ]
    );
}
