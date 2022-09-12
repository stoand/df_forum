extern crate differential_dataflow;
extern crate serde;
extern crate timely;
#[macro_use]
extern crate serde_derive;
extern crate abomonation;
extern crate console_error_panic_hook;
#[macro_use]
extern crate abomonation_derive;
extern crate wasm_bindgen_test;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

use std::cell::RefCell;
use std::rc::Rc;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;
use wasm_bindgen::prelude::*;

// use differential_dataflow::input::Input;
// use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::Count;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
// use timely::dataflow::operators::capture::{Capture, EventCore, Extract};
use wasm_bindgen::JsCast;
// use web_sys::{Document, Element, HtmlElement, Window};
use web_sys::HtmlElement;

use differential_dataflow::input::InputSession;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(contents: &str);
}

// only works for V8 based javascript engines (chrome, node - not firefox)
#[wasm_bindgen(
    inline_js = "export function lower_stack_trace_size() { Error.stackTraceLimit = 2; }"
)]
extern "C" {
    fn lower_stack_trace_size();
}

#[wasm_bindgen]
pub fn run0() {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));

    let output0 = Rc::new(RefCell::new(Vec::new()));
    let output1 = output0.clone();
    let worker_fn = move |worker: &mut Worker<Thread>| {
        worker.dataflow(|scope| {
            let mut input = InputSession::new();
            let manages = input.to_collection(scope);

            // let output = manages.filter(|&ev| ev == AppEvent::CountUp).capture();
            manages
                // .filter(|&ev| ev == 0)
                .inspect(move |v| output1.borrow_mut().push(format!("{:?}", v)))
                // .map(|v| (StateKey::Count, v))
                // .map(|v| (1, v))
                // .reduce(|key, input, output| {
                //     log(&format!(
                //         "key = {:?}, input = {:?}, output = {:?}",
                //         key, input, output
                //     ));
                //     // let change = if *key == AppEvent::CountUp { 33 } else { -55 };
                //     // output.push((change, change));
                //     for item in input {
                //         let (&a, b) = item;
                //         output.push((a, *b));
                //     }
                // })
                .count()
                .reduce(|key, input, output| {
                    log(&format!(
                        "key = {:?}, input = {:?}, output = {:?}",
                        key, input, output
                    ));

                    let mut min_index = 0;

                    for index in 1..input.len() {
                        if input[min_index].0 > input[index].0 {
                            min_index = index;
                        }
                    }
                    output.push((*input[min_index].0, 1));
                })
                // .reduce(move |_key, s, t| {
                //     t.push((s[0].1.clone(), 1));
                // })
                .inspect(|res| log(&format!("count = {:?}", res)));
            input
        })

        // index load page event fires on first load
        // input.insert(("session", "page", ("home_page_root", "body")));
        // input.advance_to(1);

        // ON_HOME_SIGN_IN
        // these events are triggered on a button press
        // event inside the home page
        // input.remove(("session", "page", ("home_page_root", "02312")));
        // input.insert(("session", "sign_in", ("name", "user0123")));
        // input.insert(("session", "sign_in", ("date", "10-08-2022")));
        // input.insert(("session", "page", ("posts_page_root", "02312")));
        // input.advance_to(2);

        // // ON_POST_CREATED
        // input.insert(("rnd-post-id", "post", ("title", "hello im a new post")));
        // input.insert(("rnd-post-id", "post", ("body", "post body goes here")));
        // input.advance_to(3);
    };

    let alloc = Thread::new();
    let mut worker = Worker::new(WorkerConfig::default(), alloc);
    let input = worker_fn(&mut worker);

    let window = web_sys::window().expect("could not get window");
    let document = window.document().expect("could not get document");
    let body = document
        .query_selector("body")
        .expect("could not get body")
        .unwrap();

    let count_up = document.create_element("button").unwrap();
    count_up.set_text_content(Some("count up"));
    body.append_child(&count_up).unwrap();

    let count_down = document.create_element("button").unwrap();
    count_down.set_text_content(Some("count down"));
    body.append_child(&count_down).unwrap();

    let input0 = Rc::new(RefCell::new(input));
    let worker0 = Rc::new(RefCell::new(worker));

    let input1 = input0.clone();
    let input2 = input0.clone();
    let worker1 = worker0.clone();
    let time0 = Rc::new(RefCell::new(0));
    let time1 = time0.clone();
    let time2 = time0.clone();
    input0.borrow_mut().advance_to(0);

    let count_up_clj = Closure::<dyn FnMut()>::new(move || {
        log("inserting");
        log(&format!("t0 = {:?}", time0.borrow()));
        input0.borrow_mut().insert(32);
    });

    let count_up_el = count_up.dyn_ref::<HtmlElement>().unwrap();
    count_up_el.set_onclick(Some(count_up_clj.as_ref().unchecked_ref()));

    count_up_clj.forget();
    let count_down_clj = Closure::<dyn FnMut()>::new(move || {
        log("inserting");
        log(&format!("t1 = {:?}", time1.borrow()));
        input1.borrow_mut().insert(34u64);
    });

    let count_down_el = count_down.dyn_ref::<HtmlElement>().unwrap();
    count_down_el.set_onclick(Some(count_down_clj.as_ref().unchecked_ref()));

    count_down_clj.forget();

    let commit = document.create_element("button").unwrap();
    commit.set_text_content(Some("commmit"));
    body.append_child(&commit).unwrap();

    let commit_clj = Closure::<dyn FnMut()>::new(move || {
        log("committing");
        *time2.borrow_mut() += 1;
        input2.borrow_mut().advance_to(*time2.borrow());

        log(&format!("t1 = {:?}", time2.borrow()));
        for _ in 0..100 {
            input2.borrow_mut().flush();
            worker1.borrow_mut().step();
        }
    });

    let commit_el = commit.dyn_ref::<HtmlElement>().unwrap();
    commit_el.set_onclick(Some(commit_clj.as_ref().unchecked_ref()));

    commit_clj.forget();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[wasm_bindgen_test]
    fn count_basic() {
        lower_stack_trace_size();
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
    #[wasm_bindgen_test]
    fn reduce_least() {
        lower_stack_trace_size();
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

    // compute total post likes
    #[wasm_bindgen_test]
    fn app_state_reduce_aggregation() {
        lower_stack_trace_size();
        let output0 = Rc::new(RefCell::new(Vec::new()));
        let output1 = output0.clone();

        let session_token: String = "3k21f0".into();

        let worker_fn = move |worker: &mut Worker<Thread>| {
            worker.dataflow(|scope| {
                let mut input = InputSession::new();
                let manages = input.to_collection(scope);
                // .filter(|(_, persisted)| persisted != Persisted::Deleted);
                // take a session, get the user from that session, get posts from that user
                // get total likes from those posts

                // possibly order items by time
                // we want:
                // key = 3, User { Joe } , User { Doe } , Deleted
                use differential_dataflow::trace::implementations::ord::*;

                let filter_newest = manages
                    // todo separate out newest for every id
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
                        "asdf",
                        move |key, input, outputs| {
                            log(&format!(
                                "key = {:?}, input = {:?}, output = {:?}",
                                key, input, outputs
                            ));

                            outputs.push((input.len(), 1));
                            // outputs.push((*input[0].0, 1));
                        },
                    )
                    .as_collection(|&a, &b| (a, b))
                    .inspect(|v| {
                        log(&format!("v = {:?}", v));
                    });

                let current_session_user_id = manages.flat_map(move |(_id, persisted)| {
                    if let Persisted::Session { user_id, token } = persisted {
                        if *token == session_token {
                            vec![(user_id, ())]
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    }
                });
                let belonging_posts = manages.flat_map(|(_id, persisted)| {
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

                    for (persisted, _) in inputs {
                        if let Persisted::Post { likes, .. } = persisted.1 {
                            total_likes += likes;
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
                token: "3k21f0".into(),
                user_id: 3,
            },
        ));
        input0.borrow_mut().insert((
            56,
            Persisted::Session {
                token: "3k21f0".into(),
                user_id: 77,
            },
        ));
        input0
            .borrow_mut()
            .insert((3, Persisted::User { name: "Joe".into() }));
        input0
            .borrow_mut()
            .insert((77, Persisted::User { name: "Joe".into() }));

        input0.borrow_mut().insert((
            29,
            Persisted::Post {
                title: "other_user".into(),
                user_id: 77,
                likes: 81,
            },
        ));
        input0.borrow_mut().insert((
            10,
            Persisted::Post {
                title: "asdf".into(),
                user_id: 3,
                likes: 5,
            },
        ));
        input0.borrow_mut().insert((
            11,
            Persisted::Post {
                title: "other".into(),
                user_id: 3,
                likes: 3,
            },
        ));
        input0.borrow_mut().insert((
            12,
            Persisted::Post {
                title: "ignore_this".into(),
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
        input1.borrow_mut().insert((3, Persisted::Deleted));
        input1
            .borrow_mut()
            .insert((3, Persisted::User { name: "Doe".into() }));
        input1.borrow_mut().remove((
            11,
            Persisted::Post {
                title: "other".into(),
                user_id: 3,
                likes: 3,
            },
        ));
        // todo update post likes
        input1.borrow_mut().insert((
            11,
            Persisted::Post {
                title: "other".into(),
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
}
