extern crate differential_dataflow;
extern crate serde;
extern crate timely;
#[macro_use]
extern crate serde_derive;

use std::cell::RefCell;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;
use wasm_bindgen::prelude::*;

use differential_dataflow::input::Input;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Iterate;
use timely::dataflow::operators::capture::{Capture, Extract, EventCore};
use wasm_bindgen::JsCast;
use web_sys::{Document, Element, HtmlElement, Window};

use differential_dataflow::input::InputSession;

fn add_rm_str(val: &isize) -> String {
    if *val == 1 {
        "add".into()
    } else {
        "rm".into()
    }
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log1(contents: &str);
}

fn log(contents: &str) {
    println!("{}", contents);
}

fn clickable_button() {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AppState {
    count: u32,
}

// #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord)]
// enum AppEvent {
//     CountUp,
//     CountDown,
// }

#[wasm_bindgen]
pub fn run0() {

    
    
    let worker_fn = move |worker: &mut Worker<Thread>| {
        let (mut input, output) = worker.dataflow(|scope| {
            let (input, manages) = scope.new_collection();

            // let output = manages.filter(|&ev| ev == AppEvent::CountUp).capture();
            let output = manages.filter(|&ev| ev == 0).inner.capture();

            (input, output)
        });

        input.insert(0u64);
        input.advance_to(1u32);
        
        input.advance_to(2u32);
        input.close();

        // if let Ok(EventCore::Messages(v0, v1)) = output.try_recv() {
        if let Ok(_something) = output.try_recv() {
            println!("got something");
            // println!("got! {:?}", v0);               
        }

        // println!("got: {:?}", output.extract());
        // let mut input = InputSession::new();

        // worker.dataflow(|scope| {
        //     let manages = input.to_collection(scope);

        //     manages
        //         .inspect(|&tup| log(&format!("all -- {:?}", tup)))
        //         .filter(|&tup| match tup {
        //             ("session", "page", ("home_page_root", _attach_to)) => true,
        //             _ => false,
        //         })
        //         .inspect(move |((_el, _at, (_val0, attach_to)), time, add_rm)| {
        //             log(&format!(
        //                 "{} home page to element with id: {:?}; at time {:?}",
        //                 add_rm_str(add_rm),
        //                 attach_to,
        //                 time
        //             ));
        //         });
        // });

        // input.insert(("session", "page", ("home_page_root2", "asdf")));
        // input.advance_to(2usize);

        // TODO check:
        // https://timelydataflow.github.io/timely-dataflow/chapter_4/chapter_4_4.html

        // let mut time = 0;

        // time += 1;

        // input
        //     .borrow_mut()
        //     .insert(("session", "page", ("home_page_root", "g22")));
        // input.borrow_mut().advance_to(time);

        // let window = web_sys::window().expect("could not get window");
        // let document = window.document().expect("could not get document");
        // let body = document
        //     .query_selector("body")
        //     .expect("could not get body")
        //     .unwrap();

        // let val = document.create_element("button").unwrap();
        // val.set_text_content(Some("rust says hi"));
        // body.append_child(&val).unwrap();

        // let clj = Closure::<dyn FnMut()>::new(move || {
        //     input.insert(("session", "page", ("home_page_root", time)));
        //     time += 1;
        //     input.advance_to(time);
        //     log("hello");
        // });

        // let val2 = val.dyn_ref::<HtmlElement>().unwrap();
        // val2.set_onclick(Some(clj.as_ref().unchecked_ref()));

        // clj.forget();
        //
        //

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
    let result = worker_fn(&mut worker);
    while worker.step_or_park(None) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        run0();
    }
}
