extern crate differential_dataflow;
extern crate serde;
extern crate timely;
#[macro_use]
extern crate serde_derive;
extern crate console_error_panic_hook;

use std::cell::RefCell;
use std::rc::Rc;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;
use wasm_bindgen::prelude::*;

use differential_dataflow::input::Input;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Iterate;
use timely::dataflow::operators::capture::{Capture, EventCore, Extract};
use wasm_bindgen::JsCast;
use web_sys::{Document, Element, HtmlElement, Window};

use differential_dataflow::input::InputSession;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(contents: &str);
}

fn log1(contents: &str) {
    println!("{}", contents);
}

fn clickable_button() {}

#[wasm_bindgen]
pub fn run0() {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let worker_fn = move |worker: &mut Worker<Thread>| {
        let shared1 = Rc::new(RefCell::new(Vec::new()));
        let shared2 = shared1.clone();

        let mut input = worker.dataflow(|scope| {
            let (input, manages) = scope.new_collection();

            // let output = manages.filter(|&ev| ev == AppEvent::CountUp).capture();
            let _output = manages
                .filter(|&ev| ev == 0)
                .inspect(move |v| shared1.borrow_mut().push(format!("val: {:?}", v)));

            input
        });

        let mut time = 0;

        input.insert(0u64);
        input.advance_to(time);
        time += 1;

        log(&format!("got: {:?}", *shared2.borrow()));

        // input
        //     .borrow_mut()
        //     .insert(("session", "page", ("home_page_root", "g22")));
        // input.borrow_mut().advance_to(time);

        let window = web_sys::window().expect("could not get window");
        let document = window.document().expect("could not get document");
        let body = document
            .query_selector("body")
            .expect("could not get body")
            .unwrap();

        let val = document.create_element("button").unwrap();
        val.set_text_content(Some("rust says hi"));
        body.append_child(&val).unwrap();

        let clj = Closure::<dyn FnMut()>::new(move || {
            log("hello");
            log(&format!("got: {:?}", *shared2.borrow()));
            // TODO fix error that these two lines cause
            input.insert(time);
            input.advance_to(time);
            time += 1;
            log(&format!("t = {:?}", time));
        });

        let val2 = val.dyn_ref::<HtmlElement>().unwrap();
        val2.set_onclick(Some(clj.as_ref().unchecked_ref()));

        clj.forget();

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

    // Step on every JS Interval
    let step = Closure::<dyn FnMut()>::new(move || {
        worker.step();
    });

    let window = web_sys::window().expect("could not get window");
    let _ = window.set_interval_with_callback(step.as_ref().unchecked_ref());

    step.forget();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        run0();
    }
}
