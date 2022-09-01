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
            let _output = manages
                // .filter(|&ev| ev == 0)
                .inspect(move |v| output1.borrow_mut().push(format!("output0 = {:?}", v)));
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
    let mut input = worker_fn(&mut worker);

    let mut time = 0;
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
        input.insert(time + 3000);
        input.advance_to(time);
        input.flush();
        time += 1;
        log(&format!("t = {:?}", time));
        worker.step();

        log(&format!("output0 after step = {:?}", output0.borrow()));
    });

    let val2 = val.dyn_ref::<HtmlElement>().unwrap();
    val2.set_onclick(Some(clj.as_ref().unchecked_ref()));

    clj.forget();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        run0();
    }
}
