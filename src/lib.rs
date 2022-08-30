extern crate differential_dataflow;
extern crate timely;

use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;
use wasm_bindgen::prelude::*;

use differential_dataflow::input::Input;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::Iterate;
// use js_sys::{Array, Date};
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
    fn log(contents: &str);
}

fn clickable_button() {}

#[wasm_bindgen]
pub fn run0() {
    let worker_fn = move |worker: &mut Worker<Thread>| {
        let mut input = InputSession::new();

        worker.dataflow(|scope| {
            let manages = input.to_collection(scope);

            manages
                .filter(|&tup| match tup {
                    ("session", "page", ("home_page_root", _attach_to)) => true,
                    _ => false,
                })
                .inspect(|((_el, _at, (_val0, attach_to)), time, add_rm)| {
                    log(&format!(
                        "{} home page to element with id: {:?}; at time {:?}",
                        add_rm_str(add_rm),
                        attach_to,
                        time
                    ));
                });
        });

        let mut time = 0;

        time += 1;

        input.insert(("session", "page", ("home_page_root", time)));
        input.advance_to(time);
        

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
            input.insert(("session", "page", ("home_page_root", time)));
            time += 1;
            input.advance_to(time);
            log("hello");
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
