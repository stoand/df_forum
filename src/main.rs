extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::InputSession;

// #[derive(Clone,Debug,PartialEq, Eq, PartialOrd, Ord)]
// enum EventType {
//     Stuff0,
//     Stuff1,
// }

fn main() {
    run0();
}

fn add_rm_str(val: &isize) -> String {
    if *val == 1 {
        "add".into()
    } else {
        "rm".into()
    }
}

fn run0() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();

        worker.dataflow(|scope| {
            let manages = input.to_collection(scope);

            // manages
            //     // .map(|(m2, m1)| (m1, m2))
            //     // .join(&manages)
            //     .inspect(|x| println!("  -- {:?}", x));

            manages
                .filter(|&tup| match tup {
                    ("session", "page", ("home_page_root", _attach_to)) => true,
                    _ => false,
                })
                .inspect(|((_el, _at, (_val0, attach_to)), time, add_rm)| {
                    println!(
                        "{} home page to element with id: {:?}; at time {:?}",
                        add_rm_str(add_rm),
                        attach_to,
                        time
                    )
                });
        });
        input.advance_to(0);

        // index load page event fires on first load
        input.insert(("session", "page", ("home_page_root", "02312")));
        input.advance_to(1);

        // ON_HOME_SIGN_IN
        // these events are triggered on a button press
        // event inside the home page
        input.remove(("session", "page", ("home_page_root", "02312")));
        input.insert(("session", "sign_in", ("name", "user0123")));
        input.insert(("session", "sign_in", ("date", "10-08-2022")));
        input.insert(("session", "page", ("posts_page_root", "02312")));
        input.advance_to(2);


        // ON_POST_CREATED
        input.insert(("rnd-post-id", "post", ("title", "hello im a new post")));
        input.insert(("rnd-post-id", "post", ("body", "post body goes here")));
        input.advance_to(3);
    })
    .expect("fail");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        run0();
    }
}
