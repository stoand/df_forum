extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Join;

#[derive(Clone,Debug,PartialEq, Eq, PartialOrd, Ord)]
enum EventType {
    Stuff0,
    Stuff1,
}

fn main() {
    run0();
}

fn run0() {
    
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();

        worker.dataflow(|scope| {
            let manages = input.to_collection(scope);

            manages
                // .map(|(m2, m1)| (m1, m2))
                // .join(&manages)
                .inspect(|x| println!("{:?}", x));
        });

        let size = std::env::args().nth(1).unwrap().parse().unwrap();
        input.advance_to(0);
        for person in 0..size {
            
            input.insert((person / 2, EventType::Stuff0));
        }

        input.advance_to(1);
        input.remove((2, EventType::Stuff0));
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
