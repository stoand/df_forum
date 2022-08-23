extern crate timely;

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use timely::dataflow::operators::{Inspect, ToStream};

    #[test]
    fn it_works() {
        assert_eq!(1, 1, "all");

        timely::example(|scope| {
            (0..10)
                .to_stream(scope)
                .inspect(|x| println!("seen {:?}", x));
        });
    }
}
