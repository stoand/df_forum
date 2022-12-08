pub mod page_post_ids;
pub mod post_aggr;
pub mod post_liked_by_user;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Reduce;
use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::operators::Filter;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::Map;
use timely::dataflow::Scope;

use crate::forum_minimal::{InputFormat, Persisted, POSTS_PER_PAGE};
use log::debug;

pub fn post_pages<G: Scope>(
    collection: &Collection<G, InputFormat>,
) -> Collection<G, (u64, u64, u64)>
where
    G::Timestamp: Lattice + Ord + Copy,
{
    collection
        .flat_map(|(_addr, (post_id, persisted))| {
            if let Persisted::Post = persisted {
                vec![post_id]
            } else {
                vec![]
            }
        })
        .inner
        .map(|(post_id, time, diff)| (((), (time, post_id)), time, diff))
        .as_collection()
        // reduce will automatically order by time
        .reduce(|_, inputs, outputs| {
            debug!("only inputs = {:?}", inputs);
            for (index, ((_time, post_id), diff)) in inputs.into_iter().rev().enumerate() {
                if *diff > 0 {
                    let page = (index / POSTS_PER_PAGE) as u64;
                    let position = (index % POSTS_PER_PAGE) as u64;
                    outputs.push(((*post_id, page, position), 1));
                }
            }
        })
        .map(|((), val)| val)
        .inspect(|v| debug!("post pages -- {:?}", v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use timely::dataflow::operators::ToStream;

    #[tokio::test]
    pub async fn test_post_pages() {
        crate::init_logger();
        let addr0: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        timely::example(move |scope| {
            let stream = vec![
                ((addr0, (5, Persisted::Post)), 0, 1),
                // (6, Persisted::Post),
                // (7, Persisted::Post),
            ]
            .to_stream(scope)
            .as_collection();

            post_pages(&stream).inspect(|v| debug!("got val {:?}", v));
        });
    }
}
