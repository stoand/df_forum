pub mod page_post_ids;
pub mod post_aggr;
pub mod post_liked_by_user;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use std::net::SocketAddr;
use timely::dataflow::operators::Filter;
// use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::Map;
use timely::dataflow::Scope;

use crate::forum_minimal::{Collection, InputFormat, Persisted, POSTS_PER_PAGE};
use log::debug;

pub fn shared_post_pages<'a>(
    collection: &Collection<'a, InputFormat>,
) -> Collection<'a, (SocketAddr, u64, u64, u64)> {
    let result = collection
        .flat_map(|(addr, (post_id, persisted))| {
            if let Persisted::Post = persisted {
                vec![(post_id, addr)]
            } else {
                vec![]
            }
        })
        .inner
        .map(|((post_id, addr), time, diff)| (((), (time, addr, post_id)), time, diff))
        .as_collection()
        // reduce will automatically order by time
        .reduce(|_, inputs, outputs| {
            debug!("input = {:?}, output = {:?}", inputs, outputs);

            let (mut added, mut removed): (Vec<_>, Vec<_>) =
                inputs.to_vec().into_iter().partition(|(_, diff)| *diff > 0);

            added.sort_by_key(|((time, _addr, _id), _diff)| -(*time as isize));
            removed.sort_by_key(|((time, _addr, _id), _diff)| -(*time as isize));

            let mut page_item_index: u64 = 0;
            let mut page = 0;

            for ((creation_time, addr, id), _diff) in added {
                outputs.push(((*addr, *id, page, *creation_time), 1));

                let dup_removed = removed
                    .iter()
                    .find(|((_creation_time, _addr, other_id), _diff)| id == other_id);
                if dup_removed != None {
                    outputs.push(((*addr, *id, page, *creation_time), -1));
                    continue;
                } else {
                    outputs.push(((*addr, *id, page, *creation_time), 1));
                }
                page_item_index += 1;
                if page_item_index >= POSTS_PER_PAGE as u64 {
                    page_item_index = 0;
                    page += 1;
                }
            }
        })
        .map(|((), val)| val)
        // .inner
        // .filter(|(_, _time, diff)| *diff > 0)
        // .as_collection()
        .inspect(|v| debug!("post pages -- {:?}", v));

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::dataflow::operators::ToStream;

    #[tokio::test]
    pub async fn test_shared_post_pages() {
        crate::init_logger();
        let addr0: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        timely::example(move |scope| {
            let stream = vec![
                ((addr0, (5, Persisted::Post)), 0, 1),
                ((addr0, (6, Persisted::Post)), 0, 1),
                ((addr0, (7, Persisted::Post)), 0, 1),
                ((addr0, (5, Persisted::Post)), 1, -1),
            ]
            .to_stream(scope)
            .as_collection();

            shared_post_pages(&stream).inspect(|v| debug!("got val {:?}", v));
        });
    }
}
