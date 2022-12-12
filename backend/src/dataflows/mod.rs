pub mod page_post_ids;
pub mod post_aggr;
pub mod post_liked_by_user;
pub mod post_total_likes;

use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use std::net::SocketAddr;
use timely::dataflow::operators::Map;

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
        // reduce will automatically order by time (left-most value in the tuple)
        .reduce(|(), inputs, outputs| {
            debug!("inputs: {:?}", inputs);

            let mut visible = Vec::new();

            for ((time, addr, add_post_id), diff) in inputs {
                if *diff > 0 {
                    let mut found_removal = false;

                    for ((_time, _, rem_post_id), diff) in inputs {
                        if *diff < 0 && add_post_id == rem_post_id {
                            found_removal = true;
                        }
                    }

                    if !found_removal {
                        visible.push((addr, add_post_id, time));
                    }
                }
            }

            for (index, (addr, post_id, time)) in visible.into_iter().rev().enumerate() {
                let page = (index / POSTS_PER_PAGE) as u64;
                outputs.push(((*addr, *post_id, page, *time), 1));
            }
        })
        .map(|((), val)| val)
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

            shared_post_pages(&stream).inspect_batch(move |_time, v| {
                assert_eq!(
                    v,
                    vec![
                        ((addr0, 5, 1, 0), 0, 1),
                        ((addr0, 5, 1, 0), 1, -1),
                        ((addr0, 6, 0, 0), 0, 1),
                        ((addr0, 7, 0, 0), 0, 1)
                    ]
                );
                debug!("got val {:?}", v);
            });
        });
    }
}
