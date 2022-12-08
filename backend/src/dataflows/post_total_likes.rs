use crate::forum_minimal::{OutputScopeCollection, Persisted, QueryResult, ScopeCollection};
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use timely::dataflow::operators::Map;

pub fn post_total_likes_dataflow<'a>(
    collection: &ScopeCollection<'a>,
) -> OutputScopeCollection<'a> {
    let posts_liked_by_user = collection.flat_map(|(_addr, (_user_id, persisted))| {
        if let Persisted::PostLike(liked_post) = persisted {
            vec![(liked_post, ())]
        // add an additional count so that counting to zero is possible
        } else if Persisted::Post == persisted {
            vec![(0, ())]
        } else {
            vec![]
        }
    });

    let active_addrs = collection.flat_map(|(addr, (_user_id, persisted))| {
        if let Persisted::ViewPostsPage(_page) = persisted {
            vec![(0, addr)]
        } else {
            vec![]
        }
    });

    posts_liked_by_user
        .reduce(|_post_id, inputs, outputs| {
            let count = inputs
                .into_iter()
                .filter(|(_, diff)| *diff > 0)
                .collect::<Vec<_>>()
                .len();
            // remove additional count that makes counting to zero possible
            outputs.push((count - 1, 1));
        })
        .filter(|(post_id, _count)| *post_id != 0)
        .map(|(post_id, count)| (0, (post_id, count)))
        .join(&active_addrs)
        .inner
        .map(|((_, ((post_id, count), addr)), time, diff)| {
            let result = if diff > 0 {
                vec![(addr, QueryResult::PostTotalLikes(post_id, count as u64))]
            } else {
                vec![]
            };

            (result, time, diff)
        })
        .as_collection()
}
