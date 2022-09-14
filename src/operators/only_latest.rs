use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use differential_dataflow::{Collection, Data, ExchangeData};
use timely::dataflow::operators::Map;
use timely::dataflow::*;

// TODO: replace the hardcoded isize with a generic Semigroup

pub trait OnlyLatest<G: Scope, K: Data, V: Data>
where
    G::Timestamp: Lattice + Ord,
{
    fn only_latest(&self) -> Collection<G, (K, V), isize>;
}

impl<G, K, V> OnlyLatest<G, K, V> for Collection<G, (K, V), isize>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
    K: ExchangeData + Hashable,
    V: ExchangeData,
{
    fn only_latest(&self) -> Collection<G, (K, V), isize> {
        self.inner
            .map(|((id, persisted), time, diff)| {
                // let reduce sort by time
                ((id, (time.clone(), persisted)), time.clone(), diff)
            })
            .as_collection()
            .reduce(|_key, inputs, outputs| {
                // log(&format!(
                //     "key = {:?}, input = {:?}, output = {:?}",
                //     _key, inputs, outputs
                // ));

                for i in 0..inputs.len() {
                    
                    let value_ref: &(_, V) = inputs[i].clone().0;
                    let value: (_, V) = value_ref.clone();
                    
                    if i == inputs.len() - 1 {
                        outputs.push((value, 1));
                    } else {
                        outputs.push((value, -1));
                    }
                }
            })
            .map(|(id, (_time, persisted))| (id, persisted))
    }
}
