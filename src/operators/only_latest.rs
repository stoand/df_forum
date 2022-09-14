use differential_dataflow::difference::{Abelian, Monoid};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Reduce;
use differential_dataflow::AsCollection;
use differential_dataflow::{Collection, ExchangeData};
use timely::dataflow::operators::Map;
use timely::dataflow::*;

pub trait OnlyLatest<G, K, V, R>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: Abelian + ExchangeData,
{
    fn only_latest(&self) -> Collection<G, (K, V), R>;
}

impl<G, K, V, R> OnlyLatest<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: Abelian + ExchangeData,
{
    fn only_latest(&self) -> Collection<G, (K, V), R> {
        self.inner
            .map(|((id, persisted), time, diff)| {
                // let reduce sort by time
                ((id, (time.clone(), persisted)), time.clone(), diff)
            })
            .as_collection()
            .reduce(|_key, inputs, outputs| {
                let abelian = inputs[0].clone().1;
                let positive_abelian: R = if abelian > Monoid::zero() {
                    abelian
                } else {
                    abelian.neg()
                };
                let negative_abelian = positive_abelian.clone().neg();

                for i in 0..inputs.len() {
                    let value_ref: &(_, V) = inputs[i].clone().0;
                    let value: (_, V) = value_ref.clone();
                    if i == inputs.len() - 1 {
                        // TODO: replace zeros with 1 and -1
                        outputs.push((value, positive_abelian.clone()));
                    } else {
                        outputs.push((value, negative_abelian.clone()));
                    }
                }
            })
            .map(|(id, (_time, persisted))| (id, persisted))
    }
}
