
use timely::dataflow::*;
use differential_dataflow::{Data, Collection, ExchangeData};
use differential_dataflow::difference::{Semigroup, Abelian};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::AsCollection;

pub trait OnlyLatest<G: Scope, K: Data, V: Data, R: Semigroup> where G::Timestamp: Lattice+Ord {

    
    fn only_latest(&self) -> Collection<G, (K, V), R>;
}


impl<G, K, V, R> OnlyLatest<G, K, V, R> for Collection<G, (K, V), R>
    where
        G: Scope,
        G::Timestamp: Lattice+Ord,
        K: ExchangeData+Hashable,
        V: ExchangeData,
        R: ExchangeData+Semigroup,
 {

    fn only_latest(&self) -> Collection<G, (K, V), R> {
        self.inner.as_collection()
    }
 }
