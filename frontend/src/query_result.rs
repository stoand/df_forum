use crate::df_tuple_items::Id;
use crate::persisted::Persisted;

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryResultAggregate {
    PostCount(u64),
}

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryResult {
    AddPersisted(Id, Persisted),
    DeletePersisted(Id),
    Aggregate(QueryResultAggregate),
}
