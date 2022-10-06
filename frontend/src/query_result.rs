use crate::df_tuple_items::Id;
use crate::persisted::{Persisted, Post};

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Query {
    PostsInPage(usize),
}

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryResult {
    AddPost(Id, Post),
    PostCount(u64),
    DeletePersisted(Id),
    PagePosts(usize, Vec<u64>),

    PersistedField(u64, Persisted),
}
