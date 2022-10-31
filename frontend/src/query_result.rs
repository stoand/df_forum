use crate::df_tuple_items::Id;
use crate::persisted::{Persisted, Post};

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Query {
    PostsInPage(usize),
    PostCount,
    PostTitle(u64),
    Posts,
    PostAggregates,
}

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryResult {
    // AddPost(Id, Post),
    PostCount(u64),
    DeletePersisted(Id),
    PagePosts(Vec<u64>),

    PostAggregates(u64, u64),

    PostTitle(String),
    
    AddPost(u64, String, String),

    DeletePost(u64),
}
