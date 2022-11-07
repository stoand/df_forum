use crate::df_tuple_items::Id;

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryResult {
    // AddPost(Id, Post),
    PostCount(u64),
    DeletePersisted(Id),
    PagePosts(Vec<u64>),

    PostAggregates(u64, u64), // post count, page count
    
    AddPost(u64, String, String),

    DeletePost(u64),


    UpdatePostList(Vec<u64>),

    PostTitle(String),
    PostBody(String),
}
