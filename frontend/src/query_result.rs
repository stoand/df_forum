use crate::df_tuple_items::Id;

#[derive(Abomonation, Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryResult {
    // AddPost(Id, Post),
    PostCount(u64),
    DeletePersisted(Id),

    PostAggregates(u64, u64), // post count, page count
    AddPost(u64, String, String),

    DeletePost(u64),

    PagePost(u64, u64, u64), // id, page, page_item_index

    PostTitle(u64, String),
    PostBody(u64, String),
    PostCreator(u64, String),
}
