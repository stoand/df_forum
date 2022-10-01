use crate::df_tuple_items::Id;
use crate::persisted::Post;

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryResult {
    AddPost(Id, Post),
    PostCount(u64),
    DeletePersisted(Id),
}
