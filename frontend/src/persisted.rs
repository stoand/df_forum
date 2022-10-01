pub use crate::df_tuple_items::Id;

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Post {
    pub title: String,
    pub body: String,
    pub user_id: u64,
    pub likes: u64,
}

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Persisted {
    // Session { token: String, user_id: u64 },
    // User { name: String },
    Post(Post),
    Deleted,
}

pub type PersistedItems = Vec<(Id, Persisted)>;
