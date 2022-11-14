pub use crate::df_tuple_items::{Id, Diff};

#[derive(Abomonation, Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Post {
    pub title: String,
    pub body: String,
    pub user_id: u64,
    pub likes: u64,
}

#[derive(Abomonation, Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Persisted {
    // Session { token: String, user_id: u64 },
    // User { name: String },
    Post,
    PostTitle(String),
    PostBody(String),
    PostUserId(u64),
    PostLikes(u64),


    // loads aggregations as well
    ViewPosts, // (session id)
    

    // reloads only posts
    ViewPostsPage(u64),
    
    Session(String), // username
}

pub type PersistedItems = Vec<(Id, Persisted, Diff)>;
