#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Persisted {
    Session {
        token: String,
        user_id: u64,
    },
    User {
        name: String,
    },
    Post {
        title: String,
        body: String,
        user_id: u64,
        likes: u64,
    },
    PostDeleted {
        id: u64,
    },
    Deleted,
}

