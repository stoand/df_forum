#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryResult {
    PostCount(u64),
    Post {
        id: u64,
        title: String,
        body: String,
        user_id: u64,
        likes: u64,
    },
    PostDeleted {
        id: u64,
    },
    
}
