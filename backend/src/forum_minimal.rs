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
        user_id: u64,
        likes: u64,
    },
    Deleted,
}

#[derive(Hash, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForumMinimal {
    pub state: Vec<Persisted>,
    pub connection_count: u64,
}

impl ForumMinimal {
    
    pub fn new() -> Self {
        ForumMinimal { state: Vec::new(), connection_count: 0 }
    }
    
    pub fn say_hi(&mut self) {
        self.connection_count += 1;        
        println!("forum_minimal says hi, count = {}", self.connection_count);
    }

    // #SPC-forum_minimal.create_post
    // pub fn create_post(data: String) {
    // }
}

// // #SPC-forum_minimal.aggregates_global_post_count
// pub fn aggregates_global_post_count() {
// }

// #[test]
// pub fn test_aggregates_global_post_count() {
// }
