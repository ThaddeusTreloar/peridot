
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    partition_count: i32,
}

impl TopicMetadata {
    pub fn new(partition_count: i32) -> Self {
        Self { 
            partition_count,
        }
    }

    pub fn partition_count(&self) -> i32 {
        self.partition_count
    }
}

