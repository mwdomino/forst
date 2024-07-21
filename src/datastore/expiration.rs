use std::time::SystemTime;

use super::event::Event;
use crate::datastore::Datastore;
use tokio::sync::mpsc::Sender;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ExpirationEntry {
    pub id: i64,
    pub key: String,
    pub expires_at: SystemTime,
}

impl Ord for ExpirationEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap
        other.expires_at.cmp(&self.expires_at)
    }
}

impl PartialOrd for ExpirationEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
