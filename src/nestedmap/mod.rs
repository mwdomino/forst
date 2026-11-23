use foldhash::HashMap;
use std::collections::VecDeque;

use bytes::Bytes;

pub mod config;
pub mod delete;
pub mod get;
pub mod options;
pub mod query;
pub mod set;
pub mod test_helpers;

// One node per key segment: "a.b.c" stores its items at children[a].children[b].children[c].
#[derive(Debug)]
pub struct NestedMap {
    // Unordered: a hash lookup per segment is much faster than a BTreeMap on ingest
    children: HashMap<String, NestedMap>,
    // Newest timestamp first, at most max_history long
    items: VecDeque<Item>,
    max_history: usize,
    // Expiry time of this node's pending entry in the datastore's expiry heap
    scheduled_expiry: Option<i64>,
}

// Timestamps are nanoseconds since the Unix epoch. An item is identified by (key, timestamp).
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Item {
    pub key: String,
    pub value: Bytes,
    pub timestamp: i64,
    pub expires_at: Option<i64>,
}

impl NestedMap {
    pub fn new(max_history: usize) -> Self {
        NestedMap {
            children: HashMap::default(),
            items: VecDeque::new(),
            max_history,
            scheduled_expiry: None,
        }
    }

    fn node(&self, keys: &str) -> Option<&NestedMap> {
        keys.split(config::DELIMITER)
            .try_fold(self, |node, key| node.children.get(key))
    }

    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.items.is_empty()
    }
}
