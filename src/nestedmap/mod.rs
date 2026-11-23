use std::collections::{BTreeMap, VecDeque};
use std::time::SystemTime;

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
    children: BTreeMap<String, NestedMap>,
    // Newest first, at most max_history long
    items: VecDeque<Item>,
    max_history: usize,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Item {
    pub key: String,
    pub value: Vec<u8>,
    pub timestamp: SystemTime,
    pub id: i64,
}

impl NestedMap {
    pub fn new(max_history: usize) -> Self {
        NestedMap {
            children: BTreeMap::new(),
            items: VecDeque::new(),
            max_history,
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
