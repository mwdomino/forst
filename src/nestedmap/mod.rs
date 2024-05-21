use std::collections::{BTreeMap, VecDeque};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

pub mod config;
pub mod delete;
pub mod get;
pub mod options;
pub mod query;
pub mod set;
pub mod test_helpers;

#[derive(Debug)]
pub struct NestedMap {
    data: BTreeMap<String, NestedValue>,
    max_history: usize,
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub key: String,
    pub value: Vec<u8>,
    pub timestamp: SystemTime,
    pub id: i64,
}

#[derive(Debug)]
pub enum NestedValue {
    Map(NestedMap),
    Items(VecDeque<Item>),
}

// Helper function to get mutable reference to nested map if the variant is Map
impl NestedValue {
    pub fn as_map_mut(&mut self) -> &mut BTreeMap<String, NestedValue> {
        match self {
            NestedValue::Map(map) => &mut map.data,
            _ => panic!("Expected NestedValue to be Map"),
        }
    }
}

impl NestedMap {
    pub fn new(max_history: usize) -> Self {
        NestedMap {
            data: BTreeMap::new(),
            max_history,
        }
    }

    pub fn eviction_callback(&mut self, keys: &str, id: i64) {
        let _ = self.delete_by_id(keys, id);
    }
}
