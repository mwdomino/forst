use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::sync::{atomic::AtomicI64, Arc, Mutex};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use expiration_manager::ExpirationEntry;

pub mod config;
pub mod delete;
pub mod expiration_manager;
pub mod get;
pub mod options;
pub mod query;
pub mod set;
pub mod test_helpers;

#[derive(Debug)]
pub struct NestedMap {
    data: BTreeMap<String, NestedValue>,
    max_history: usize,
    exp_heap: Mutex<BinaryHeap<ExpirationEntry>>,
    id_counter: Arc<AtomicI64>,
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
            exp_heap: Mutex::new(BinaryHeap::new()),
            id_counter: Arc::new(AtomicI64::new(0)),
        }
    }
}
