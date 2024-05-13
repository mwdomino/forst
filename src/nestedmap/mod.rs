use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod config;
pub mod delete;
pub mod get;
pub mod options;
pub mod query;
pub mod set;
pub mod test_helpers;

#[derive(PartialEq, Eq, Debug)]
struct ExpirationEntry {
    expires_at: SystemTime,
    id: i64,
    keys: String,
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

#[derive(Debug)]
pub struct NestedMap {
    data: BTreeMap<String, NestedValue>,
    max_history: usize,
    exp_heap: Mutex<BinaryHeap<ExpirationEntry>>,
    id_counter: Arc<AtomicI64>,
}

#[derive(Debug)]
pub enum NestedValue {
    Map(NestedMap),
    Items(VecDeque<Item>),
}

#[derive(PartialEq, Debug, Clone)]
pub struct Item {
    key: String,
    pub value: Vec<u8>,
    timestamp: SystemTime,
    id: i64,
}

// New
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

impl NestedMap {
    pub fn check_expirations(&mut self) {
        let now = SystemTime::now();
        let mut heap = self.exp_heap.lock().unwrap();

        // Collect keys to delete
        let mut keys_to_delete = Vec::new();
        while let Some(entry) = heap.peek() {
            if entry.expires_at > now {
                break;
            }
            let entry = heap.pop().unwrap();
            keys_to_delete.push(entry.keys.clone());
        }
        drop(heap); // Explicitly drop heap to release the lock

        // Process deletions
        for keys in keys_to_delete {
            self.delete_at_index(&keys, 1); // TODO - use the real index
        }
    }
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
