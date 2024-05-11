use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::time::SystemTime;

pub mod config;
//pub mod delete;
pub mod get;
pub mod options;
pub mod query;
pub mod set;
pub mod test_helpers;

#[derive(PartialEq, Debug)]
pub struct NestedMap {
    data: BTreeMap<String, NestedValue>,
    max_history: usize,
}

#[derive(PartialEq, Debug)]
pub enum NestedValue {
    Map(NestedMap),
    Items(VecDeque<Item>),
}

#[derive(PartialEq, Debug, Clone)]
pub struct Item {
    key: String,
    value: Vec<u8>,
    timestamp: SystemTime,
}

// New
impl NestedMap {
    pub fn new(max_history: usize) -> Self {
        NestedMap {
            data: BTreeMap::new(),
            max_history,
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
