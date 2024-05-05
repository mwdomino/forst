use std::collections::HashMap;
use std::time::SystemTime;

pub mod get;
pub mod set;
pub mod test_helpers;

#[derive(PartialEq, Debug)]
pub struct NestedMap {
    data: HashMap<String, NestedValue>,
    max_history: usize,
}

#[derive(PartialEq, Debug)]
pub enum NestedValue {
    Map(NestedMap),
    Items(Vec<Item>),
}

#[derive(PartialEq, Debug)]
pub struct Item {
    key: Vec<String>,
    value: Vec<u8>,
    timestamp: SystemTime,
}

// New
impl NestedMap {
    pub fn new(max_history: usize) -> Self {
        NestedMap {
            data: HashMap::new(),
            max_history,
        }
    }
}

// Helper function to get mutable reference to nested map if the variant is Map
impl NestedValue {
    pub fn as_map_mut(&mut self) -> &mut HashMap<String, NestedValue> {
        match self {
            NestedValue::Map(map) => &mut map.data,
            _ => panic!("Expected NestedValue to be Map"),
        }
    }
}
