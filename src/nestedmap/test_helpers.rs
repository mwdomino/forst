use super::*;

pub fn items_equal(a: &Item, b: &Item) -> bool {
    a.key == b.key && a.value == b.value
}

pub struct TestCase {
    pub name: &'static str,
    pub setup: Box<dyn Fn(&mut NestedMap)>,
    pub search_keys: String,
    pub expected: Vec<Item>,
    pub max_history: usize,
}

// create_item returns an item with a static ID suitable for testing only
pub fn create_item(key: &str, value: &[u8]) -> Item {
    return Item {
        key: key.to_string(),
        value: value.to_vec(),
        timestamp: SystemTime::now(),
        id: 1,
    };
}

#[macro_export]
macro_rules! vec_string {
    ( $( $x:expr ),* ) => {
        vec![
            $( $x.to_string(), )*
        ]
    };
}
