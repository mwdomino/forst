use super::config::VALUE_KEY;
use super::{Item, NestedMap, NestedValue};

impl NestedMap {
    pub fn get(&self, keys: &[String]) -> Option<&Item> {
        let mut current_map = &self.data;

        for key in keys.iter() {
            if let Some(NestedValue::Map(map)) = current_map.get(key) {
                current_map = &map.data;
            } else {
                return None; // Early exit if no map is found
            }
        }

        // Try to retrieve items at the VALUE_KEY in the final map
        if let Some(NestedValue::Items(items)) = current_map.get(VALUE_KEY) {
            items.first() // Return the first item if available
        } else {
            None
        }
    }
}

mod tests {
    use super::*;
    use crate::nestedmap::test_helpers::items_equal;
    use std::time::SystemTime;

    #[test]
    fn test_get_exact() {
        let mut nm = NestedMap::new(1);
        let expected: &Item = &Item {
            key: vec!["a".to_string()],
            value: b"the value a".to_vec(),
            timestamp: SystemTime::now(),
        };

        nm.set(&expected.key.clone(), &expected.value.clone());

        let result = nm.get(&expected.key.clone());
        assert!(result.is_some());
        let item = result.unwrap();
        assert_eq!(items_equal(item, expected), true);
    }
}
