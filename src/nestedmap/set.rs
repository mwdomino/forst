use super::config::VALUE_KEY;
use super::{Item, NestedMap, NestedValue};
use std::time::SystemTime;

impl NestedMap {
    pub fn set(&mut self, keys: &[String], value: &[u8]) {
        let mut current_map = &mut self.data;

        // Traverse to the appropriate node
        for key in keys.iter() {
            current_map = current_map
                .entry(key.clone())
                .or_insert_with(|| NestedValue::Map(NestedMap::new(self.max_history)))
                .as_map_mut();
        }

        // Access or create the items list at the final key under VALUE_KEY
        let items = current_map
            .entry(VALUE_KEY.to_string())
            .or_insert_with(|| NestedValue::Items(Vec::new()));

        if let NestedValue::Items(items) = items {
            let new_item = Item {
                key: keys.to_vec(),
                value: value.to_vec(),
                timestamp: SystemTime::now(),
            };

            // Prepend new item to the list to keep the newest items at the start
            if items.len() >= self.max_history {
                items.pop(); // Remove the oldest item if we exceed the max history
            }
            items.insert(0, new_item); // Insert new item at the start of the list
        }
    }
}

mod tests {
    use super::*;
    use crate::nestedmap::test_helpers::items_equal;
    use crate::vec_string;

    #[test]
    fn test_set() {
        let mut nm = NestedMap::new(1);
        let expected = &Item {
            key: vec_string!["a"],
            value: b"some value a".to_vec(),
            timestamp: SystemTime::now(),
        };

        nm.set(&vec_string!["a"], b"some value a");
        if let Some(NestedValue::Items(items)) = nm.data.get("a").and_then(|v| match v {
            NestedValue::Map(map) => map.data.get(VALUE_KEY),
            _ => None,
        }) {
            assert_eq!(items.len(), 1); // Ensure there is only one item

            let actual_item = &items[0];
            assert_eq!(items_equal(actual_item, expected), true)
        } else {
            panic!("Expected NestedValue::Items, got something else");
        }
    }

    #[test]
    fn test_set_deep() {
        let mut nm = NestedMap::new(1);
        let key: Vec<String> = vec_string!["a", "b", "c"];
        let value: Vec<u8> = b"some value abc".to_vec();
        let expected = &Item {
            key: key.clone(),
            value: value.clone(),
            timestamp: SystemTime::now(),
        };

        nm.set(&key, &value);
        if let Some(NestedValue::Map(map1)) = nm.data.get("a") {
            if let Some(NestedValue::Map(map2)) = map1.data.get("b") {
                if let Some(NestedValue::Map(map3)) = map2.data.get("c") {
                    if let Some(NestedValue::Items(items)) = map3.data.get(VALUE_KEY) {
                        assert_eq!(items.len(), 1); // Ensure there is only one item
                        assert_eq!(items_equal(&items[0], expected), true);
                    } else {
                        panic!("Expected NestedValue::Items for key 'c', got something else");
                    }
                } else {
                    panic!("Expected NestedValue::Map for key 'c', got something else");
                }
            } else {
                panic!("Expected NestedValue::Map for key 'b', got something else");
            }
        } else {
            panic!("Expected NestedValue::Map for key 'a', got something else");
        }
    }

    #[test]
    fn test_set_with_history() {
        let mut nm = NestedMap::new(5);

        let expected_first = &Item {
            key: vec_string!["a"],
            value: b"some value a1".to_vec(),
            timestamp: SystemTime::now(),
        };

        let expected_second = &Item {
            key: vec_string!["a"],
            value: b"some value a2".to_vec(),
            timestamp: SystemTime::now(),
        };

        nm.set(&expected_first.key, &expected_first.value);
        nm.set(&expected_second.key, &expected_second.value);

        if let Some(NestedValue::Map(map)) = nm.data.get("a") {
            if let Some(NestedValue::Items(items)) = map.data.get(VALUE_KEY) {
                assert_eq!(items.len(), 2);

                assert_eq!(items_equal(&items[0], expected_second), true);
                assert_eq!(items_equal(&items[1], expected_first), true);
            } else {
                panic!("Expected NestedValue::Items, got something else");
            }
        } else {
            panic!("Expected NestedValue::Map for key 'a', got something else");
        }
    }
}
