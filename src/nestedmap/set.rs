use std::time::SystemTime;

use super::{Item, NestedMap, NestedValue};

impl NestedMap {
    pub fn set(&mut self, keys: &[String], value: &[u8]) {
        let mut current_map = &mut self.data;

        // Find the deepest existing nested map
        // or create a new one
        for key in keys.iter().take(keys.len() - 1) {
            current_map = current_map
                .entry(key.to_string())
                .or_insert_with(|| NestedValue::Map(NestedMap::new(self.max_history)))
                .as_map_mut();
        }

        // Insert the final value into the deepest nested map
        let nested_value = current_map
            .entry(keys.last().unwrap().to_string())
            .or_insert_with(|| NestedValue::Items(Vec::new()));

        if let NestedValue::Items(items) = nested_value {
            let new_item = Item {
                key: keys.to_vec(),
                value: value.to_vec(),
                timestamp: SystemTime::now(),
            };

            if items.len() >= self.max_history {
                items.pop();
            }
            items.insert(0, new_item);
        }
    }
}

mod tests {
    use super::*;
    use crate::nestedmap::test_helpers::items_equal;

    #[test]
    fn test_set() {
        let mut nm = NestedMap::new(1);
        let expected = &Item {
            key: vec!["a".to_string()],      // Convert to Vec<String>
            value: b"some value a".to_vec(), // Convert to Vec<u8>
            timestamp: SystemTime::now(),    // Use current timestamp
        };

        nm.set(&["a".to_string()], b"some value a");
        if let NestedValue::Items(items) = nm.data.get("a").unwrap() {
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
        let key: Vec<String> = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let value: Vec<u8> = b"some value abc".to_vec();
        let expected = &Item {
            key: key.clone(),
            value: value.clone(),
            timestamp: SystemTime::now(),
        };

        nm.set(&key, &value);
        if let Some(NestedValue::Map(map1)) = nm.data.get("a") {
            if let Some(NestedValue::Map(map2)) = map1.data.get("b") {
                if let Some(NestedValue::Items(items)) = map2.data.get("c") {
                    assert_eq!(items.len(), 1); // Ensure there is only one item
                    assert_eq!(items_equal(&items[0], expected), true);
                } else {
                    panic!("Expected NestedValue::Items for key 'c', got something else");
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
            key: vec!["a".to_string()],
            value: b"some value a1".to_vec(),
            timestamp: SystemTime::now(),
        };

        let expected_second = &Item {
            key: vec!["a".to_string()],
            value: b"some value a2".to_vec(),
            timestamp: SystemTime::now(),
        };

        nm.set(&expected_first.key, &expected_first.value);
        nm.set(&expected_second.key, &expected_second.value);

        if let NestedValue::Items(items) = nm.data.get("a").unwrap() {
            assert_eq!(items.len(), 2);

            assert_eq!(items_equal(&items[0], expected_second), true);
            assert_eq!(items_equal(&items[1], expected_first), true);
        } else {
            panic!("Expected NestedValue::Items, got something else");
        }
    }
}
