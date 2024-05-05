use super::{Item, NestedMap, NestedValue};

impl NestedMap {
    pub fn get(&self, keys: &[String]) -> Option<&Item> {
        let mut current_map = &self.data;

        for (i, key) in keys.iter().enumerate() {
            if let Some(nested_value) = current_map.get(key) {
                match nested_value {
                    NestedValue::Map(next_map) => {
                        if i == keys.len() - 1 {
                            return None;
                        } else {
                            current_map = &next_map.data;
                        }
                    }
                    NestedValue::Items(items) => {
                        if let Some(item) = items.first() {
                            return Some(item);
                        } else {
                            return None;
                        }
                    }
                }
            } else {
                return None;
            }
        }
        None
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
