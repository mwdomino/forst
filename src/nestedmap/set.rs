use super::config::*;
use super::options::SetOptions;
use super::{Item, NestedMap, NestedValue};
use std::time::SystemTime;

impl NestedMap {
    pub fn set(&mut self, keys: String, value: &[u8], options: Option<SetOptions>) {
        let options = options.unwrap_or_default();
        let mut current_map = &mut self.data;

        // Traverse to the appropriate node
        for key in keys.split(DELIMITER) {
            current_map = current_map
                .entry(key.to_string())
                .or_insert_with(|| NestedValue::Map(NestedMap::new(self.max_history)))
                .as_map_mut();
        }

        // Access or create the items list at the final key under VALUE_KEY
        let items = current_map
            .entry(VALUE_KEY.to_string())
            .or_insert_with(|| NestedValue::Items(Vec::new()));

        if let NestedValue::Items(items) = items {
            let new_item = Item {
                key: keys,
                value: value.to_vec(),
                timestamp: SystemTime::now(),
            };

            if options.preserve_history == false {
                if items.len() > 0 {
                    items[0] = new_item;
                } else {
                    items.insert(0, new_item);
                }

                return;
            }

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
    use crate::nestedmap::test_helpers::*;
    use crate::vec_string;
    use std::time::SystemTime;

    #[test]
    fn test_set() {
        //let mut nm = NestedMap::new(1);

        let test_cases = vec![
            TestCase {
                name: "Test depth 1",
                setup: Box::new(|nm| {
                    nm.set("a".to_string(), b"the value a", None);
                }),
                search_keys: "a".to_string(),
                expected: vec![Item {
                    key: "a".to_string(),
                    value: b"the value a".to_vec(),
                    timestamp: SystemTime::now(),
                }],
                max_history: 1,
            },
            TestCase {
                name: "Test depth 3",
                setup: Box::new(|nm| {
                    nm.set("a.b.c".to_string(), b"the value abc", None);
                }),
                search_keys: "a.b.c".to_string(),
                expected: vec![Item {
                    key: "a.b.c".to_string(),
                    value: b"the value abc".to_vec(),
                    timestamp: SystemTime::now(),
                }],
                max_history: 1,
            },
            TestCase {
                name: "Test depth 6",
                setup: Box::new(|nm| {
                    nm.set("a.b.c.d.e.f".to_string(), b"the value abcdef", None);
                }),
                search_keys: "a.b.c.d.e.f".to_string(),
                expected: vec![Item {
                    key: "a.b.c.d.e.f".to_string(),
                    value: b"the value abcdef".to_vec(),
                    timestamp: SystemTime::now(),
                }],
                max_history: 1,
            },
        ];

        set_tests(test_cases)
    }

    fn test_set_without_history() {}
    fn test_set_history() {}
    fn test_set_mixed_history() {}

    fn set_tests(test_cases: Vec<TestCase>) {
        for test in test_cases {
            let mut nm = NestedMap::new(test.max_history);
            (test.setup)(&mut nm);

            if let Some(item) = nm.get(test.search_keys) {
                assert_eq!(items_equal(item, &test.expected[0]), true);
            }
        }
    }
}
