use super::config::VALUE_KEY;
use super::{Item, NestedMap, NestedValue};

impl NestedMap {
    pub fn query(&self, keys: Vec<String>, history_max: usize) -> Vec<Item> {
        let mut results = Vec::new();
        self.query_recursive(&keys, self, &mut results, history_max);
        results
    }

    fn query_recursive(
        &self,
        keys: &[String],
        current: &NestedMap,
        results: &mut Vec<Item>,
        history_max: usize,
    ) {
        if keys.is_empty() {
            // Collect items at the current level using VALUE_KEY
            if let Some(NestedValue::Items(items)) = current.data.get(VALUE_KEY) {
                let count = std::cmp::min(items.len(), history_max);
                results.extend_from_slice(&items[..count]);
            }
            return;
        }

        let next_key = &keys[0];
        let remaining_keys = &keys[1..];

        match next_key.as_str() {
            "*" => {
                // Iterate through all entries in the current map
                for (key, value) in &current.data {
                    if key == VALUE_KEY {
                        if let NestedValue::Items(items) = value {
                            let count = std::cmp::min(items.len(), history_max);
                            results.extend_from_slice(&items[..count]);
                        }
                    } else if let NestedValue::Map(nested_map) = value {
                        // Recurse into every nested map when "*" is encountered
                        self.query_recursive(remaining_keys, nested_map, results, history_max);
                    }
                }
            }
            ">" => {
                self.collect_all(current, results, true, history_max);
            }
            _ => {
                if let Some(NestedValue::Map(nested_map)) = current.data.get(next_key) {
                    if remaining_keys.is_empty() {
                        // Check for VALUE_KEY in the last map
                        if let Some(NestedValue::Items(items)) = nested_map.data.get(VALUE_KEY) {
                            let count = std::cmp::min(items.len(), history_max);
                            results.extend_from_slice(&items[..count]);
                        }
                    } else {
                        self.query_recursive(remaining_keys, nested_map, results, history_max);
                    }
                }
            }
        }
    }

    fn collect_all(
        &self,
        current: &NestedMap,
        results: &mut Vec<Item>,
        skip_current_level: bool,
        history_max: usize,
    ) {
        // Only collect items if not skipping the current level
        if !skip_current_level {
            if let Some(NestedValue::Items(items)) = current.data.get(VALUE_KEY) {
                let count = std::cmp::min(items.len(), history_max);
                results.extend_from_slice(&items[..count]);
            }
        }

        // Always proceed to collect from sub-maps
        for (_, value) in &current.data {
            if let NestedValue::Map(nested_map) = value {
                // Skip the current level's items but not for sub-maps
                self.collect_all(nested_map, results, false, history_max);
            }
        }
    }
}

mod tests {
    use super::*;
    use crate::nestedmap::test_helpers::items_equal;
    use crate::*;
    use std::time::{Duration, SystemTime};

    struct TestCase {
        name: &'static str,
        setup: Box<dyn Fn(&mut NestedMap)>,
        prefix_keys: Vec<String>,
        expected: Vec<Item>,
        max_history: usize,
    }

    #[test]
    fn test_queries() {
        let test_cases = vec![
            TestCase {
                name: "Test exact match",
                setup: Box::new(|nm| {
                    nm.set(&vec_string!["a", "b", "c"], b"exact value");
                }),
                prefix_keys: vec_string!["a", "b", "c"],
                expected: vec![Item {
                    key: vec_string!["a", "b", "c"],
                    value: b"exact value".to_vec(),
                    timestamp: SystemTime::now(),
                }],
                max_history: 1,
            },
            TestCase {
                name: "Test wildcard match",
                setup: Box::new(|nm| {
                    nm.set(&vec_string!["a", "b", "c"], b"wildcard value abc");
                    nm.set(&vec_string!["a", "b", "x"], b"wildcard value abx");
                    nm.set(&vec_string!["a", "b", "y"], b"wildcard value aby");
                    nm.set(&vec_string!["a", "b", "z", "z"], b"wildcard value abzz");
                }),
                prefix_keys: vec_string!["a", "b", "*"],
                expected: vec![
                    Item {
                        key: vec_string!["a", "b", "c"],
                        value: b"wildcard value abc".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: vec_string!["a", "b", "x"],
                        value: b"wildcard value abx".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: vec_string!["a", "b", "y"],
                        value: b"wildcard value aby".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                ],
                max_history: 1,
            },
            TestCase {
                name: "Test prefix match",
                setup: Box::new(|nm| {
                    nm.set(&vec_string!["a", "b", "c"], b"prefix value abc");
                    nm.set(&vec_string!["a", "b", "x"], b"prefix value abx");
                    nm.set(&vec_string!["a", "b", "y"], b"prefix value aby");
                    nm.set(&vec_string!["a", "b", "y", "z"], b"prefix value abyz");
                    nm.set(&vec_string!["a", "b", "y", "z", "z"], b"prefix value abyzz");
                }),
                prefix_keys: vec_string!["a", "b", "y", ">"],
                expected: vec![
                    Item {
                        key: vec_string!["a", "b", "y", "z"],
                        value: b"prefix value abyz".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: vec_string!["a", "b", "y", "z", "z"],
                        value: b"prefix value abyzz".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                ],
                max_history: 1,
            },
            // More test cases can be added here in the same format
        ];

        for test in test_cases {
            let mut nm = NestedMap::new(test.max_history);
            (test.setup)(&mut nm);
            let results = nm.query(test.prefix_keys, test.max_history);
            assert_eq!(
                results.len(),
                test.expected.len(),
                "Test {}: Expected {} results, got {}",
                test.name,
                test.expected.len(),
                results.len()
            );

            // Sorting by keys before comparing, since order is not guaranteed
            let mut sorted_results = results;
            let mut sorted_expected = test.expected;
            sorted_results.sort_by_key(|item| item.key.clone());
            sorted_expected.sort_by_key(|item| item.key.clone());

            assert!(
                sorted_results
                    .iter()
                    .zip(sorted_expected.iter())
                    .all(|(a, b)| items_equal(a, b)),
                "Test {}: Items do not match.",
                test.name
            );
        }
    }
}
