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
    use crate::nestedmap::test_helpers::*;
    use crate::vec_string;
    use std::time::SystemTime;

    #[test]
    fn test_get_exact() {
        //let mut nm = NestedMap::new(1);

        let test_cases = vec![
            TestCase {
                name: "Test depth 1",
                setup: Box::new(|nm| {
                    nm.set(&vec_string!["a"], b"the value a");
                }),
                search_keys: vec_string!["a"],
                expected: vec![Item {
                    key: vec_string!["a"],
                    value: b"the value a".to_vec(),
                    timestamp: SystemTime::now(),
                }],
                max_history: 1,
            },
            TestCase {
                name: "Test depth 3",
                setup: Box::new(|nm| {
                    nm.set(&vec_string!["a", "b", "c"], b"the value abc");
                }),
                search_keys: vec_string!["a", "b", "c"],
                expected: vec![Item {
                    key: vec_string!["a", "b", "c"],
                    value: b"the value abc".to_vec(),
                    timestamp: SystemTime::now(),
                }],
                max_history: 1,
            },
            TestCase {
                name: "Test depth 6",
                setup: Box::new(|nm| {
                    nm.set(
                        &vec_string!["a", "b", "c", "d", "e", "f"],
                        b"the value abcdef",
                    );
                }),
                search_keys: vec_string!["a", "b", "c", "d", "e", "f"],
                expected: vec![Item {
                    key: vec_string!["a", "b", "c", "d", "e", "f"],
                    value: b"the value abcdef".to_vec(),
                    timestamp: SystemTime::now(),
                }],
                max_history: 1,
            },
        ];

        for test in test_cases {
            let mut nm = NestedMap::new(test.max_history);
            (test.setup)(&mut nm);
            let results = nm.query(test.search_keys, test.max_history);
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
