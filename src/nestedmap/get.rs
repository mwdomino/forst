use super::config::*;
use super::*;

impl NestedMap {
    pub fn get(&self, keys: &str) -> Option<&Item> {
        let mut current_map = &self.data;

        for key in keys.split(DELIMITER) {
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
                    nm.set(&"a".to_string(), b"the value a", None);
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
                    nm.set(&"a.b.c".to_string(), b"the value abc", None);
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
                    nm.set(&"a.b.c.d.e.f".to_string(), b"the value abcdef", None);
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

        get_tests(test_cases)
    }

    fn get_tests(test_cases: Vec<TestCase>) {
        for test in test_cases {
            let mut nm = NestedMap::new(test.max_history);
            (test.setup)(&mut nm);

            if let Some(item) = nm.get(&test.search_keys) {
                assert_eq!(items_equal(item, &test.expected[0]), true);
            }
        }
    }
}
