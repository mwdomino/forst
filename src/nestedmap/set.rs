use super::config::*;
use super::options::SetOptions;
use super::{Item, NestedMap, NestedValue};
use std::collections::VecDeque;
use std::time::SystemTime;

impl NestedMap {
    pub fn set(&mut self, keys: &str, value: &[u8], options: Option<SetOptions>) {
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
            .or_insert_with(|| NestedValue::Items(VecDeque::new()));

        if let NestedValue::Items(items) = items {
            let new_item = Item {
                key: keys.to_string(),
                value: value.to_vec(),
                timestamp: SystemTime::now(),
            };

            let length: usize = items.len();

            if options.preserve_history == false {
                if length > 0 {
                    items[0] = new_item;
                } else {
                    items.insert(0, new_item);
                }

                return;
            }

            // Prepend new item to the list to keep the newest items at the start
            if length >= self.max_history {
                items.pop_back(); // Remove the oldest item if we exceed the max history
            }
            items.push_front(new_item); // Insert new item at the start of the list
        }
    }
}

mod tests {
    use super::*;
    use crate::nestedmap::options::*;
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

        set_tests(test_cases)
    }

    #[test]
    fn test_set_without_history() {
        let test_cases = vec![TestCase {
            name: "Test without history option",
            setup: Box::new(|nm| {
                for i in 1..=7 {
                    nm.set(
                        &"a.b.c.d".to_string(),
                        &format!("value{}", i).into_bytes(),
                        Some(SetOptions::new().preserve_history(false)),
                    );
                }
            }),
            search_keys: "a.b.c.d".to_string(),
            expected: vec![Item {
                key: "a.b.c.d".to_string(),
                value: b"value7".to_vec(),
                timestamp: SystemTime::now(),
            }],
            max_history: 5,
        }];

        set_tests(test_cases)
    }

    #[test]
    fn test_set_history() {
        let test_cases = vec![
            TestCase {
                name: "Test more than max_history values",
                setup: Box::new(|nm| {
                    for i in 1..=7 {
                        nm.set(
                            &"a.b.c.d".to_string(),
                            &format!("value{}", i).into_bytes(),
                            Some(SetOptions::new().preserve_history(true)),
                        );
                    }
                }),
                search_keys: "a.b.c.d".to_string(),
                expected: vec![
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value7".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value6".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value5".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value4".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value3".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                ],
                max_history: 5,
            },
            TestCase {
                name: "Test less than max_history values",
                setup: Box::new(|nm| {
                    for i in 1..=3 {
                        nm.set(
                            &"a.b.c.d".to_string(),
                            &format!("value{}", i).into_bytes(),
                            Some(SetOptions::new().preserve_history(true)),
                        );
                    }
                }),
                search_keys: "a.b.c.d".to_string(),
                expected: vec![
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value3".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value2".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value1".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                ],
                max_history: 5,
            },
            TestCase {
                name: "Test exactly max_history values",
                setup: Box::new(|nm| {
                    for i in 1..=5 {
                        nm.set(
                            &"a.b.c.d".to_string(),
                            &format!("value{}", i).into_bytes(),
                            Some(SetOptions::new().preserve_history(true)),
                        );
                    }
                }),
                search_keys: "a.b.c.d".to_string(),
                expected: vec![
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value5".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value4".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value3".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value2".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                    Item {
                        key: "a.b.c.d".to_string(),
                        value: b"value1".to_vec(),
                        timestamp: SystemTime::now(),
                    },
                ],
                max_history: 5,
            },
        ];

        set_tests(test_cases)
    }

    #[test]
    fn test_set_mixed_history() {
        let test_cases = vec![TestCase {
            name: "Test more than max_history values",
            setup: Box::new(|nm| {
                nm.set(
                    &"a.b.c.d".to_string(),
                    b"value1",
                    Some(SetOptions::new().preserve_history(true)),
                );
                nm.set(
                    &"a.b.c.d".to_string(),
                    b"value2",
                    Some(SetOptions::new().preserve_history(true)),
                );
                nm.set(
                    &"a.b.c.d".to_string(),
                    b"value3",
                    Some(SetOptions::new().preserve_history(true)),
                );
                nm.set(
                    &"a.b.c.d".to_string(),
                    b"value4",
                    Some(SetOptions::new().preserve_history(false)),
                );
                nm.set(
                    &"a.b.c.d".to_string(),
                    b"value5",
                    Some(SetOptions::new().preserve_history(true)),
                );
            }),
            search_keys: "a.b.c.d".to_string(),
            expected: vec![
                Item {
                    key: "a.b.c.d".to_string(),
                    value: b"value5".to_vec(),
                    timestamp: SystemTime::now(),
                },
                Item {
                    key: "a.b.c.d".to_string(),
                    value: b"value4".to_vec(),
                    timestamp: SystemTime::now(),
                },
                Item {
                    key: "a.b.c.d".to_string(),
                    value: b"value2".to_vec(),
                    timestamp: SystemTime::now(),
                },
                Item {
                    key: "a.b.c.d".to_string(),
                    value: b"value1".to_vec(),
                    timestamp: SystemTime::now(),
                },
            ],
            max_history: 5,
        }];

        set_tests(test_cases)
    }

    fn set_tests(test_cases: Vec<TestCase>) {
        for test in test_cases {
            let mut nm = NestedMap::new(test.max_history);
            (test.setup)(&mut nm);

            let results = nm.query(
                &test.search_keys,
                Some(GetOptions::new().history_count(test.max_history)),
            );
            assert_eq!(results.len(), test.expected.len());
            for (i, v) in results.iter().enumerate() {
                assert_eq!(items_equal(&v, &test.expected[i]), true);
            }
        }
    }
}
