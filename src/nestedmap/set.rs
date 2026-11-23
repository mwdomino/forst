use super::config::*;
use super::options::SetOptions;
use super::{Item, NestedMap};

impl NestedMap {
    pub fn set(&mut self, keys: &str, value: Item, options: Option<SetOptions>) {
        let options = options.unwrap_or_default();
        let max_history = self.max_history;
        let mut node = self;

        // Traverse to the appropriate node, allocating a key only for new nodes
        for key in keys.split(DELIMITER) {
            if !node.children.contains_key(key) {
                node.children
                    .insert(key.to_owned(), NestedMap::new(max_history));
            }
            node = node.children.get_mut(key).unwrap();
        }

        let items = &mut node.items;
        if !options.preserve_history {
            match items.front_mut() {
                Some(front) => *front = value,
                None => items.push_front(value),
            }
            return;
        }

        // Prepend new item to the list to keep the newest items at the start
        if items.len() >= max_history {
            items.pop_back(); // Remove the oldest item if we exceed the max history
        }
        items.push_front(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nestedmap::options::*;
    use crate::nestedmap::test_helpers::*;

    #[test]
    fn test_set() {
        //let mut nm = NestedMap::new(1);

        let test_cases = vec![
            TestCase {
                name: "Test depth 1",
                setup: Box::new(|nm| {
                    nm.set("a", create_item("a", b"the value a"), None);
                }),
                search_keys: "a".to_string(),
                expected: vec![create_item("a", b"the value a")],
                max_history: 1,
            },
            TestCase {
                name: "Test depth 3",
                setup: Box::new(|nm| {
                    nm.set("a.b.c", create_item("a.b.c", b"the value abc"), None);
                }),
                search_keys: "a.b.c".to_string(),
                expected: vec![create_item("a.b.c", b"the value abc")],
                max_history: 1,
            },
            TestCase {
                name: "Test depth 6",
                setup: Box::new(|nm| {
                    nm.set(
                        "a.b.c.d.e.f",
                        create_item("a.b.c.d.e.f", b"the value abcdef"),
                        None,
                    );
                }),
                search_keys: "a.b.c.d.e.f".to_string(),
                expected: vec![create_item("a.b.c.d.e.f", b"the value abcdef")],
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
                        "a.b.c.d",
                        create_item("a.b.c.d", &format!("value{}", i).into_bytes()),
                        Some(SetOptions::new().preserve_history(false)),
                    );
                }
            }),
            search_keys: "a.b.c.d".to_string(),
            expected: vec![create_item("a.b.c.d", b"value7")],
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
                            "a.b.c.d",
                            create_item("a.b.c.d", &format!("value{}", i).into_bytes()),
                            Some(SetOptions::new().preserve_history(true)),
                        );
                    }
                }),
                search_keys: "a.b.c.d".to_string(),
                expected: vec![
                    create_item("a.b.c.d", b"value7"),
                    create_item("a.b.c.d", b"value6"),
                    create_item("a.b.c.d", b"value5"),
                    create_item("a.b.c.d", b"value4"),
                    create_item("a.b.c.d", b"value3"),
                ],
                max_history: 5,
            },
            TestCase {
                name: "Test less than max_history values",
                setup: Box::new(|nm| {
                    for i in 1..=3 {
                        nm.set(
                            "a.b.c.d",
                            create_item("a.b.c.d", &format!("value{}", i).into_bytes()),
                            Some(SetOptions::new().preserve_history(true)),
                        );
                    }
                }),
                search_keys: "a.b.c.d".to_string(),
                expected: vec![
                    create_item("a.b.c.d", b"value3"),
                    create_item("a.b.c.d", b"value2"),
                    create_item("a.b.c.d", b"value1"),
                ],
                max_history: 5,
            },
            TestCase {
                name: "Test exactly max_history values",
                setup: Box::new(|nm| {
                    for i in 1..=5 {
                        nm.set(
                            "a.b.c.d",
                            create_item("a.b.c.d", &format!("value{}", i).into_bytes()),
                            Some(SetOptions::new().preserve_history(true)),
                        );
                    }
                }),
                search_keys: "a.b.c.d".to_string(),
                expected: vec![
                    create_item("a.b.c.d", b"value5"),
                    create_item("a.b.c.d", b"value4"),
                    create_item("a.b.c.d", b"value3"),
                    create_item("a.b.c.d", b"value2"),
                    create_item("a.b.c.d", b"value1"),
                ],
                max_history: 5,
            },
        ];

        set_tests(test_cases)
    }

    #[test]
    fn test_set_mixed_history() {
        let test_cases = vec![TestCase {
            name: "Overwrite without history replaces only the newest item",
            setup: Box::new(|nm| {
                nm.set(
                    "a.b.c.d",
                    create_item("a.b.c.d", b"value1"),
                    Some(SetOptions::new().preserve_history(true)),
                );
                nm.set(
                    "a.b.c.d",
                    create_item("a.b.c.d", b"value2"),
                    Some(SetOptions::new().preserve_history(true)),
                );
                nm.set(
                    "a.b.c.d",
                    create_item("a.b.c.d", b"value3"),
                    Some(SetOptions::new().preserve_history(false)),
                );
                nm.set(
                    "a.b.c.d",
                    create_item("a.b.c.d", b"value4"),
                    Some(SetOptions::new().preserve_history(true)),
                );
                nm.set(
                    "a.b.c.d",
                    create_item("a.b.c.d", b"value5"),
                    Some(SetOptions::new().preserve_history(true)),
                );
            }),
            search_keys: "a.b.c.d".to_string(),
            expected: vec![
                create_item("a.b.c.d", b"value5"),
                create_item("a.b.c.d", b"value4"),
                create_item("a.b.c.d", b"value3"),
                create_item("a.b.c.d", b"value1"),
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

            assert_eq!(results.len(), test.expected.len(), "{}", test.name);
            for (i, v) in results.iter().enumerate() {
                assert!(items_equal(v, &test.expected[i]), "{}", test.name);
            }
        }
    }
}
