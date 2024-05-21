use super::config::*;
use super::*;

impl NestedMap {
    pub fn delete(&mut self, keys: &str) -> bool {
        let keys: Vec<&str> = keys.split(DELIMITER).collect();
        let mut current_map = &mut self.data;

        for (i, key) in keys.iter().enumerate() {
            if i == keys.len() - 1 {
                // Last key, attempt to delete
                return current_map.remove(*key).is_some();
            }

            // Not the last key, dive deeper
            match current_map.get_mut(*key) {
                Some(NestedValue::Map(map)) => {
                    current_map = &mut map.data;
                }
                _ => return false, // Next key is not a map, or key not found
            }
        }

        false
    }

    pub fn delete_at_index(&mut self, keys: &str, index: usize) -> bool {
        let keys: Vec<&str> = keys.split(DELIMITER).collect();
        let mut current_map = &mut self.data;

        for (i, key) in keys.iter().enumerate() {
            if i == keys.len() - 1 {
                // At the last key, access the nested items via VALUE_KEY
                if let Some(NestedValue::Map(final_map)) = current_map.get_mut(*key) {
                    if let Some(NestedValue::Items(items)) = final_map.data.get_mut(VALUE_KEY) {
                        if index < items.len() {
                            items.remove(index);

                            // Optionally remove the VALUE_KEY if no items left
                            if items.is_empty() {
                                final_map.data.remove(VALUE_KEY);
                            }

                            return true;
                        }
                        return false; // Index out of bounds
                    }
                    return false; // VALUE_KEY not found or not containing items
                }
                return false; // Last key does not lead to a Map
            }

            // Navigate deeper into the map
            if let Some(NestedValue::Map(map)) = current_map.get_mut(*key) {
                current_map = &mut map.data;
            } else {
                return false;
            }
        }

        false
    }

    pub fn delete_by_id(&mut self, keys: &str, id: i64) -> bool {
        let keys: Vec<&str> = keys.split(DELIMITER).collect();
        let mut current_map = &mut self.data;

        for (i, key) in keys.iter().enumerate() {
            if i == keys.len() - 1 {
                // At the last key, access the nested items via VALUE_KEY
                if let Some(NestedValue::Map(final_map)) = current_map.get_mut(*key) {
                    if let Some(NestedValue::Items(items)) = final_map.data.get_mut(VALUE_KEY) {
                        for (idx, item) in items.iter().enumerate() {
                            if item.id == id {
                                items.remove(idx);

                                // Optionally remove the VALUE_KEY if no items left
                                if items.is_empty() {
                                    final_map.data.remove(VALUE_KEY);
                                }

                                return true;
                            }
                        }
                    }
                }
            }

            // Navigate deeper into the map
            if let Some(NestedValue::Map(map)) = current_map.get_mut(*key) {
                current_map = &mut map.data;
            } else {
                return false;
            }
        }

        false
    }
}

mod tests {
    #[allow(unused_imports)]
    use self::options::{GetOptions, SetOptions};

    use super::*;
    use crate::nestedmap::test_helpers::*;

    #[test]
    fn test_delete() {
        let test_cases = vec![
            TestCase {
                name: "Test depth 1",
                setup: Box::new(|nm| {
                    nm.set("a", &create_item("a", b"the value a"), None);
                }),
                search_keys: "a".to_string(),
                expected: Vec::new(),
                max_history: 1,
            },
            TestCase {
                name: "Test depth 3",
                setup: Box::new(|nm| {
                    nm.set("a.b.c", &create_item("a.b.c", b"the value abc"), None);
                }),
                search_keys: "a.b.c".to_string(),
                expected: Vec::new(),
                max_history: 1,
            },
            TestCase {
                name: "Test depth 5",
                setup: Box::new(|nm| {
                    nm.set(
                        "a.b.c.d.e",
                        &create_item("a.b.c.d.e", b"the value abcde"),
                        None,
                    );
                }),
                search_keys: "a.b.c.d.e".to_string(),
                expected: Vec::new(),
                max_history: 1,
            },
        ];

        delete_tests(test_cases)
    }

    #[test]
    fn test_delete_with_prefix() {
        let test_cases = vec![
            TestCase {
                name: "Test depth 3",
                setup: Box::new(|nm| {
                    nm.set("a", &create_item("a", b"the value a"), None);
                    nm.set("a.b", &create_item("a.b", b"the value ab"), None);
                    nm.set("a.b.c", &create_item("a.b.c", b"the value abc"), None);
                    nm.set("a.b.c.d", &create_item("a.b.c.d", b"the value abcd"), None);
                    nm.set(
                        "a.b.c.d.e",
                        &create_item("a.b.c.d.e", b"the value abcde"),
                        None,
                    );
                }),
                search_keys: "a.b.c".to_string(),
                expected: vec![
                    create_item("a.b.c.d", b"the value abcd"),
                    create_item("a.b.c", b"the value abc"),
                ],
                max_history: 1,
            },
            TestCase {
                name: "Test depth 6",
                setup: Box::new(|nm| {
                    nm.set("a", &create_item("a", b"the value a"), None);
                    nm.set("a.b", &create_item("a.b", b"the value ab"), None);
                    nm.set("a.b.c", &create_item("a.b.c", b"the value abc"), None);
                    nm.set("a.b.c.d", &create_item("a.b.c.d", b"the value abcd"), None);
                    nm.set(
                        "a.b.c.d.e",
                        &create_item("a.b.c.d.e", b"the value abcde"),
                        None,
                    );
                    nm.set(
                        "a.b.c.d.e.f",
                        &create_item("a.b.c.d.e.f", b"the value abcdef"),
                        None,
                    );
                    nm.set(
                        "a.b.c.d.e.f.g",
                        &create_item("a.b.c.d.e.f.g", b"the value abcdefg"),
                        None,
                    );
                }),
                search_keys: "a.b.c.d.e.f".to_string(),
                expected: vec![create_item("a.b.c.d.e.f.g", b"the value abcdefg")],
                max_history: 1,
            },
        ];

        delete_tests(test_cases)
    }

    #[test]
    fn test_delete_at_index() {
        let mut nm = NestedMap::new(3);

        nm.set(
            "a.b.c",
            &create_item("a.b.c", b"value1"),
            Some(SetOptions::new().preserve_history(true)),
        );
        nm.set(
            "a.b.c",
            &create_item("a.b.c", b"value2"),
            Some(SetOptions::new().preserve_history(true)),
        );
        nm.set(
            "a.b.c",
            &create_item("a.b.c", b"value3"),
            Some(SetOptions::new().preserve_history(true)),
        );

        // delete index 2
        let r: bool = nm.delete_at_index("a.b.c", 2);
        assert!(r);

        let items: Vec<Item> = nm.query("a.b.c", Some(GetOptions::new().history_count(3)));
        assert_eq!(items.len(), 2);

        assert_eq!(items[0].value, b"value3");
        assert_eq!(items[1].value, b"value2");
    }

    #[test]
    fn test_delete_at_index_last() {
        let mut nm = NestedMap::new(3);

        nm.set(
            "a.b.c",
            &create_item("a.b.c", b"value1"),
            Some(SetOptions::new().preserve_history(true)),
        );

        // delete index 0
        let r: bool = nm.delete_at_index("a.b.c", 0);
        assert!(r);

        let items: Vec<Item> = nm.query("a.b.c", Some(GetOptions::new().history_count(3)));
        assert_eq!(items.len(), 0);
    }

    fn delete_tests(test_cases: Vec<TestCase>) {
        for test in test_cases {
            let mut nm = NestedMap::new(test.max_history);
            (test.setup)(&mut nm);

            let result: bool = nm.delete(&test.search_keys);
            assert!(result);

            for exp in test.expected {
                if let Some(item) = nm.get(&exp.key) {
                    panic!("Expected {:?} to be deleted", item);
                }
            }
        }
    }
}
