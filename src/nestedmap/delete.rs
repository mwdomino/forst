use std::collections::VecDeque;
use std::str::Split;

use super::config::*;
use super::*;

impl NestedMap {
    // Removes the node at `keys` and everything below it.
    pub fn delete(&mut self, keys: &str) -> bool {
        let (parent, last) = match keys.rsplit_once(DELIMITER) {
            Some((parent, last)) => (self.node_mut(parent), last),
            None => (Some(self), keys),
        };
        parent.is_some_and(|p| p.children.remove(last).is_some())
    }

    pub fn delete_at_index(&mut self, keys: &str, index: usize) -> bool {
        self.remove_item(&mut keys.split(DELIMITER), &mut |items| {
            items.remove(index).is_some()
        })
    }

    pub fn delete_by_id(&mut self, keys: &str, id: i64) -> bool {
        self.remove_item(&mut keys.split(DELIMITER), &mut |items| match items
            .iter()
            .position(|item| item.id == id)
        {
            Some(idx) => items.remove(idx).is_some(),
            None => false,
        })
    }

    fn node_mut(&mut self, keys: &str) -> Option<&mut NestedMap> {
        keys.split(DELIMITER)
            .try_fold(self, |node, key| node.children.get_mut(key))
    }

    // Applies `remove` to the items at the end of `keys`, then prunes nodes left empty.
    fn remove_item(
        &mut self,
        keys: &mut Split<'_, &str>,
        remove: &mut dyn FnMut(&mut VecDeque<Item>) -> bool,
    ) -> bool {
        let Some(key) = keys.next() else {
            return remove(&mut self.items);
        };
        let Some(child) = self.children.get_mut(key) else {
            return false;
        };
        let removed = child.remove_item(keys, remove);
        if removed && child.is_empty() {
            self.children.remove(key);
        }
        removed
    }
}

#[cfg(test)]
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
                    nm.set("a", create_item("a", b"the value a"), None);
                }),
                search_keys: "a".to_string(),
                expected: Vec::new(),
                max_history: 1,
            },
            TestCase {
                name: "Test depth 3",
                setup: Box::new(|nm| {
                    nm.set("a.b.c", create_item("a.b.c", b"the value abc"), None);
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
                        create_item("a.b.c.d.e", b"the value abcde"),
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
                    nm.set("a", create_item("a", b"the value a"), None);
                    nm.set("a.b", create_item("a.b", b"the value ab"), None);
                    nm.set("a.b.c", create_item("a.b.c", b"the value abc"), None);
                    nm.set("a.b.c.d", create_item("a.b.c.d", b"the value abcd"), None);
                    nm.set(
                        "a.b.c.d.e",
                        create_item("a.b.c.d.e", b"the value abcde"),
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
                    nm.set("a", create_item("a", b"the value a"), None);
                    nm.set("a.b", create_item("a.b", b"the value ab"), None);
                    nm.set("a.b.c", create_item("a.b.c", b"the value abc"), None);
                    nm.set("a.b.c.d", create_item("a.b.c.d", b"the value abcd"), None);
                    nm.set(
                        "a.b.c.d.e",
                        create_item("a.b.c.d.e", b"the value abcde"),
                        None,
                    );
                    nm.set(
                        "a.b.c.d.e.f",
                        create_item("a.b.c.d.e.f", b"the value abcdef"),
                        None,
                    );
                    nm.set(
                        "a.b.c.d.e.f.g",
                        create_item("a.b.c.d.e.f.g", b"the value abcdefg"),
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
            create_item("a.b.c", b"value1"),
            Some(SetOptions::new().preserve_history(true)),
        );
        nm.set(
            "a.b.c",
            create_item("a.b.c", b"value2"),
            Some(SetOptions::new().preserve_history(true)),
        );
        nm.set(
            "a.b.c",
            create_item("a.b.c", b"value3"),
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
            create_item("a.b.c", b"value1"),
            Some(SetOptions::new().preserve_history(true)),
        );

        // delete index 0
        let r: bool = nm.delete_at_index("a.b.c", 0);
        assert!(r);

        let items: Vec<Item> = nm.query("a.b.c", Some(GetOptions::new().history_count(3)));
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn test_delete_by_id_prunes_empty_nodes() {
        let mut nm = NestedMap::new(1);
        nm.set("a.b.c", create_item("a.b.c", b"abc"), None);
        nm.set("a.x", create_item("a.x", b"ax"), None);

        assert!(nm.delete_by_id("a.b.c", 1));
        assert!(!nm.children["a"].children.contains_key("b"));
        assert!(nm.get("a.x").is_some());
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
