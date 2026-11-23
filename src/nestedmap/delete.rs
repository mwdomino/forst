use std::str::Split;

use super::config::*;
use super::*;

impl NestedMap {
    // Removes the node at `keys` and everything below it. Returns the removed items.
    pub fn delete(&mut self, keys: &str) -> Vec<Item> {
        let mut removed = Vec::new();
        let mut take = |node: &mut NestedMap| {
            let last = keys.rsplit(DELIMITER).next().unwrap();
            match node.children.remove(last) {
                Some(child) => {
                    child.drain_into(&mut removed);
                    true
                }
                None => false,
            }
        };
        match keys.rsplit_once(DELIMITER) {
            Some((parent, _)) => {
                self.edit(&mut parent.split(DELIMITER), &mut take);
            }
            None => {
                take(self);
            }
        }
        removed
    }

    pub fn delete_at_index(&mut self, keys: &str, index: usize) -> Option<Item> {
        let mut removed = None;
        self.edit(&mut keys.split(DELIMITER), &mut |node| {
            removed = node.items.remove(index);
            removed.is_some()
        });
        removed
    }

    // Removes the items at `keys` that expired by `now`, for the heap entry due at `entry_at`.
    // Returns them, and the time of the next expiry check the caller must schedule.
    pub fn expire(&mut self, keys: &str, now: i64, entry_at: i64) -> (Vec<Item>, Option<i64>) {
        let mut expired = Vec::new();
        let mut next = None;
        self.edit(&mut keys.split(DELIMITER), &mut |node| {
            for item in std::mem::take(&mut node.items) {
                if item.expires_at.is_some_and(|at| at <= now) {
                    expired.push(item);
                } else {
                    node.items.push_back(item);
                }
            }
            // Only the node's live entry reschedules; an outdated one would add a duplicate
            if node.scheduled_expiry == Some(entry_at) {
                next = node.items.iter().filter_map(|item| item.expires_at).min();
                node.scheduled_expiry = next;
            }
            !expired.is_empty()
        });
        (expired, next)
    }

    fn drain_into(self, out: &mut Vec<Item>) {
        out.extend(self.items);
        for child in self.children.into_values() {
            child.drain_into(out);
        }
    }

    // Applies `change` to the node at the end of `keys`, then prunes nodes it left empty.
    // `change` returns whether it removed anything.
    fn edit(
        &mut self,
        keys: &mut Split<'_, &str>,
        change: &mut dyn FnMut(&mut NestedMap) -> bool,
    ) -> bool {
        let Some(key) = keys.next() else {
            return change(self);
        };
        let Some(child) = self.children.get_mut(key) else {
            return false;
        };
        let removed = child.edit(keys, change);
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
                    nm.set(create_item("a", b"the value a"), None);
                }),
                search_keys: "a".to_string(),
                expected: Vec::new(),
                max_history: 1,
            },
            TestCase {
                name: "Test depth 3",
                setup: Box::new(|nm| {
                    nm.set(create_item("a.b.c", b"the value abc"), None);
                }),
                search_keys: "a.b.c".to_string(),
                expected: Vec::new(),
                max_history: 1,
            },
            TestCase {
                name: "Test depth 5",
                setup: Box::new(|nm| {
                    nm.set(create_item("a.b.c.d.e", b"the value abcde"), None);
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
                    nm.set(create_item("a", b"the value a"), None);
                    nm.set(create_item("a.b", b"the value ab"), None);
                    nm.set(create_item("a.b.c", b"the value abc"), None);
                    nm.set(create_item("a.b.c.d", b"the value abcd"), None);
                    nm.set(create_item("a.b.c.d.e", b"the value abcde"), None);
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
                    nm.set(create_item("a", b"the value a"), None);
                    nm.set(create_item("a.b", b"the value ab"), None);
                    nm.set(create_item("a.b.c", b"the value abc"), None);
                    nm.set(create_item("a.b.c.d", b"the value abcd"), None);
                    nm.set(create_item("a.b.c.d.e", b"the value abcde"), None);
                    nm.set(create_item("a.b.c.d.e.f", b"the value abcdef"), None);
                    nm.set(create_item("a.b.c.d.e.f.g", b"the value abcdefg"), None);
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
            create_item("a.b.c", b"value1"),
            Some(SetOptions::new().preserve_history(true)),
        );
        nm.set(
            create_item("a.b.c", b"value2"),
            Some(SetOptions::new().preserve_history(true)),
        );
        nm.set(
            create_item("a.b.c", b"value3"),
            Some(SetOptions::new().preserve_history(true)),
        );

        // delete index 2
        assert_eq!(
            nm.delete_at_index("a.b.c", 2).unwrap().value,
            &b"value1"[..]
        );

        let items: Vec<Item> = nm.query("a.b.c", Some(GetOptions::new().history_count(3)));
        assert_eq!(items.len(), 2);

        assert_eq!(items[0].value, &b"value3"[..]);
        assert_eq!(items[1].value, &b"value2"[..]);
    }

    #[test]
    fn test_delete_at_index_last() {
        let mut nm = NestedMap::new(3);

        nm.set(
            create_item("a.b.c", b"value1"),
            Some(SetOptions::new().preserve_history(true)),
        );

        // delete index 0
        assert!(nm.delete_at_index("a.b.c", 0).is_some());

        let items: Vec<Item> = nm.query("a.b.c", Some(GetOptions::new().history_count(3)));
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn test_expire_prunes_empty_nodes() {
        let mut nm = NestedMap::new(1);
        let mut item = create_item("a.b.c", b"abc");
        item.expires_at = Some(10);
        nm.set(item, None);
        nm.set(create_item("a.x", b"ax"), None);

        // An outdated entry neither removes unexpired items nor reschedules
        assert_eq!(nm.expire("a.b.c", 9, 5), (vec![], None));
        let (expired, next) = nm.expire("a.b.c", 10, 10);
        assert_eq!((expired.len(), next), (1, None));
        assert!(!nm.children["a"].children.contains_key("b"));
        assert!(nm.get("a.x").is_some());
    }

    #[test]
    fn test_delete_returns_subtree_items() {
        let mut nm = NestedMap::new(1);
        nm.set(create_item("a.b", b"ab"), None);
        nm.set(create_item("a.b.c", b"abc"), None);
        nm.set(create_item("a.x", b"ax"), None);

        assert_eq!(nm.delete("a.b").len(), 2);
        assert!(nm.get("a.x").is_some());
        assert!(nm.delete("a.b").is_empty());
    }

    fn delete_tests(test_cases: Vec<TestCase>) {
        for test in test_cases {
            let mut nm = NestedMap::new(test.max_history);
            (test.setup)(&mut nm);

            assert!(!nm.delete(&test.search_keys).is_empty(), "{}", test.name);

            for exp in test.expected {
                if let Some(item) = nm.get(&exp.key) {
                    panic!("Expected {:?} to be deleted", item);
                }
            }
        }
    }
}
