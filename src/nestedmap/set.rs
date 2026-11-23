use super::config::*;
use super::options::SetOptions;
use super::{Item, NestedMap};

#[derive(Debug, PartialEq, Eq)]
pub enum SetOutcome {
    // `schedule` is set when the caller must add an expiry entry for this key at that time.
    // `evicted` is the item this write replaced or pushed out of history.
    Applied {
        schedule: Option<(i64, String)>,
        evicted: Option<Item>,
    },
    // An item with the same timestamp is already stored.
    Duplicate,
    // Older than what is kept: the current item without history, or all kept history.
    Stale,
}

impl NestedMap {
    // Stores `value` at `value.key`.
    pub fn set(&mut self, value: Item, options: Option<SetOptions>) -> SetOutcome {
        let options = options.unwrap_or_default();
        let max_history = self.max_history.max(1);
        let mut node = self;

        // Traverse to the appropriate node, allocating a key only for new nodes
        for key in value.key.split(DELIMITER) {
            if !node.children.contains_key(key) {
                node.children
                    .insert(key.to_owned(), NestedMap::new(node.max_history));
            }
            node = node.children.get_mut(key).unwrap();
        }

        let timestamp = value.timestamp;
        // Keep one pending expiry entry per node: add one only if this item expires first
        let schedule = value
            .expires_at
            .filter(|&at| node.scheduled_expiry.is_none_or(|s| at < s))
            .map(|at| (at, value.key.clone()));
        let items = &mut node.items;
        let mut evicted = None;

        if !options.preserve_history {
            match items.front_mut() {
                None => items.push_front(value),
                Some(front) if timestamp > front.timestamp => {
                    evicted = Some(std::mem::replace(front, value))
                }
                Some(front) if timestamp == front.timestamp => return SetOutcome::Duplicate,
                Some(_) => return SetOutcome::Stale,
            }
        } else {
            // Insert by timestamp, so retried or reordered writes land in place
            let pos = items.partition_point(|item| item.timestamp > timestamp);
            if items
                .get(pos)
                .is_some_and(|item| item.timestamp == timestamp)
            {
                return SetOutcome::Duplicate;
            }
            if pos >= max_history {
                return SetOutcome::Stale;
            }
            items.insert(pos, value);
            if items.len() > max_history {
                evicted = items.pop_back();
            }
        }

        if let Some((at, _)) = schedule {
            node.scheduled_expiry = Some(at);
        }
        SetOutcome::Applied { schedule, evicted }
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
                    nm.set(create_item("a", b"the value a"), None);
                }),
                search_keys: "a".to_string(),
                expected: vec![create_item("a", b"the value a")],
                max_history: 1,
            },
            TestCase {
                name: "Test depth 3",
                setup: Box::new(|nm| {
                    nm.set(create_item("a.b.c", b"the value abc"), None);
                }),
                search_keys: "a.b.c".to_string(),
                expected: vec![create_item("a.b.c", b"the value abc")],
                max_history: 1,
            },
            TestCase {
                name: "Test depth 6",
                setup: Box::new(|nm| {
                    nm.set(create_item("a.b.c.d.e.f", b"the value abcdef"), None);
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
                    create_item("a.b.c.d", b"value1"),
                    Some(SetOptions::new().preserve_history(true)),
                );
                nm.set(
                    create_item("a.b.c.d", b"value2"),
                    Some(SetOptions::new().preserve_history(true)),
                );
                nm.set(
                    create_item("a.b.c.d", b"value3"),
                    Some(SetOptions::new().preserve_history(false)),
                );
                nm.set(
                    create_item("a.b.c.d", b"value4"),
                    Some(SetOptions::new().preserve_history(true)),
                );
                nm.set(
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

    fn item_at(key: &str, value: &[u8], timestamp: i64) -> Item {
        Item {
            timestamp,
            ..create_item(key, value)
        }
    }

    #[test]
    fn test_set_is_idempotent_and_ordered_by_timestamp() {
        let history = Some(SetOptions::new().preserve_history(true));
        let mut nm = NestedMap::new(3);

        for ts in [20, 10, 30, 20, 30] {
            nm.set(item_at("h", b"v", ts), history.clone());
        }
        let ts: Vec<i64> = nm
            .query("h", Some(GetOptions::new().history_count(3)))
            .iter()
            .map(|i| i.timestamp)
            .collect();
        assert_eq!(ts, vec![30, 20, 10]);

        // Full history: older than everything kept is stale, newer pushes out the oldest
        assert_eq!(
            nm.set(item_at("h", b"v", 5), history.clone()),
            SetOutcome::Stale
        );
        nm.set(item_at("h", b"v", 40), history.clone());
        assert_eq!(
            nm.query("h", Some(GetOptions::new().history_count(3)))
                .len(),
            3
        );
        assert_eq!(
            nm.query("h", Some(GetOptions::new().history_count(3)))[2].timestamp,
            20
        );
    }

    #[test]
    fn test_set_without_history_is_last_write_wins() {
        let mut nm = NestedMap::new(1);
        let applied = SetOutcome::Applied {
            schedule: None,
            evicted: None,
        };

        assert_eq!(nm.set(item_at("k", b"new", 20), None), applied);
        assert_eq!(nm.set(item_at("k", b"old", 10), None), SetOutcome::Stale);
        assert_eq!(
            nm.set(item_at("k", b"dup", 20), None),
            SetOutcome::Duplicate
        );
        assert_eq!(nm.get("k").unwrap().value, &b"new"[..]);
    }

    #[test]
    fn test_set_schedules_one_expiry_per_node() {
        let mut nm = NestedMap::new(1);
        let expiring = |ts, at| Item {
            expires_at: Some(at),
            ..item_at("k", b"v", ts)
        };

        assert!(matches!(
            nm.set(expiring(1, 100), None),
            SetOutcome::Applied {
                schedule: Some((100, _)),
                ..
            }
        ));
        // A later expiry needs no new entry: the pending one reschedules when it fires
        assert!(matches!(
            nm.set(expiring(2, 200), None),
            SetOutcome::Applied {
                schedule: None,
                evicted: Some(_)
            }
        ));
        assert_eq!(nm.expire("k", 100, 100), (vec![], Some(200)));
    }
}
