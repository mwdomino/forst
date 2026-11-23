use super::config::*;
use super::options::GetOptions;
use super::{Item, NestedMap};

impl NestedMap {
    pub fn query(&self, keys: &str, options: Option<GetOptions>) -> Vec<Item> {
        let options = options.unwrap_or_default();
        let mut results = Vec::new();
        let keys: Vec<&str> = keys.split(DELIMITER).collect();
        self.query_recursive(&keys, &mut results, options.history_count);
        results
    }

    fn query_recursive(&self, keys: &[&str], results: &mut Vec<Item>, history_max: usize) {
        let Some((&next_key, remaining_keys)) = keys.split_first() else {
            results.extend(self.items.iter().take(history_max).cloned());
            return;
        };

        match next_key {
            // Recurse into every child when "*" is encountered
            WILDCARD => {
                for child in self.children.values() {
                    child.query_recursive(remaining_keys, results, history_max);
                }
            }
            // Collect everything below this level, but not this level itself
            COLLECTOR => {
                for child in self.children.values() {
                    child.collect_all(results, history_max);
                }
            }
            _ => {
                if let Some(child) = self.children.get(next_key) {
                    child.query_recursive(remaining_keys, results, history_max);
                }
            }
        }
    }

    fn collect_all(&self, results: &mut Vec<Item>, history_max: usize) {
        results.extend(self.items.iter().take(history_max).cloned());
        for child in self.children.values() {
            child.collect_all(results, history_max);
        }
    }
}

// Whether query(pattern) would return items stored at `key`. Used to route subscription events.
pub fn matches(pattern: &str, key: &str) -> bool {
    let mut keys = key.split(DELIMITER);
    for p in pattern.split(DELIMITER) {
        let key = keys.next();
        match p {
            COLLECTOR => return key.is_some(),
            WILDCARD if key.is_some() => {}
            _ if key == Some(p) => {}
            _ => return false,
        }
    }
    keys.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nestedmap::test_helpers::*;

    #[test]
    fn test_queries() {
        let test_cases = vec![
            TestCase {
                name: "Test exact match",
                setup: Box::new(|nm| {
                    nm.set("a.b.c", create_item("a.b.c", b"exact value"), None);
                }),
                search_keys: "a.b.c".to_string(),
                expected: vec![create_item("a.b.c", b"exact value")],
                max_history: 1,
            },
            TestCase {
                name: "Test wildcard match",
                setup: Box::new(|nm| {
                    nm.set("a.b", create_item("a.b", b"wildcard value ab"), None);
                    nm.set("a.b.c", create_item("a.b.c", b"wildcard value abc"), None);
                    nm.set("a.b.x", create_item("a.b.x", b"wildcard value abx"), None);
                    nm.set("a.b.y", create_item("a.b.y", b"wildcard value aby"), None);
                    nm.set(
                        "a.b.z.z",
                        create_item("a.b.z.z", b"wildcard value abzz"),
                        None,
                    );
                }),
                search_keys: "a.b.*".to_string(),
                expected: vec![
                    create_item("a.b.c", b"wildcard value abc"),
                    create_item("a.b.x", b"wildcard value abx"),
                    create_item("a.b.y", b"wildcard value aby"),
                ],
                max_history: 1,
            },
            TestCase {
                name: "Test prefix match",
                setup: Box::new(|nm| {
                    nm.set("a.b.c", create_item("a.b.c", b"prefix value abc"), None);
                    nm.set("a.b.x", create_item("a.b.x", b"prefix value abx"), None);
                    nm.set("a.b.y", create_item("a.b.y", b"prefix value aby"), None);
                    nm.set(
                        "a.b.y.z",
                        create_item("a.b.y.z", b"prefix value abyz"),
                        None,
                    );
                    nm.set(
                        "a.b.y.z.z",
                        create_item("a.b.y.z.z", b"prefix value abyzz"),
                        None,
                    );
                }),
                search_keys: "a.b.y.>".to_string(),
                expected: vec![
                    create_item("a.b.y.z", b"prefix value abyz"),
                    create_item("a.b.y.z.z", b"prefix value abyzz"),
                ],
                max_history: 1,
            },
            TestCase {
                name: "Test prefix and wildcard match",
                setup: Box::new(|nm| {
                    nm.set("a.b.c", create_item("a.b.c", b"prefix value abc"), None);
                    nm.set("a.c.x", create_item("a.c.x", b"prefix value acx"), None);
                    nm.set("a.d.y", create_item("a.d.y", b"prefix value ady"), None);
                    nm.set(
                        "a.e.y.z",
                        create_item("a.e.y.z", b"prefix value aeyz"),
                        None,
                    );
                    nm.set(
                        "a.f.y.z.z",
                        create_item("a.f.y.z.z", b"prefix value afyzz"),
                        None,
                    );
                }),
                search_keys: "a.*.y.>".to_string(),
                expected: vec![
                    create_item("a.e.y.z", b"prefix value aeyz"),
                    create_item("a.f.y.z.z", b"prefix value afyzz"),
                ],
                max_history: 1,
            },
            TestCase {
                name: "Test prefix match #2",
                setup: Box::new(|nm| {
                    nm.set(
                        "interface.lab1.p01.rk01.esr1a.management0.oper-status",
                        create_item(
                            "interface.lab1.p01.rk01.esr1a.management0.oper-status",
                            b"up",
                        ),
                        None,
                    );
                    nm.set(
                        "interface.lab1.p01.rk01.esr1a.ethernet1.oper-status",
                        create_item("interface.lab1.p01.rk01.esr1a.ethernet1.oper-status", b"up"),
                        None,
                    );
                    nm.set(
                        "interface.lab1.p01.rk01.esr1a.ethernet2.oper-status",
                        create_item("interface.lab1.p01.rk01.esr1a.ethernet2.oper-status", b"up"),
                        None,
                    );
                    nm.set(
                        "interface.lab1.p01.rk01.esr1a.management0.admin-status",
                        create_item(
                            "interface.lab1.p01.rk01.esr1a.management0.admin-status",
                            b"up",
                        ),
                        None,
                    );
                    nm.set(
                        "interface.lab1.p01.rk01.esr1a.ethernet1.admin-status",
                        create_item(
                            "interface.lab1.p01.rk01.esr1a.ethernet1.admin-status",
                            b"up",
                        ),
                        None,
                    );
                    nm.set(
                        "interface.lab1.p01.rk01.esr1a.ethernet2.admin-status",
                        create_item(
                            "interface.lab1.p01.rk01.esr1a.ethernet2.admin-status",
                            b"up",
                        ),
                        None,
                    );
                    nm.set(
                        "interface.lab1.p01.rk01.esr1a.management0.ifindex",
                        create_item(
                            "interface.lab1.p01.rk01.esr1a.management0.ifindex",
                            b"999999",
                        ),
                        None,
                    );
                    nm.set(
                        "interface.lab1.p01.rk01.esr1a.ethernet1.ifindex",
                        create_item("interface.lab1.p01.rk01.esr1a.ethernet1.ifindex", b"1"),
                        None,
                    );
                    nm.set(
                        "interface.lab1.p01.rk01.esr1a.ethernet2.ifindex",
                        create_item("interface.lab1.p01.rk01.esr1a.ethernet2.ifindex", b"2"),
                        None,
                    );
                }),
                search_keys: "interface.lab1.p01.rk01.esr1a.management0.>".to_string(),
                expected: vec![
                    create_item(
                        "interface.lab1.p01.rk01.esr1a.management0.oper-status",
                        b"up",
                    ),
                    create_item(
                        "interface.lab1.p01.rk01.esr1a.management0.admin-status",
                        b"up",
                    ),
                    create_item(
                        "interface.lab1.p01.rk01.esr1a.management0.ifindex",
                        b"999999",
                    ),
                ],
                max_history: 1,
            },
            TestCase {
                name: "Test prefix match #3",
                setup: Box::new(|nm| {
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.session-state",
                        create_item("bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.session-state", b"established"),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-state",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-state",
                            b"established",
                        ),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.local-as",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.local-as",
                            b"65000",
                        ),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-as",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-as",
                            b"65000",
                        ),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-description",
                        create_item("bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-description", b"esr1b"),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-type",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-type",
                            b"internal",
                        ),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-group",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-group",
                            b"default",
                        ),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.session-state",
                        create_item("bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.session-state", b"established"),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-state",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-state",
                            b"established",
                        ),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.local-as",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.local-as",
                            b"65000",
                        ),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-as",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-as",
                            b"65000",
                        ),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-description",
                        create_item("bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-description", b"esr1b"),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-type",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-type",
                            b"internal",
                        ),
                        None,
                    );
                    nm.set(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-group",
                        create_item(
                            "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-group",
                            b"default",
                        ),
                        None,
                    );
                }),
                search_keys: "bgp.neighbor.lab1.p01.rk01.*.*.peer-ip.*.*".to_string(),
                expected: vec![
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.session-state",
                        b"established",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-state",
                        b"established",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.local-as",
                        b"65000",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-as",
                        b"65000",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-description",
                        b"esr1b",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-type",
                        b"internal",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-group",
                        b"default",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.session-state",
                        b"established",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-state",
                        b"established",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.local-as",
                        b"65000",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-as",
                        b"65000",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-description",
                        b"esr1b",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-type",
                        b"internal",
                    ),
                    create_item(
                        "bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-group",
                        b"default",
                    ),
                ],
                max_history: 1,
            },
        ];

        query_tests(test_cases)
    }

    fn query_tests(test_cases: Vec<TestCase>) {
        for test in test_cases {
            let mut nm = NestedMap::new(test.max_history);
            (test.setup)(&mut nm);
            let results = nm.query(
                &test.search_keys,
                Some(GetOptions::new().history_count(test.max_history)),
            );
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

    #[test]
    fn test_matches_agrees_with_query() {
        let keys = ["a", "a.b", "a.b.c", "a.b.x", "a.c.c", "a.b.c.d", "b.b.c"];
        let mut nm = NestedMap::new(1);
        for key in keys {
            nm.set(key, create_item(key, b"v"), None);
        }

        let patterns = [
            "a",
            "a.b",
            "a.*",
            "a.*.c",
            "*.b.c",
            "a.>",
            "a.b.>",
            ">",
            "*",
            "a.b.c.d.e",
            "x.>",
        ];
        for pattern in patterns {
            let mut from_query: Vec<String> =
                nm.query(pattern, None).into_iter().map(|i| i.key).collect();
            let mut from_matches: Vec<String> = keys
                .iter()
                .filter(|k| matches(pattern, k))
                .map(|k| k.to_string())
                .collect();
            from_query.sort();
            from_matches.sort();
            assert_eq!(from_query, from_matches, "pattern {pattern}");
        }
    }
}
