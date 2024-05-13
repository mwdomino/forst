use super::config::*;
use super::options::GetOptions;
use super::{Item, NestedMap, NestedValue};

impl NestedMap {
    pub fn query(&self, keys: &str, options: Option<GetOptions>) -> Vec<Item> {
        let options = options.unwrap_or_default();
        let mut results = Vec::new();
        let keys: Vec<&str> = keys.split(DELIMITER).collect();
        self.query_recursive(&keys, self, &mut results, options.history_count);
        results
    }

    fn query_recursive<'a>(
        &self,
        keys: &[&'a str],
        current: &NestedMap,
        results: &mut Vec<Item>,
        history_max: usize,
    ) {
        if keys.is_empty() {
            // Collect items at the current level using VALUE_KEY
            if let Some(NestedValue::Items(items)) = current.data.get(VALUE_KEY) {
                results.extend(items.iter().take(history_max).cloned());
            }
            return;
        }

        let next_key = keys[0];
        let remaining_keys = &keys[1..];

        match next_key {
            WILDCARD => {
                // Iterate through all entries in the current map
                for (key, value) in &current.data {
                    if key == VALUE_KEY {
                        if let NestedValue::Items(items) = value {
                            results.extend(items.iter().take(history_max).cloned());
                        }
                    } else if let NestedValue::Map(nested_map) = value {
                        // Recurse into every nested map when "*" is encountered
                        self.query_recursive(&remaining_keys, nested_map, results, history_max);
                    }
                }
            }
            COLLECTOR => {
                self.collect_all(current, results, true, history_max);
            }
            _ => {
                if let Some(NestedValue::Map(nested_map)) = current.data.get(next_key) {
                    if remaining_keys.is_empty() {
                        // Check for VALUE_KEY in the last map
                        if let Some(NestedValue::Items(items)) = nested_map.data.get(VALUE_KEY) {
                            results.extend(items.iter().take(history_max).cloned());
                        }
                    } else {
                        self.query_recursive(&remaining_keys, nested_map, results, history_max);
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
                results.extend(items.iter().take(history_max).cloned());
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
    use crate::nestedmap::test_helpers::*;
    use crate::*;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_queries() {
        let test_cases = vec![
            TestCase {
                name: "Test exact match",
                setup: Box::new(|nm| {
                    nm.set(&"a.b.c".to_string(), b"exact value", None);
                }),
                search_keys: "a.b.c".to_string(),
                expected: vec![create_item("a.b.c", b"exact value")],
                max_history: 1,
            },
            TestCase {
                name: "Test wildcard match",
                setup: Box::new(|nm| {
                    nm.set(&"a.b.c".to_string(), b"wildcard value abc", None);
                    nm.set(&"a.b.x".to_string(), b"wildcard value abx", None);
                    nm.set(&"a.b.y".to_string(), b"wildcard value aby", None);
                    nm.set(&"a.b.z.z".to_string(), b"wildcard value abzz", None);
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
                    nm.set(&"a.b.c".to_string(), b"prefix value abc", None);
                    nm.set(&"a.b.x".to_string(), b"prefix value abx", None);
                    nm.set(&"a.b.y".to_string(), b"prefix value aby", None);
                    nm.set(&"a.b.y.z".to_string(), b"prefix value abyz", None);
                    nm.set(&"a.b.y.z.z".to_string(), b"prefix value abyzz", None);
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
                    nm.set(&"a.b.c".to_string(), b"prefix value abc", None);
                    nm.set(&"a.c.x".to_string(), b"prefix value acx", None);
                    nm.set(&"a.d.y".to_string(), b"prefix value ady", None);
                    nm.set(&"a.e.y.z".to_string(), b"prefix value aeyz", None);
                    nm.set(&"a.f.y.z.z".to_string(), b"prefix value afyzz", None);
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
                        &"interface.lab1.p01.rk01.esr1a.management0.oper-status".to_string(),
                        b"up",
                        None,
                    );
                    nm.set(
                        &"interface.lab1.p01.rk01.esr1a.ethernet1.oper-status".to_string(),
                        b"up",
                        None,
                    );
                    nm.set(
                        &"interface.lab1.p01.rk01.esr1a.ethernet2.oper-status".to_string(),
                        b"up",
                        None,
                    );
                    nm.set(
                        &"interface.lab1.p01.rk01.esr1a.management0.admin-status".to_string(),
                        b"up",
                        None,
                    );
                    nm.set(
                        &"interface.lab1.p01.rk01.esr1a.ethernet1.admin-status".to_string(),
                        b"up",
                        None,
                    );
                    nm.set(
                        &"interface.lab1.p01.rk01.esr1a.ethernet2.admin-status".to_string(),
                        b"up",
                        None,
                    );
                    nm.set(
                        &"interface.lab1.p01.rk01.esr1a.management0.ifindex".to_string(),
                        b"999999",
                        None,
                    );
                    nm.set(
                        &"interface.lab1.p01.rk01.esr1a.ethernet1.ifindex".to_string(),
                        b"1",
                        None,
                    );
                    nm.set(
                        &"interface.lab1.p01.rk01.esr1a.ethernet2.ifindex".to_string(),
                        b"2",
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
                        &"bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.session-state"
                            .to_string(),
                        b"established",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-state"
                            .to_string(),
                        b"established",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.local-as"
                            .to_string(),
                        b"65000",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-as"
                            .to_string(),
                        b"65000",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-description"
                            .to_string(),
                        b"esr1b",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-type"
                            .to_string(),
                        b"internal",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1b.default.peer-ip.1_1_1_1.peer-group"
                            .to_string(),
                        b"default",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.session-state"
                            .to_string(),
                        b"established",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-state"
                            .to_string(),
                        b"established",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.local-as"
                            .to_string(),
                        b"65000",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-as"
                            .to_string(),
                        b"65000",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-description"
                            .to_string(),
                        b"esr1b",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-type"
                            .to_string(),
                        b"internal",
                        None,
                    );
                    nm.set(
                        &"bgp.neighbor.lab1.p01.rk01.esr1a.default.peer-ip.1_1_1_2.peer-group"
                            .to_string(),
                        b"default",
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
}
