use super::*;

impl NestedMap {
    pub fn get(&self, keys: &str) -> Option<&Item> {
        self.node(keys)?.items.front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nestedmap::test_helpers::*;

    #[test]
    fn test_get_exact() {
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

        get_tests(test_cases)
    }

    // "__value" was once a reserved internal key; it must behave like any other segment.
    #[test]
    fn test_get_reserved_looking_segment() {
        let mut nm = NestedMap::new(1);
        nm.set("a.__value", create_item("a.__value", b"child"), None);
        nm.set("a", create_item("a", b"parent"), None);

        assert_eq!(nm.get("a").unwrap().value, b"parent");
        assert_eq!(nm.get("a.__value").unwrap().value, b"child");
    }

    fn get_tests(test_cases: Vec<TestCase>) {
        for test in test_cases {
            let mut nm = NestedMap::new(test.max_history);
            (test.setup)(&mut nm);

            if let Some(item) = nm.get(&test.search_keys) {
                assert!(items_equal(item, &test.expected[0]));
            }
        }
    }
}
