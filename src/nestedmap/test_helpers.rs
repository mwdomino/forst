use super::*;

pub fn items_equal(a: &Item, b: &Item) -> bool {
    a.key == b.key && a.value == b.value
}

pub struct TestCase {
    pub name: &'static str,
    pub setup: Box<dyn Fn(&mut NestedMap)>,
    pub search_keys: String,
    pub expected: Vec<Item>,
    pub max_history: usize,
}

#[macro_export]
macro_rules! vec_string {
    ( $( $x:expr ),* ) => {
        vec![
            $( $x.to_string(), )*
        ]
    };
}
