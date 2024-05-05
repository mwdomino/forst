use super::Item;

pub fn items_equal(a: &Item, b: &Item) -> bool {
    a.key == b.key && a.value == b.value
}
