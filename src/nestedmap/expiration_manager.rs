// expiration manager handles expirations for keys
//
// each set() call will set an ExpirationEntry containing a key,
// the timestamp that key expires, and a unique ID to identify the entry.
//
// We will run a timer delaying cleanup until the first key is scheduled to expire
// This timer will need to be updated whenever the heap is reordered in case a closer
// expiry has been inserted at the top.
//
// Once the timer fires, we will peek/pop entries off the top of the heap until we
// run into an event whose expiry time has not occured. We will then reset the timer
// to the expiry of that next event.
//
// When deleting events, we will pull the list of Items at the key path scheduled for expiration
// and then iterate through them looking for the unique ID. If it is found, we delete it, if not
// we simply return. In either case we will remove the ExpirationEntry from the heap.

use crate::nestedmap::SystemTime;

#[derive(PartialEq, Eq, Debug)]
pub struct ExpirationEntry {
    pub expires_at: SystemTime,
    pub id: i64,
    pub keys: String,
}

impl Ord for ExpirationEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap
        other.expires_at.cmp(&self.expires_at)
    }
}

impl PartialOrd for ExpirationEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
