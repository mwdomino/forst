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

// ExpirationManager handles expirations for keys
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

use std::{collections::BinaryHeap, sync::{Arc, Mutex}, time::Duration};

use tokio::{sync::Notify, time::sleep};

use crate::nestedmap::SystemTime;

use super::NestedMap;

#[derive(PartialEq, Eq, Debug, Clone)]
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

#[derive(Debug)]
pub struct ExpirationManager {
    pub entries: BinaryHeap<ExpirationEntry>,
    data: Arc<Mutex<NestedMap>>,
    notify: Arc<Notify>,
}

impl ExpirationManager {
    pub fn new(data: Arc<Mutex<NestedMap>>) -> Self {
        ExpirationManager {
            entries: BinaryHeap::new(),
            data,
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn set(&mut self, entry: ExpirationEntry) {
        self.entries.push(entry);
        self.notify.notify_one(); // Notify to cancel the existing timer
        self.schedule_next();
    }

    fn schedule_next(&self) {
        if let Some(next_expiry) = self.entries.peek() {
            let now = SystemTime::now();
            let duration = if next_expiry.expires_at > now {
                next_expiry
                    .expires_at
                    .duration_since(now)
                    .unwrap_or(Duration::new(0, 0))
            } else {
                Duration::new(0, 0)
            };

            let data_clone = self.data.clone();
            let next_entry = next_expiry.clone();
            let notify = self.notify.clone();

            tokio::spawn(async move {
                tokio::select! {
                    _ = sleep(duration) => {
                        // timeout has expired, call the eviction_callback
                        let mut data = data_clone.lock().unwrap();
                        data.eviction_callback(&next_entry.keys, next_entry.id);
                    }
                    _ = notify.notified() => {
                        // Timer was canceled
                    }
                }
            });
        }
    }
}
