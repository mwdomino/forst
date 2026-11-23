use std::cmp::Reverse;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::nestedmap::options::{GetOptions, SetOptions};
use crate::nestedmap::NestedMap;
use expiration::{ExpirationEntry, ExpirationHeap};

pub use crate::nestedmap::Item;

pub mod expiration;

#[derive(Debug)]
pub struct Datastore {
    map: Arc<RwLock<NestedMap>>,
    ttl: Arc<Mutex<ExpirationHeap>>,
    wake: Arc<Notify>,
    id_counter: AtomicI64,
}

impl Datastore {
    // Must be called inside a tokio runtime: it spawns the expiration task.
    pub fn new(max_history: usize) -> Self {
        let datastore = Datastore {
            map: Arc::new(RwLock::new(NestedMap::new(max_history))),
            ttl: Arc::default(),
            wake: Arc::default(),
            id_counter: AtomicI64::new(0),
        };

        tokio::spawn(expiration::reap(
            datastore.map.clone(),
            datastore.ttl.clone(),
            datastore.wake.clone(),
        ));
        datastore
    }

    pub fn set(&self, key: String, value: Vec<u8>, options: Option<SetOptions>) {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let ttl = options.as_ref().map(|o| o.ttl).filter(|ttl| !ttl.is_zero());

        let new_item = Item {
            key: key.clone(),
            value,
            timestamp: SystemTime::now(),
            id,
        };

        // Insert before scheduling expiry, so the reaper never misses the item.
        self.map.write().set(&key, new_item, options);

        // A ttl too large to represent never expires.
        if let Some(expires_at) = ttl.and_then(|ttl| Instant::now().checked_add(ttl)) {
            let mut heap = self.ttl.lock();
            let earliest = heap.peek().is_none_or(|e| expires_at < e.0.expires_at);
            heap.push(Reverse(ExpirationEntry {
                expires_at,
                id,
                key,
            }));
            drop(heap);
            if earliest {
                self.wake.notify_one();
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<Item> {
        self.map.read().get(key).cloned()
    }

    pub fn query(&self, key: &str, options: Option<GetOptions>) -> Vec<Item> {
        self.map.read().query(key, options)
    }

    pub fn delete(&self, key: &str) -> bool {
        self.map.write().delete(key)
    }

    pub fn delete_at_index(&self, key: &str, index: usize) -> bool {
        self.map.write().delete_at_index(key, index)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    fn set_ttl(ds: &Datastore, key: &str, ms: u64) {
        let opts = SetOptions::new().ttl(Duration::from_millis(ms));
        ds.set(key.to_string(), b"abc".to_vec(), Some(opts));
    }

    #[tokio::test]
    async fn test_expiration() {
        let ds = Datastore::new(1);

        set_ttl(&ds, "a.b.c", 100);
        set_ttl(&ds, "a.b.b", 100);
        set_ttl(&ds, "a.b.d", 200);
        set_ttl(&ds, "a.b.e", 400);
        assert_eq!(ds.query("a.b.>", None).len(), 4);

        sleep(Duration::from_millis(110)).await;
        assert_eq!(ds.query("a.b.>", None).len(), 2);
        assert!(ds.get("a.b.c").is_none());

        sleep(Duration::from_millis(110)).await;
        assert_eq!(ds.query("a.b.>", None).len(), 1);
        assert!(ds.get("a.b.d").is_none());

        sleep(Duration::from_millis(210)).await;
        assert_eq!(ds.query("a.b.>", None).len(), 0);
        assert!(ds.get("a.b.e").is_none());
    }

    // An earlier expiry added after a later one must wake the reaper early.
    #[tokio::test]
    async fn test_earlier_expiry_wakes_reaper() {
        let ds = Datastore::new(1);

        set_ttl(&ds, "a.late", 10_000);
        sleep(Duration::from_millis(10)).await;
        set_ttl(&ds, "a.early", 50);

        sleep(Duration::from_millis(100)).await;
        assert!(ds.get("a.early").is_none());
        assert!(ds.get("a.late").is_some());
    }

    #[tokio::test]
    async fn test_huge_ttl_never_expires() {
        let ds = Datastore::new(1);
        let opts = SetOptions::new().ttl(Duration::from_secs(i64::MAX as u64));
        ds.set("a".to_string(), b"abc".to_vec(), Some(opts));
        assert!(ds.get("a").is_some());
    }
}
