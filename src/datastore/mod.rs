use std::collections::BinaryHeap;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicI64, Arc};
use std::time::SystemTime;
use tokio::sync::{Mutex, Notify};

use crate::nestedmap::options::SetOptions;
use crate::nestedmap::{Item, NestedMap};

use expiration::ExpirationEntry;

pub mod expiration;

#[derive(Debug)]
pub struct Datastore {
    map: Arc<Mutex<NestedMap>>,
    ttl: Arc<Mutex<BinaryHeap<ExpirationEntry>>>,
    id_counter: Arc<AtomicI64>,
    notify: Arc<Notify>,
}

impl Datastore {
    pub fn new(max_history: usize) -> Self {
        Datastore {
            map: Arc::new(Mutex::new(NestedMap::new(max_history))),
            ttl: Arc::new(Mutex::new(BinaryHeap::new())),
            id_counter: Arc::new(AtomicI64::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    // Async method to expose set functionality
    pub async fn set(&self, key: String, value: &[u8], options: Option<SetOptions>) {
        let mut map = self.map.lock().await;

        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);

        if let Some(ref options) = options {
            if options.ttl.as_millis() > 0 {
                let expires_at = SystemTime::now() + options.ttl;
                let expiration_entry = ExpirationEntry {
                    expires_at,
                    id,
                    key: key.to_string(),
                };

                self.set_ttl(expiration_entry).await;
            }
        }

        let new_item = Item {
            key: key.to_string(),
            value: value.to_vec(),
            timestamp: SystemTime::now(),
            id,
        };

        map.set(&key, &new_item, options);
    }

    pub async fn get(&self, key: &str) -> Option<Item> {
        let map = self.map.lock().await;
        map.get(key).cloned()
    }
}

mod tests {
    use super::*;
    use std::time::Duration;

    use tokio::time::sleep;

    #[tokio::test]
    async fn test_expiration() {
        let ds = Datastore::new(1);

        // set value with ttl
        ds.set(
            "a.b.c".to_string(),
            b"abc",
            Some(SetOptions::new().ttl(Duration::from_millis(100))),
        )
        .await;

        // get value
        if ds.get("a.b.c").await.is_none() {
            panic!("Did not find key");
        }

        // sleep for 200ms
        let duration = Duration::from_millis(120);
        sleep(duration).await;

        if ds.get("a.b.c").await.is_some() {
            panic!("Found key that should have been removed!")
        }
    }
}
