use tokio::sync::{Mutex, Notify};
use tokio::time::{sleep, Sleep};
use std::collections::BinaryHeap;
use std::sync::{atomic::AtomicI64, Arc};
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use crate::nestedmap::{NestedMap, Item};
use crate::nestedmap::options::SetOptions;

use expiration::ExpirationEntry;

pub mod expiration;

#[derive(Debug)]
pub struct Datastore {
    map: Arc<Mutex<NestedMap>>,
    ttl: Arc<Mutex<BinaryHeap<ExpirationEntry>>>,
    timer: Arc<Mutex<Option<Sleep>>>,
    id_counter: Arc<AtomicI64>,
    notify: Arc<Notify>,
}

impl Datastore {
    pub fn new(max_history: usize) -> Self {
        Datastore {
            map: Arc::new(Mutex::new(NestedMap::new(max_history))),
            ttl: Arc::new(Mutex::new(BinaryHeap::new())),
            timer: Arc::new(Mutex::new(None)),
            id_counter: Arc::new(AtomicI64::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    // Async method to expose set functionality
    pub async fn set(&self, key: String, value: &[u8], options: Option<SetOptions>) {
        let mut map = self.map.lock().await;

        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);

        if let Some(ref options) = options {
            if options.ttl.as_secs() > 0 {
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

        let ttl = self.ttl.lock().await;

        let should_notify = !ttl.is_empty();
        map.set(&key, &new_item, options);

        if should_notify {
            self.notify.notify_one();
        } else {
            self.schedule_next();
        }
    }

    pub async fn get(&self, key: &str) -> Option<Item> {
        let map = self.map.lock().await;
        map.get(key).cloned()
    }
}

