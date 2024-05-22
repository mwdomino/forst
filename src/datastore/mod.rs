use std::collections::BinaryHeap;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicI64, Arc};
use std::time::SystemTime;
use tokio::sync::{Mutex, Notify};

use crate::nestedmap::options::{GetOptions, SetOptions};
use crate::nestedmap::NestedMap;
use expiration::ExpirationEntry;

pub use crate::nestedmap::Item;

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
    pub async fn set(self: Arc<Self>, key: String, value: &[u8], options: Option<SetOptions>) {
        let mut map = self.clone().map.lock().await;

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

    pub async fn query(&self, key: &str, options: Option<GetOptions>) -> Vec<Item> {
        let map = self.map.lock().await;
        map.query(key, options)
    }
}

mod tests {
    use super::*;
    use std::time::Duration;

    use tokio::time::sleep;

    #[tokio::test]
    async fn test_expiration() {
        let ds = Arc::new(Datastore::new(1));

        // set value with ttl
        println!("#### SETTING A");
        ds.clone().set(
            "a.b.c".to_string(),
            b"abc",
            Some(SetOptions::new().ttl(Duration::from_millis(100))),
        )
        .await;

        println!("#### SETTING B");
        ds.clone().set(
            "a.b.d".to_string(),
            b"abd",
            Some(SetOptions::new().ttl(Duration::from_millis(200))),
        )
        .await;

        println!("#### SETTING C");
        ds.clone().set(
            "a.b.e".to_string(),
            b"abe",
            Some(SetOptions::new().ttl(Duration::from_millis(300))),
        )
        .await;



        // get values
        let items = ds.query("a.b.>", None).await;
        assert_eq!(items.len(), 3);

        // check first expiration
        sleep(Duration::from_millis(120)).await;

        if ds.get("a.b.c").await.is_some() {
            panic!("Found key that should have been removed! a.b.c")
        }

        // check second expiration
        sleep(Duration::from_millis(220)).await;

        if ds.get("a.b.d").await.is_some() {
            panic!("Found key that should have been removed! a.b.d")
        }

        // check last expiration
        sleep(Duration::from_millis(320)).await;

        if ds.get("a.b.e").await.is_some() {
            panic!("Found key that should have been removed! a.b.e")
        }
    }
}
