use std::collections::BinaryHeap;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicI64, Arc};
use std::time::{Duration, SystemTime};
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
}

impl Datastore {
    pub fn new(max_history: usize, expiration_poll: Option<Duration>) -> Self {
        let ds = Datastore {
            map: Arc::new(Mutex::new(NestedMap::new(max_history))),
            ttl: Arc::new(Mutex::new(BinaryHeap::new())),
            id_counter: Arc::new(AtomicI64::new(0)),
        };


        if let Some(interval) = expiration_poll {
            ds.start_polling(interval);
        }

        ds
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
        let ds = Arc::new(Datastore::new(1, Some(Duration::from_millis(50))));

        // set value with ttl
        ds.clone()
          .set(
              "a.b.c".to_string(),
              b"abc",
              Some(SetOptions::new().ttl(Duration::from_millis(100))),
          )
          .await;

        ds.clone()
          .set(
              "a.b.d".to_string(),
              b"abd",
              Some(SetOptions::new().ttl(Duration::from_millis(200))),
          )
          .await;

        ds.clone()
          .set(
              "a.b.e".to_string(),
              b"abe",
              Some(SetOptions::new().ttl(Duration::from_millis(400))),
          )
          .await;

        // get values
        let items = ds.query("a.b.>", None).await;
        assert_eq!(items.len(), 3);

        // check first expiration (at 150ms)
        sleep(Duration::from_millis(150)).await;
        let items = ds.query("a.b.>", None).await;
        assert_eq!(items.len(), 2);

        if ds.get("a.b.c").await.is_some() {
            panic!("Found key that should have been removed! a.b.c")
        }

        // check second expiration (at 250ms)
        sleep(Duration::from_millis(100)).await;
        let items = ds.query("a.b.>", None).await;
        assert_eq!(items.len(), 1);

        if ds.get("a.b.d").await.is_some() {
            panic!("Found key that should have been removed! a.b.d")
        }

        // check last expiration (at 450ms)
        sleep(Duration::from_millis(200)).await;
        let items = ds.query("a.b.>", None).await;
        assert_eq!(items.len(), 0);

        if ds.get("a.b.e").await.is_some() {
            panic!("Found key that should have been removed! a.b.e")
        }
    }
}
