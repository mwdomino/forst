use crate::datastore::Datastore;
use crate::nestedmap::NestedMap;

use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::time::sleep;
use tokio::sync::Mutex;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ExpirationEntry {
    pub expires_at: SystemTime,
    pub id: i64,
    pub key: String,
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

impl Datastore {
    pub async fn set_ttl(&self, entry: ExpirationEntry) {
        let mut ttl = self.ttl.lock().await;

        ttl.push(entry);
        drop(ttl);
    }

    pub fn start_polling(&self, interval: Duration) {
        let map = Arc::clone(&self.map);
        let ttl = Arc::clone(&self.ttl);

        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                Datastore::process_expired_entries(&map, &ttl).await;
            }
        });
    }

    async fn process_expired_entries(map: &Arc<Mutex<NestedMap>>, ttl: &Arc<Mutex<BinaryHeap<ExpirationEntry>>>) {
        loop {
            let mut ttl_guard = ttl.lock().await;

            if let Some(top) = ttl_guard.peek() {
                let now = SystemTime::now();

                // clean up expired entry
                if top.expires_at <= now {
                    if let Some(expired_entry) = ttl_guard.pop() {
                        println!("Processing expired entry: {:?} at: {:?}", expired_entry, SystemTime::now());

                        let mut map_guard = map.lock().await;

                        map_guard.delete_by_id(&expired_entry.key, expired_entry.id);
                    }
                } else {
                    // top not expired
                    break;
                }
            } else {
                // heap empty
                break;
            }

            // Drop the lock before continuing the loop to avoid holding the lock while processing
            drop(ttl_guard);
        }
    }
}
