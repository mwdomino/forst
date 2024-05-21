use crate::datastore::Datastore;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

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

        let should_notify = !ttl.is_empty();

        ttl.push(entry);
        drop(ttl);

        if should_notify {
            self.notify.notify_one();
        } else {
            self.schedule_next().await;
        }
    }

    pub async fn schedule_next(&self) {
        let ttl = self.ttl.lock().await;

        if let Some(next_expiry) = ttl.peek() {
            let now = SystemTime::now();
            let duration = if next_expiry.expires_at > now {
                next_expiry
                    .expires_at
                    .duration_since(now)
                    .unwrap_or(Duration::new(0, 0))
            } else {
                Duration::new(0, 0)
            };

            let data_clone = self.map.clone();
            let next_entry = next_expiry.clone();
            let notify = self.notify.clone();

            tokio::spawn(async move {
                tokio::select! {
                    _ = sleep(duration) => {
                        // timeout has expired, call the eviction_callback
                        let mut data = data_clone.lock().await;
                        data.delete_by_id(&next_entry.key, next_entry.id);
                    }
                    _ = notify.notified() => {
                        // Timer was canceled
                    }
                }
            });
        }
    }
}
