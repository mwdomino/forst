use crate::datastore::Datastore;
use std::time::{Duration, SystemTime};
use std::sync::Arc;
use tokio::time::sleep;
use tokio::sync::Notify;

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
    pub async fn set_ttl(self: Arc<Self>, entry: ExpirationEntry) {
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

    pub async fn schedule_next(self: Arc<Self>) {
        let next_expiry = {
            let ttl = self.ttl.lock().await;
            ttl.peek().cloned()
        };

        if let Some(next_expiry) = next_expiry {
            let now = SystemTime::now();
            let duration = if next_expiry.expires_at > now {
                next_expiry.expires_at.duration_since(now).unwrap_or(Duration::new(0, 0))
            } else {
                Duration::new(0, 0)
            };

            let self_weak = Arc::downgrade(&self);
            tokio::spawn(async move {
                Datastore::handle_timer(self_weak, duration, next_expiry).await;
            });
        }
    }

    async fn handle_timer(self_weak: Weak<Self>, duration: Duration, next_expiry: ExpirationEntry) {
        tokio::select! {
            _ = sleep(duration) => {
                if let Some(self_arc) = self_weak.upgrade() {
                    let mut data = self_arc.map.lock().await;
                    data.delete_by_id(&next_expiry.key, next_expiry.id);
                    self_arc.schedule_next().await;
                }
            }
            _ = Notify::notified(&self_weak.upgrade().expect("Datastore must be valid").notify) => {
                if let Some(self_arc) = self_weak.upgrade() {
                    self_arc.schedule_next().await;
                }
            }
        }
    }
}
