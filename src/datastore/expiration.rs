use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio::time::{sleep_until, Instant};

use crate::nestedmap::NestedMap;

// Max expired entries removed per write-lock hold, so a mass expiry does not stall readers.
const REAP_BATCH: usize = 1024;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ExpirationEntry {
    pub expires_at: Instant,
    pub id: i64,
    pub key: String,
}

pub type ExpirationHeap = BinaryHeap<Reverse<ExpirationEntry>>;

// Sleeps until the next expiry (or until `wake` signals an earlier one) and removes expired items.
pub async fn reap(
    map: Arc<RwLock<NestedMap>>,
    heap: Arc<Mutex<ExpirationHeap>>,
    wake: Arc<Notify>,
) {
    loop {
        let now = Instant::now();
        let mut expired = Vec::new();
        let next = {
            let mut heap = heap.lock();
            while expired.len() < REAP_BATCH && heap.peek().is_some_and(|e| e.0.expires_at <= now) {
                expired.push(heap.pop().unwrap().0);
            }
            heap.peek().map(|e| e.0.expires_at)
        };

        if !expired.is_empty() {
            let mut map = map.write();
            for e in &expired {
                map.delete_by_id(&e.key, e.id);
            }
        }

        if expired.len() == REAP_BATCH {
            tokio::task::yield_now().await;
            continue;
        }

        match next {
            Some(at) => tokio::select! {
                _ = sleep_until(at) => {}
                _ = wake.notified() => {}
            },
            None => wake.notified().await,
        }
    }
}
