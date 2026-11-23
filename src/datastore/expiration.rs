use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;

use super::{now_ns, until, Event, EventKind, Shard};

// Max keys checked per write-lock hold, so a mass expiry does not stall readers.
const REAP_BATCH: usize = 1024;
// Upper bound on one sleep, so a wall-clock change delays expiry by at most this much.
const MAX_SLEEP: Duration = Duration::from_secs(1);

// Keys to check for expired items, earliest first. A node holds at most one live entry.
#[derive(Debug, Default)]
pub struct ExpirationHeap(BinaryHeap<Reverse<(i64, String)>>);

impl ExpirationHeap {
    pub fn new() -> Self {
        Self::default()
    }

    // Returns true when `at` is now the earliest entry, so the reaper must wake.
    pub fn push(&mut self, at: i64, key: String) -> bool {
        let earliest = self.peek().is_none_or(|next| at < next);
        self.0.push(Reverse((at, key)));
        earliest
    }

    pub fn peek(&self) -> Option<i64> {
        self.0.peek().map(|e| e.0 .0)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn pop_due(&mut self, now: i64) -> Option<(i64, String)> {
        if self.peek()? > now {
            return None;
        }
        self.0.pop().map(|e| e.0)
    }
}

// Sleeps until the next expiry (or until woken for an earlier one) and removes expired items.
pub(super) async fn reap(shared: Arc<Shard>) {
    loop {
        let now = now_ns();
        let (checked, next) = {
            let mut state = shared.state.write();
            let state = &mut *state;
            let publish = shared.has_subscribers();
            let mut events = Vec::new();
            let mut removed_any = false;
            let mut checked = 0;

            while checked < REAP_BATCH {
                let Some((at, key)) = state.ttl.pop_due(now) else {
                    break;
                };
                checked += 1;
                let (expired, next) = state.map.expire(&key, now, at);
                removed_any |= !expired.is_empty();
                if publish {
                    events.extend(expired.into_iter().map(|item| Event {
                        kind: EventKind::Expire,
                        item,
                    }));
                }
                if let Some(at) = next {
                    state.ttl.push(at, key);
                }
            }
            if removed_any {
                shared.commit(state, events);
            }
            (checked, state.ttl.peek())
        };

        if checked == REAP_BATCH {
            tokio::task::yield_now().await;
            continue;
        }

        match next {
            Some(at) => tokio::select! {
                _ = tokio::time::sleep(until(at).min(MAX_SLEEP)) => {}
                _ = shared.wake_reaper.notified() => {}
            },
            None => shared.wake_reaper.notified().await,
        }
    }
}
