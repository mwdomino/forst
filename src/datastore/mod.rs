use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tokio::sync::{broadcast, Notify};

use crate::nestedmap::config::{COLLECTOR, DELIMITER, WILDCARD};
use crate::nestedmap::options::{GetOptions, SetOptions};
use crate::nestedmap::query::matches;
use crate::nestedmap::set::SetOutcome;
use crate::nestedmap::NestedMap;
use expiration::ExpirationHeap;

pub use crate::nestedmap::Item;

pub mod expiration;

// Change batches buffered per subscriber before it is cut off as lagged.
const SUBSCRIBER_BUFFER: usize = 1024;

// One item to write. Timestamps and ttl are nanoseconds.
#[derive(Debug, Clone, Default)]
pub struct SetItem {
    pub key: String,
    pub value: Vec<u8>,
    // Source timestamp since the Unix epoch. 0 means the server receive time.
    pub timestamp_ns: i64,
    // Lifetime after timestamp_ns. 0 means never expire.
    pub ttl_ns: i64,
    pub preserve_history: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Rejected {
    pub index: usize,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Set,
    Delete,
    Expire,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub kind: EventKind,
    pub item: Item,
}

// All changes made under one write lock hold, in order.
#[derive(Debug)]
pub struct ChangeBatch {
    pub lsn: u64,
    pub events: Vec<Event>,
}

impl ChangeBatch {
    pub fn matching<'a>(&'a self, pattern: &'a str) -> impl Iterator<Item = &'a Event> {
        self.events
            .iter()
            .filter(move |e| matches(pattern, &e.item.key))
    }
}

pub struct Subscription {
    // The newest matching item per key at the moment of subscribing
    pub snapshot: Vec<Item>,
    // Every change after the snapshot, exactly once. Lagged means: subscribe again.
    pub changes: broadcast::Receiver<Arc<ChangeBatch>>,
}

#[derive(Debug)]
struct State {
    map: NestedMap,
    ttl: ExpirationHeap,
    // Log sequence number of the last change batch
    lsn: u64,
}

#[derive(Debug)]
struct Shared {
    state: RwLock<State>,
    wake_reaper: Notify,
    changes: broadcast::Sender<Arc<ChangeBatch>>,
}

impl Shared {
    // Commits one change batch. Call with the write lock held, so subscribers that register
    // under the read lock see each batch either in their snapshot or as a change, never both.
    fn commit(&self, state: &mut State, events: Vec<Event>) {
        state.lsn += 1;
        if !events.is_empty() {
            // Fails only when there are no subscribers
            let _ = self.changes.send(Arc::new(ChangeBatch {
                lsn: state.lsn,
                events,
            }));
        }
    }

    fn has_subscribers(&self) -> bool {
        self.changes.receiver_count() > 0
    }
}

#[derive(Debug)]
pub struct Datastore {
    shared: Arc<Shared>,
}

pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

impl Datastore {
    // Must be called inside a tokio runtime: it spawns the expiration task.
    pub fn new(max_history: usize) -> Self {
        let shared = Arc::new(Shared {
            state: RwLock::new(State {
                map: NestedMap::new(max_history),
                ttl: ExpirationHeap::new(),
                lsn: 0,
            }),
            wake_reaper: Notify::new(),
            changes: broadcast::channel(SUBSCRIBER_BUFFER).0,
        });
        tokio::spawn(expiration::reap(shared.clone()));
        Datastore { shared }
    }

    // Applies every valid item under one write lock and returns the invalid ones.
    // An item whose (key, timestamp) is already stored, or that is older than what is kept,
    // is skipped silently, so retrying a batch is safe.
    pub fn set_batch(&self, items: Vec<SetItem>) -> Vec<Rejected> {
        let now = now_ns();
        let mut rejected = Vec::new();
        let mut valid = Vec::with_capacity(items.len());
        for (index, item) in items.into_iter().enumerate() {
            match validate(item, now) {
                Ok(v) => valid.push(v),
                Err(reason) => rejected.push(Rejected { index, reason }),
            }
        }
        if valid.is_empty() {
            return rejected;
        }

        // Copy items for subscribers before locking, to keep the write lock hold short.
        // The check repeats under the lock, so a subscriber that just joined misses nothing.
        let clone_all = |valid: &[(Item, bool)]| valid.iter().map(|(i, _)| i.clone()).collect();
        let copies: Option<Vec<Item>> = self.shared.has_subscribers().then(|| clone_all(&valid));

        let mut state = self.shared.state.write();
        let copies: Vec<Item> = match (self.shared.has_subscribers(), copies) {
            (false, _) => Vec::new(),
            (true, Some(copies)) => copies,
            (true, None) => clone_all(&valid),
        };
        let mut copies = copies.into_iter();
        let mut events = Vec::new();
        let mut applied = false;
        let mut wake = false;
        // Freed after the lock is released, to keep the write lock hold short
        let mut evicted = Vec::new();

        for (item, preserve_history) in valid {
            let event = copies.next();
            let options = SetOptions::new().preserve_history(preserve_history);
            let SetOutcome::Applied {
                schedule,
                evicted: old,
            } = state.map.set(item, Some(options))
            else {
                continue;
            };
            applied = true;
            evicted.extend(old);
            if let Some((at, key)) = schedule {
                wake |= state.ttl.push(at, key);
            }
            if let Some(item) = event {
                events.push(Event {
                    kind: EventKind::Set,
                    item,
                });
            }
        }

        if applied {
            self.shared.commit(&mut state, events);
        }
        drop(state);
        drop(evicted);
        if wake {
            self.shared.wake_reaper.notify_one();
        }
        rejected
    }

    pub fn set(
        &self,
        key: String,
        value: Vec<u8>,
        options: Option<SetOptions>,
    ) -> Result<(), &'static str> {
        let options = options.unwrap_or_default();
        let item = SetItem {
            key,
            value,
            timestamp_ns: 0,
            ttl_ns: i64::try_from(options.ttl.as_nanos()).unwrap_or(i64::MAX),
            preserve_history: options.preserve_history,
        };
        match self.set_batch(vec![item]).pop() {
            Some(r) => Err(r.reason),
            None => Ok(()),
        }
    }

    pub fn get(&self, key: &str) -> Option<Item> {
        self.shared.state.read().map.get(key).cloned()
    }

    // Results are sorted by key segments; each key's history stays newest first.
    pub fn query(&self, key: &str, options: Option<GetOptions>) -> Vec<Item> {
        let mut items = self.shared.state.read().map.query(key, options);
        // Sort outside the lock; the stable sort keeps history order
        items.sort_by(|a, b| a.key.split(DELIMITER).cmp(b.key.split(DELIMITER)));
        items
    }

    pub fn delete(&self, key: &str) -> bool {
        self.remove(|map| map.delete(key))
    }

    pub fn delete_at_index(&self, key: &str, index: usize) -> bool {
        self.remove(|map| map.delete_at_index(key, index).into_iter().collect())
    }

    pub fn subscribe(&self, pattern: &str) -> Subscription {
        let state = self.shared.state.read();
        Subscription {
            changes: self.shared.changes.subscribe(),
            snapshot: state.map.query(pattern, None),
        }
    }

    // Runs `remove` under the write lock and publishes what it removed as Delete events.
    fn remove(&self, remove: impl FnOnce(&mut NestedMap) -> Vec<Item>) -> bool {
        let mut state = self.shared.state.write();
        let removed = remove(&mut state.map);
        if removed.is_empty() {
            return false;
        }
        let events = if self.shared.has_subscribers() {
            let kind = EventKind::Delete;
            removed
                .into_iter()
                .map(|item| Event { kind, item })
                .collect()
        } else {
            Vec::new()
        };
        self.shared.commit(&mut state, events);
        true
    }
}

fn validate(item: SetItem, now: i64) -> Result<(Item, bool), &'static str> {
    let bad_segment = |s: &str| s.is_empty() || s == WILDCARD || s == COLLECTOR;
    if item.key.split(DELIMITER).any(bad_segment) {
        return Err("key has an empty, \"*\" or \">\" segment");
    }
    if item.timestamp_ns < 0 {
        return Err("timestamp_ns is negative");
    }
    if item.ttl_ns < 0 {
        return Err("ttl is negative");
    }

    let timestamp = if item.timestamp_ns == 0 {
        now
    } else {
        item.timestamp_ns
    };
    // A ttl too large to represent never expires.
    let expires_at = match item.ttl_ns {
        0 => None,
        ttl => timestamp.checked_add(ttl),
    };
    if expires_at.is_some_and(|at| at <= now) {
        return Err("already expired: timestamp_ns + ttl is in the past");
    }

    let stored = Item {
        key: item.key,
        value: item.value.into(),
        timestamp,
        expires_at,
    };
    Ok((stored, item.preserve_history))
}

// Time from now until `at` (ns since epoch), zero if past.
fn until(at: i64) -> Duration {
    Duration::from_nanos(at.saturating_sub(now_ns()).max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::{RecvError, TryRecvError};

    use tokio::time::sleep;

    fn set_ttl(ds: &Datastore, key: &str, ms: u64) {
        let opts = SetOptions::new().ttl(Duration::from_millis(ms));
        ds.set(key.to_string(), b"abc".to_vec(), Some(opts))
            .unwrap();
    }

    fn item(key: &str, ts: i64) -> SetItem {
        SetItem {
            key: key.to_string(),
            value: format!("{key}@{ts}").into_bytes(),
            timestamp_ns: ts,
            ..SetItem::default()
        }
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

    // An overwritten key keeps one heap entry, and the newest value survives the old expiry.
    #[tokio::test]
    async fn test_overwrite_reschedules_expiry() {
        let ds = Datastore::new(1);

        set_ttl(&ds, "k", 50);
        set_ttl(&ds, "k", 150);
        set_ttl(&ds, "k", 150);
        assert_eq!(ds.shared.state.read().ttl.len(), 1);

        sleep(Duration::from_millis(80)).await;
        assert!(ds.get("k").is_some());
        sleep(Duration::from_millis(120)).await;
        assert!(ds.get("k").is_none());
        assert_eq!(ds.shared.state.read().ttl.len(), 0);
    }

    #[tokio::test]
    async fn test_huge_ttl_never_expires() {
        let ds = Datastore::new(1);
        let opts = SetOptions::new().ttl(Duration::from_secs(i64::MAX as u64));
        ds.set("a".to_string(), b"abc".to_vec(), Some(opts))
            .unwrap();
        assert!(ds.get("a").unwrap().expires_at.is_none());
    }

    #[tokio::test]
    async fn test_set_batch_rejects_only_invalid_items() {
        let ds = Datastore::new(1);
        let expired = SetItem {
            ttl_ns: 1,
            ..item("a.old", 1)
        };
        let rejected = ds.set_batch(vec![
            item("a.ok", 0),
            item("a..b", 0),
            item("a.*", 0),
            expired,
            item("a.neg", -1),
            item("a.ok2", 0),
        ]);
        let indexes: Vec<usize> = rejected.iter().map(|r| r.index).collect();
        assert_eq!(indexes, vec![1, 2, 3, 4]);
        assert_eq!(ds.query("a.>", None).len(), 2);
    }

    #[tokio::test]
    async fn test_subscribe_snapshot_then_changes() {
        let ds = Datastore::new(1);
        ds.set_batch(vec![item("dev.r1.cpu", 1), item("dev.r1.mem", 1)]);

        let mut sub = ds.subscribe("dev.*.cpu");
        assert_eq!(sub.snapshot.len(), 1);
        assert_eq!(sub.snapshot[0].key, "dev.r1.cpu");

        ds.set_batch(vec![item("dev.r2.cpu", 2), item("dev.r2.mem", 2)]);
        ds.delete("dev.r1");
        ds.set(
            "dev.r3.cpu".to_string(),
            b"x".to_vec(),
            Some(SetOptions::new().ttl(Duration::from_millis(20))),
        )
        .unwrap();
        sleep(Duration::from_millis(60)).await;

        let mut seen = Vec::new();
        let mut last_lsn = 0;
        loop {
            match sub.changes.try_recv() {
                Ok(batch) => {
                    assert!(batch.lsn > last_lsn);
                    last_lsn = batch.lsn;
                    seen.extend(
                        batch
                            .matching("dev.*.cpu")
                            .map(|e| (e.kind, e.item.key.clone())),
                    );
                }
                Err(TryRecvError::Empty) => break,
                Err(e) => panic!("{e:?}"),
            }
        }
        let k = |s: &str| s.to_string();
        assert_eq!(
            seen,
            vec![
                (EventKind::Set, k("dev.r2.cpu")),
                (EventKind::Delete, k("dev.r1.cpu")),
                (EventKind::Set, k("dev.r3.cpu")),
                (EventKind::Expire, k("dev.r3.cpu")),
            ]
        );
    }

    // A subscriber that joins during writes must end with exactly the store's state:
    // nothing missed between snapshot and changes, nothing applied twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_subscribe_during_writes_has_no_gap() {
        let ds = Arc::new(Datastore::new(1));
        let writer = {
            let ds = ds.clone();
            tokio::task::spawn_blocking(move || {
                for ts in 1..=2000 {
                    let batch = (0..50)
                        .map(|k| item(&format!("k.{}", (ts + k) % 97), ts))
                        .collect();
                    ds.set_batch(batch);
                }
            })
        };

        sleep(Duration::from_millis(2)).await;
        let mut sub = ds.subscribe(">");
        let mut view: std::collections::HashMap<String, i64> = sub
            .snapshot
            .iter()
            .map(|i| (i.key.clone(), i.timestamp))
            .collect();
        assert!(
            !view.is_empty(),
            "subscribed before any write; test proves nothing"
        );

        // Read concurrently with the writer until it has been quiet for a while
        let idle = Duration::from_millis(200);
        while let Ok(change) = tokio::time::timeout(idle, sub.changes.recv()).await {
            let batch = change.expect("lagged; raise SUBSCRIBER_BUFFER");
            for e in &batch.events {
                let prev = view.insert(e.item.key.clone(), e.item.timestamp);
                assert!(
                    prev.is_none_or(|p| p < e.item.timestamp),
                    "event seen twice"
                );
            }
        }
        writer.await.unwrap();

        let store: std::collections::HashMap<String, i64> = ds
            .query(">", None)
            .into_iter()
            .map(|i| (i.key, i.timestamp))
            .collect();
        assert_eq!(view, store);
    }

    #[tokio::test]
    async fn test_slow_subscriber_lags() {
        let ds = Datastore::new(1);
        let mut sub = ds.subscribe(">");
        for ts in 1..=(SUBSCRIBER_BUFFER as i64 + 1) {
            ds.set_batch(vec![item("k", ts)]);
        }
        assert!(matches!(
            sub.changes.recv().await,
            Err(RecvError::Lagged(_))
        ));
    }
}
