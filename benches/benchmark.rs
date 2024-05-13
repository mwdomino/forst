use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::distributions::{Alphanumeric, Distribution};
use rand::{thread_rng, Rng};
use rs_datastore::nestedmap::options::*;
use rs_datastore::nestedmap::{Item, NestedMap}; // Import your NestedMap module

fn bench_get(c: &mut Criterion) {
    let mut nm = NestedMap::new(1);

    nm.set(&"a.b.c.d.e".to_string(), b"some value a", None);

    c.bench_function("get_key a.b.c.d.e", |b| {
        b.iter(|| {
            let _ = nm.get(&"a.b.c.d.e".to_string());
        });
    });
}

fn bench_set(c: &mut Criterion) {
    let mut nm = NestedMap::new(5);

    c.bench_function("set_key a.b.c.d.e", |b| {
        b.iter(|| {
            nm.set(
                &"a.b.c.d.e".to_string(),
                b"some value a",
                Some(SetOptions::new().preserve_history(true)),
            );
        });
    });
}

fn bench_query_direct(c: &mut Criterion) {
    let mut nm = NestedMap::new(5);
    seed_queries(&mut nm);

    c.bench_function("query exact a.b.c", |b| {
        b.iter(|| {
            nm.query(&"a.b.c".to_string(), None);
        });
    });
}

fn bench_query_wildcard(c: &mut Criterion) {
    let mut nm = NestedMap::new(5);
    seed_queries(&mut nm);

    c.bench_function("query wildcard a.b.*", |b| {
        b.iter(|| {
            nm.query(&"a.b.*".to_string(), None);
        });
    });
}

fn bench_query_prefix(c: &mut Criterion) {
    let mut nm = NestedMap::new(5);
    seed_queries(&mut nm);

    c.bench_function("query prefix a.b.y.>", |b| {
        b.iter(|| {
            nm.query(&"a.b.y.>".to_string(), None);
        });
    });
}

fn bench_query_interface(c: &mut Criterion) {
    let mut nm = NestedMap::new(5);
    seed_queries(&mut nm);

    c.bench_function(
        "query interface interface.lab1.p01.rk01.esr1a.management0.>",
        |b| {
            b.iter(|| {
                nm.query(
                    &"interface.lab1.p01.rk01.esr1a.management0.>".to_string(),
                    None,
                );
            });
        },
    );
}

fn seed_queries(nm: &mut NestedMap) {
    // wildcards
    nm.set(&"a.b.c".to_string(), b"wildcard value abc", None);
    nm.set(&"a.b.x".to_string(), b"wildcard value abx", None);
    nm.set(&"a.b.y".to_string(), b"wildcard value aby", None);
    nm.set(&"a.b.z.z".to_string(), b"wildcard value abzz", None);

    // prefix
    nm.set(&"a.b.c".to_string(), b"prefix value abc", None);
    nm.set(&"a.b.x".to_string(), b"prefix value abx", None);
    nm.set(&"a.b.y".to_string(), b"prefix value aby", None);
    nm.set(&"a.b.y.z".to_string(), b"prefix value abyz", None);
    nm.set(&"a.b.y.z.z".to_string(), b"prefix value abyzz", None);

    // deep
    nm.set(
        &"interface.lab1.p01.rk01.esr1a.management0.oper-status".to_string(),
        b"up",
        None,
    );
    nm.set(
        &"interface.lab1.p01.rk01.esr1a.ethernet1.oper-status".to_string(),
        b"up",
        None,
    );
    nm.set(
        &"interface.lab1.p01.rk01.esr1a.ethernet2.oper-status".to_string(),
        b"up",
        None,
    );
    nm.set(
        &"interface.lab1.p01.rk01.esr1a.management0.admin-status".to_string(),
        b"up",
        None,
    );
    nm.set(
        &"interface.lab1.p01.rk01.esr1a.ethernet1.admin-status".to_string(),
        b"up",
        None,
    );
    nm.set(
        &"interface.lab1.p01.rk01.esr1a.ethernet2.admin-status".to_string(),
        b"up",
        None,
    );
    nm.set(
        &"interface.lab1.p01.rk01.esr1a.management0.ifindex".to_string(),
        b"999999",
        None,
    );
    nm.set(
        &"interface.lab1.p01.rk01.esr1a.ethernet1.ifindex".to_string(),
        b"1",
        None,
    );
    nm.set(
        &"interface.lab1.p01.rk01.esr1a.ethernet2.ifindex".to_string(),
        b"2",
        None,
    );

    nm.set(
        &"a.b.c.d.e".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
    nm.set(
        &"a.b.c.d.e.f".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
    nm.set(
        &"a.b.c.d.e.f".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
    nm.set(
        &"a.b.c.d.e".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
    nm.set(
        &"a.b.c.d.e".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
    nm.set(
        &"a.b.c.d.e".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
    nm.set(
        &"a.b.c.d.e".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
    nm.set(
        &"a.b.c.d.e".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
    nm.set(
        &"a.b.c.d.e".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
    nm.set(
        &"a.b.c.d.e".to_string(),
        b"some value a",
        Some(SetOptions::new().preserve_history(true)),
    );
}

fn random_key() -> String {
    let mut rng = thread_rng();
    let len = rng.gen_range(1..10); // Random length between 1 and 10 for each part
    (0..5)
        .map(|_| {
            Alphanumeric
                .sample_iter(&mut rng)
                .take(len)
                .map(char::from)
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn bench_set_varying_ttls(c: &mut Criterion) {
    let mut nm = NestedMap::new(1);
    seed_100k_keys(&mut nm);

    let key = random_key();
    let ttl = std::time::Duration::from_secs(thread_rng().gen_range(60..86400)); // TTL between 1 minute and 1 day

    c.bench_function("set with varying TTLs in heap", |b| {
        b.iter(|| {
            nm.set(&key, b"this is a value", Some(SetOptions::new().ttl(ttl)));
        });
    });
}

fn bench_set_diverse_keys(c: &mut Criterion) {
    let mut nm = NestedMap::new(1);
    seed_100k_keys(&mut nm);
    let ttl = std::time::Duration::from_secs(thread_rng().gen_range(60..86400)); // TTL between 1 minute and 1 day
    let key = random_key();

    c.bench_function("set with diverse keys in heap", |b| {
        b.iter(|| {
            nm.set(&key, b"this is a value", Some(SetOptions::new().ttl(ttl)));
        });
    });
}

fn seed_100k_keys(nm: &mut NestedMap) {
    for i in 0..100_000 {
        let part1 = (i % 100).to_string(); // Cycle through 0 to 99
        let part2 = ((i / 100) % 100).to_string(); // Cycle through 0 to 99
        let part3 = (i / 10_000).to_string(); // Cycle through 0 to 9

        let key = format!("{}.{}.{}", part1, part2, part3);
        let value = b"sample_value";

        let options = Some(SetOptions {
            ttl: std::time::Duration::from_secs(60 * 5),
            preserve_history: false,
        });

        nm.set(&key, value, options);
    }
}

//criterion_group!(
//    benches,
//    bench_set,
//    bench_set_diverse_keys,
//    bench_set_varying_ttls
//);
criterion_group!(
    benches,
    bench_get,
    bench_set,
    bench_set_diverse_keys,
    bench_set_varying_ttls,
    bench_query_direct,
    bench_query_wildcard,
    bench_query_prefix,
    bench_query_interface
);
criterion_main!(benches);
