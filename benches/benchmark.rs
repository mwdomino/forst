use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rs_datastore::nestedmap::{Item, NestedMap}; // Import your NestedMap module

fn bench_get(c: &mut Criterion) {
    let key: Vec<String> = vec!["a".to_string(), "b".to_string(), "c".to_string()];

    let mut nm = NestedMap::new(1);
    nm.set(&key, b"some value a");

    c.bench_function("get_key", |b| {
        b.iter(|| {
            let _ = nm.get(&key);
        });
    });
}

fn bench_set(c: &mut Criterion) {
    let key: Vec<String> = vec![
        "a".to_string(),
        "b".to_string(),
        "c".to_string(),
        "d".to_string(),
        "e".to_string(),
    ];

    let mut nm = NestedMap::new(1);

    c.bench_function("set_key", |b| {
        b.iter(|| {
            nm.set(&key, b"some value a");
        });
    });
}

criterion_group!(benches, bench_get, bench_set);
criterion_main!(benches);
