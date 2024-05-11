use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rs_datastore::nestedmap::options::*;
use rs_datastore::nestedmap::{Item, NestedMap}; // Import your NestedMap module

fn bench_get(c: &mut Criterion) {
    let mut nm = NestedMap::new(1);

    nm.set(&"a.b.c.d.e".to_string(), b"some value a", None);

    c.bench_function("get_key", |b| {
        b.iter(|| {
            let _ = nm.get(&"a.b.c.d.e".to_string());
        });
    });
}

fn bench_set(c: &mut Criterion) {
    let mut nm = NestedMap::new(5);

    c.bench_function("set_key", |b| {
        b.iter(|| {
            nm.set(
                &"a.b.c.d.e".to_string(),
                b"some value a",
                Some(SetOptions::new().preserve_history(true)),
            );
        });
    });
}

criterion_group!(benches, bench_get, bench_set);
criterion_main!(benches);
