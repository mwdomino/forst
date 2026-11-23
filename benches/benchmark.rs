use criterion::{criterion_group, criterion_main, Criterion};
use rs_datastore::nestedmap::options::*;
use rs_datastore::nestedmap::test_helpers::create_item;
use rs_datastore::nestedmap::NestedMap;
use std::hint::black_box;

fn bench_get(c: &mut Criterion) {
    let mut nm = NestedMap::new(1);
    nm.set("a.b.c.d.e", &create_item("a.b.c.d.e", b"some value a"), None);

    c.bench_function("get_key a.b.c.d.e", |b| {
        b.iter(|| black_box(nm.get(black_box("a.b.c.d.e"))).is_some());
    });
}

fn bench_set(c: &mut Criterion) {
    let mut nm = NestedMap::new(5);

    c.bench_function("set_key a.b.c.d.e", |b| {
        b.iter(|| {
            nm.set(
                black_box("a.b.c.d.e"),
                &create_item("a.b.c.d.e", b"some value a"),
                Some(SetOptions::new().preserve_history(true)),
            );
        });
    });
}

criterion_group!(benches, bench_get, bench_set);
criterion_main!(benches);
