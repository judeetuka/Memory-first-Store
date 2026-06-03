use std::collections::HashMap;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

const SIZE: usize = 10_000;

fn compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");

    #[derive(Clone, Copy)]
    struct RandomKeys {
        state: usize,
    }

    impl RandomKeys {
        fn new() -> Self {
            RandomKeys { state: 0 }
        }
    }

    impl Iterator for RandomKeys {
        type Item = usize;
        fn next(&mut self) -> Option<usize> {
            // Add 1 then multiply by some 32 bit prime.
            self.state = self.state.wrapping_add(1).wrapping_mul(3_787_392_781);
            Some(self.state)
        }
    }

    group.bench_function("papaya", |b| {
        let m = papaya::HashMap::<usize, usize>::builder()
            .collector(seize::Collector::new())
            .build();

        for i in RandomKeys::new().take(SIZE) {
            m.pin().insert(i, i);
        }

        b.iter(|| {
            for i in RandomKeys::new().take(SIZE) {
                let got = m.pin().get(&i).copied();
                black_box(got);
                assert_eq!(got, Some(i));
            }
        });
    });

    group.bench_function("std", |b| {
        let mut m = HashMap::<usize, usize>::default();
        for i in RandomKeys::new().take(SIZE) {
            m.insert(i, i);
        }

        b.iter(|| {
            for i in RandomKeys::new().take(SIZE) {
                let got = m.get(&i).copied();
                black_box(got);
                assert_eq!(got, Some(i));
            }
        });
    });

    group.bench_function("dashmap", |b| {
        let m = dashmap::DashMap::<usize, usize>::default();
        for i in RandomKeys::new().take(SIZE) {
            m.insert(i, i);
        }

        b.iter(|| {
            for i in RandomKeys::new().take(SIZE) {
                let got = *m.get(&i).unwrap();
                black_box(got);
                assert_eq!(got, i);
            }
        });
    });

    group.finish();
}

criterion_group!(benches, compare);
criterion_main!(benches);
