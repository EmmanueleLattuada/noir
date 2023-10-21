use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::io::{BufWriter, Write};
use std::path::Path;
//use std::sync::Arc;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion};
use criterion::{BenchmarkId, Throughput};
use fake::Fake;
use kstring::KString;
use noir::config::PersistencyConfig;
use noir::prelude::FileSource;
use once_cell::sync::Lazy;
use rand::prelude::StdRng;
use rand::SeedableRng;

use noir::StreamEnvironment;
use noir::{BatchMode, EnvironmentConfig};

mod common;
use common::*;
use regex::Regex;
use wyhash::WyHash;

static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[a-zA-Z]+").unwrap());

fn make_file(lines: usize) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let seed = b"By imDema, edomora97 and mark03.".to_owned();
    let r = &mut StdRng::from_seed(seed);
    let mut w = BufWriter::new(&mut file);

    for _ in 0..lines {
        use fake::faker::lorem::en::*;
        let line = Sentence(10..100).fake_with_rng::<String, _>(r);
        w.write_all(line.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
    }
    drop(w);
    file
}

fn wc_fold(env: &mut StreamEnvironment, path: &Path) {
    let source = FileSource::new(path);
    let result = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            line.split_whitespace()
                .map(|s| s.to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by(|word: &String| word.clone())
        .fold(0u64, |count, _word| *count += 1)
        .collect_vec();
    std::hint::black_box(result);
}
fn wc_fold_assoc(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by_fold(
            |w| w.clone(),
            0,
            |count, _word| *count += 1,
            |count1, count2| *count1 += count2,
        )
        .unkey()
        .collect_vec();
    std::hint::black_box(result);
}
fn wc_count_assoc(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by_count(|w| w.clone())
        .unkey()
        .collect_vec();
    std::hint::black_box(result);
}
fn wc_reduce(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by(|word| word.clone())
        .map(|(_, word)| (word, 1))
        .reduce(|(_w1, c1), (_w2, c2)| *c1 += c2)
        .collect_vec();
    std::hint::black_box(result);
}
fn wc_reduce_assoc(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .map(|word| (word, 1))
        .group_by_reduce(|w| w.clone(), |(_w1, c1), (_w, c2)| *c1 += c2)
        .unkey()
        .collect_vec();
    std::hint::black_box(result);
}
fn wc_fast(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .fold_assoc(
            HashMap::<KString, u64, BuildHasherDefault<WyHash>>::default(),
            |acc, line| {
                let mut word = String::with_capacity(4);
                for c in line.chars() {
                    if !c.is_whitespace() {
                        word.push(c.to_ascii_lowercase());
                    } else if !word.is_empty() {
                        let key = KString::from_ref(word.as_str());
                        *acc.entry(key).or_default() += 1;
                        word.truncate(0);
                    }
                }
            },
            |a, mut b| {
                for (k, v) in b.drain() {
                    *a.entry(k).or_default() += v;
                }
            },
        )
        .collect_vec();
    std::hint::black_box(result);
}
fn wc_fast_kstring(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .fold_assoc(
            HashMap::<String, u64, BuildHasherDefault<WyHash>>::default(),
            |acc, line| {
                let mut word = String::with_capacity(4);
                for c in line.chars() {
                    if !c.is_whitespace() {
                        word.push(c.to_ascii_lowercase());
                    } else if !word.is_empty() {
                        let key = std::mem::replace(&mut word, String::with_capacity(4));
                        *acc.entry(key).or_default() += 1;
                    }
                }
            },
            |a, mut b| {
                for (k, v) in b.drain() {
                    *a.entry(k).or_default() += v;
                }
            },
        )
        .collect_vec();
    std::hint::black_box(result);
}

fn bench_wc(
    g: &mut BenchmarkGroup<WallTime>,
    bench: &str,
    test: &str,
    lines: usize,
    file_path: &Path,
    persistency_conf: &PersistencyConfig,
) {
    let id = BenchmarkId::new(format!("{}-snap-{}", bench, test), lines);
    g.bench_with_input(id, &lines, move |b, _| {
        let p = persistency_conf.clone();
        let mut harness = PersistentBenchBuilder::new(
            move || {
                let mut config = EnvironmentConfig::default();
                config.add_persistency(p.clone());
                StreamEnvironment::new(config)
            },
            |env| {
                run_wc(env, bench, file_path);
            },
        );

        b.iter_custom(|n| harness.bench(n));
        println!("mean snaps: {:?}", harness.mean_snap_per_run());
    });
}

// fn bench_wc_remote(g: &mut BenchmarkGroup<WallTime>, bench: &str, test: &str, lines: usize, file_path: &Path, persistency_conf: PersistencyConfig) {
//     let id = BenchmarkId::new(format!("{}-snap-{}", bench, test), lines);
//     g.bench_with_input(id, &lines, move |b, _| {
//         let p = persistency_conf.clone();
//         let mut harness = PersistentBenchBuilder::new(
//             move || {
//                 let mut config = EnvironmentConfig::default();
//                 config.add_persistency(p.clone());
//                 StreamEnvironment::new(config)
//             },
//             |env| {
//                 run_wc(env, bench, file_path);
//             },
//         );

//         b.iter_custom(|n| harness.bench(n));
//         println!(
//             "Average number of taken snapshots: {:?}",
//             harness.mean_snap_per_run()
//         );
//     });
// }

fn run_wc(env: &mut StreamEnvironment, q: &str, path: &Path) {
    match q {
        "wc-fold" => wc_fold(env, path),
        "wc-fold-assoc" => wc_fold_assoc(env, path),
        "wc-count-assoc" => wc_count_assoc(env, path),
        "wc-reduce" => wc_reduce(env, path),
        "wc-reduce-assoc" => wc_reduce_assoc(env, path),
        "wc-fast" => wc_fast(env, path),
        "wc-fast-kstring" => wc_fast_kstring(env, path),

        _ => panic!("Invalid wc bench! {q}"),
    }
}

fn wordcount_persistency_bench(c: &mut Criterion) {
    use tracing_subscriber::layer::SubscriberExt;

    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_tracy::TracyLayer::new()),
    ).expect("set up the subscriber");


    let mut g = c.benchmark_group("wordcount_persistency");
    g.sample_size(SAMPLES);

    for lines in [100_000, 1_000_000] {
        let file = make_file(lines as usize);
        let file_size = file.as_file().metadata().unwrap().len();
        let file_path = file.path();
        g.throughput(Throughput::Bytes(file_size));
        for interval in [10, 100, 1000].map(Duration::from_millis) {
            let test = format!("{interval:?}");
            let conf = persist_interval(interval);
            bench_wc(&mut g, "wc-fold-assoc", &test, lines, file_path, &conf);
            bench_wc(&mut g, "wc-reduce", &test, lines, file_path, &conf);
            bench_wc(&mut g, "wc-fast", &test, lines, file_path, &conf);
        }

        // bench_wc_remote(&mut g, "wc-fold-assoc", "10ms-remote", lines, file_path, persist_interval(Duration::from_millis(10)));
        // bench_wc_remote(&mut g, "wc-reduce", "10ms-remote", lines, file_path, persist_interval(Duration::from_millis(10)));
        // bench_wc_remote(&mut g, "wc-fast", "10ms-remote", lines, file_path, persist_interval(Duration::from_millis(10)));
    }
}

criterion_group!(benches, wordcount_persistency_bench);
criterion_main!(benches);
