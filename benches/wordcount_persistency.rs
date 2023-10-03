use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion::{BenchmarkId, Throughput};
use fake::Fake;
use kstring::KString;
use noir::config::PersistencyConfig;
use noir::prelude::FileSource;
use once_cell::sync::Lazy;
use rand::prelude::StdRng;
use rand::SeedableRng;

use noir::{BatchMode, EnvironmentConfig};
use noir::StreamEnvironment;

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
    let mut g = c.benchmark_group("wordcount_persistency");
    g.sample_size(SAMPLES);
    g.warm_up_time(WARM_UP);
    g.measurement_time(DURATION);

    macro_rules! bench_wc {
        ($q:expr, $n:expr, $p:ident) => {{
            g.bench_with_input(BenchmarkId::new(format!("{}-snap-t", $q), $n), &$n, |b, _| {
                b.iter(|| {
                    let mut config = EnvironmentConfig::local(4);
                    config.add_persistency(PersistencyConfig { 
                        server_addr: String::from("redis://127.0.0.1"), 
                        try_restart: false, 
                        clean_on_exit: true, 
                        restart_from: None, 
                        snapshot_frequency_by_item: None,
                        snapshot_frequency_by_time: None,
                    });
                    let mut env = StreamEnvironment::new(config);
                    run_wc(&mut env, $q, $p);
                    env.execute_blocking();
                })
            });
            g.bench_with_input(BenchmarkId::new(format!("{}-snap-100", $q), $n), &$n, |b, _| {
                b.iter(|| {
                    let mut config = EnvironmentConfig::local(4);
                    config.add_persistency(PersistencyConfig { 
                        server_addr: String::from("redis://127.0.0.1"), 
                        try_restart: false, 
                        clean_on_exit: true, 
                        restart_from: None, 
                        snapshot_frequency_by_item: Some(($n/(4*100)) as u64),  // 4 replicas
                        snapshot_frequency_by_time: None,
                        //snapshot_frequency_by_item: None,
                        //snapshot_frequency_by_time: Some(std::time::Duration::from_millis($n/1000)),

                    });
                    let mut env = StreamEnvironment::new(config);
                    run_wc(&mut env, $q, $p);
                    env.execute_blocking();
                })
            });            
            g.bench_with_input(
                BenchmarkId::new(format!("{}-remote-snap-t", $q), $n),
                &$n,
                |b, _| {
                    let pathb = Arc::new($p.to_path_buf());
                    let pers_conf = PersistencyConfig { 
                        server_addr: String::from("redis://127.0.0.1"), 
                        try_restart: false, 
                        clean_on_exit: true, 
                        restart_from: None, 
                        snapshot_frequency_by_item: None,
                        snapshot_frequency_by_time: None,

                    };
                    b.iter(|| {
                        let p = pathb.clone();
                        remote_loopback_deploy(5, 4, Some(pers_conf.clone()), move |mut env| {
                            run_wc(&mut env, $q, &p);
                        });
                    })
                },
            );
            g.bench_with_input(
                BenchmarkId::new(format!("{}-remote-snap-100", $q), $n),
                &$n,
                |b, _| {
                    let pathb = Arc::new($p.to_path_buf());
                    let pers_conf = PersistencyConfig { 
                        server_addr: String::from("redis://127.0.0.1"), 
                        try_restart: false, 
                        clean_on_exit: true, 
                        restart_from: None, 
                        snapshot_frequency_by_item: Some(($n/(5*4*100)) as u64),    // 5 replicas * 4 hosts
                        snapshot_frequency_by_time: None,
                        //snapshot_frequency_by_item: None,
                        //snapshot_frequency_by_time: Some(std::time::Duration::from_millis($n/1000)),

                    };
                    b.iter(|| {
                        let p = pathb.clone();
                        remote_loopback_deploy(5, 4, Some(pers_conf.clone()), move |mut env| {
                            run_wc(&mut env, $q, &p);
                        });
                    })
                },
            );
        }};
    }

    for lines in [10_000, 100_000, 1_000_000, 10_000_000] {
        let file = make_file(lines as usize);
        let file_size = file.as_file().metadata().unwrap().len();
        let file_path = file.path();
        g.throughput(Throughput::Bytes(file_size));

        bench_wc!("wc-fold", lines, file_path);
        bench_wc!("wc-fold-assoc", lines, file_path);
        //bench_wc!("wc-count-assoc", lines, file_path):
        //bench_wc!("wc-reduce", lines, file_path);
        //bench_wc!("wc-reduce-assoc", lines, file_path);
        bench_wc!("wc-fast", lines, file_path);
        //bench_wc!("wc-fast-kstring", lines, file_path);
    }
    g.finish();

    
}

criterion_group!(benches, wordcount_persistency_bench);
criterion_main!(benches);
