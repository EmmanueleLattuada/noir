use std::io::{BufWriter, Write};
//use std::os::unix::prelude::MetadataExt;
use std::path::Path;
use std::thread::available_parallelism;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion::{BenchmarkId, Throughput};
use fake::Fake;
use noir::config::PersistencyConfig;
use noir::prelude::{FileSource, Source};
use rand::prelude::StdRng;
use rand::SeedableRng;

use noir::{BatchMode, EnvironmentConfig};
use noir::StreamEnvironment;

mod common;
use common::*;

// #[global_allocator]
// static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

fn persistency_bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("persistency");
    g.sample_size(SAMPLES);
    g.warm_up_time(WARM_UP);
    g.measurement_time(DURATION);

    for lines in [0, 100, 10_000, 1_000_000] {
        let file = make_file(lines as usize);
        let file_size = file.as_file().metadata().unwrap().len(); // .size()
        g.throughput(Throughput::Bytes(file_size));

        g.bench_with_input(
            BenchmarkId::new("persistency-wordcount", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut env = StreamEnvironment::default();
                    wc_fold(&mut env, path, None);
                    env.execute();
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("persistency-wordcount-no-snapshot", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut config = EnvironmentConfig::local(
                        available_parallelism().map(|q| q.get()).unwrap_or(1) as u64,
                    );
                    config.add_persistency(PersistencyConfig { 
                        server_addr: String::from("redis://127.0.0.1"), 
                        try_restart: false, 
                        clean_on_exit: true, 
                        restart_from: None, 
                    });
                    let mut env = StreamEnvironment::new(config);
                    wc_fold(&mut env, path, None);
                    env.execute();
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("persistency-wordcount-snapshot-every-100", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut config = EnvironmentConfig::local(
                        available_parallelism().map(|q| q.get()).unwrap_or(1) as u64,
                    );
                    config.add_persistency(PersistencyConfig { 
                        server_addr: String::from("redis://127.0.0.1"), 
                        try_restart: false, 
                        clean_on_exit: true, 
                        restart_from: None, 
                    });
                    let mut env = StreamEnvironment::new(config);
                    wc_fold(&mut env, path, Some(100));
                    env.execute();
                })
            },
        );

        
    }
    g.finish();
}

fn wc_fold(env: &mut StreamEnvironment, path: &Path, snapshot_freq: Option<u64>) {
    let mut source = FileSource::new(path);
    if snapshot_freq.is_some() {
        source.set_snapshot_frequency_by_item(snapshot_freq.unwrap());
    }
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


criterion_group!(benches, persistency_bench);
criterion_main!(benches);
