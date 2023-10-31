use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkGroup};
use criterion::{BenchmarkId, Throughput};
use noir::{StreamEnvironment, prelude::ParallelIteratorSource, EnvironmentConfig, config::PersistencyConfig};

mod common;
use common::*;

fn it_simple(env: &mut StreamEnvironment, n_tuples: u64, n_iter: usize) {
       let source = ParallelIteratorSource::new(move |id, instances| {
        let chunk_size = (n_tuples + instances - 1) / instances;
        let remaining = n_tuples - n_tuples.min(chunk_size * id);
        let range = remaining.min(chunk_size);
    
        let start = id * chunk_size;
        let stop = id * chunk_size + range;
        start..stop
    });
    let (state, res) = env
        .stream(source)
        .iterate(
            n_iter,
            0,
            |s, state| {
                s.map(move |x| x + *state.get())
            },
            |delta: &mut u64, x| *delta += x,
            |old_state, delta| *old_state += delta,
            |_state| true,
        );
    let _state = state.collect_vec();
    let _res = res.collect_vec();
        
}

fn it_pre_iter(env: &mut StreamEnvironment, n_tuples: u64, n_iter: usize) {
        let source = ParallelIteratorSource::new(move |id, instances| {
        let chunk_size = (n_tuples + instances - 1) / instances;
        let remaining = n_tuples - n_tuples.min(chunk_size * id);
        let range = remaining.min(chunk_size);
    
        let start = id * chunk_size;
        let stop = id * chunk_size + range;
        start..stop
    });
    let (state, res) = env
        .stream(source)
        .filter_map(|n| {
            let sqrt = (n as f64).sqrt().ceil() as u64;
            let mut is_prime = true;
            for j in 2..n.min(sqrt) {
                if n % j == 0 {
                    is_prime = false;
                    break;
                }
            }
            if is_prime {
                None
            } else {
                Some(n)
            }
        })
        .iterate(
            n_iter,
            0,
            |s, state| {
                s.map(move |x| x + *state.get())
            },
            |delta: &mut u64, x| *delta += x,
            |old_state, delta| *old_state += delta,
            |_state| true,
        );
    let _state = state.collect_vec();
    let _res = res.collect_vec();
        
}

fn run_it(env: &mut StreamEnvironment, q: &str, n_tuples: u64, n_iter: usize) {
    match q {
        "it-simple" => it_simple(env, n_tuples, n_iter),
        "it-preiter" => it_pre_iter(env, n_tuples, n_iter),
        
        _ => panic!("Invalid bench! {q}"),
    }
}

fn bench_it(
    g: &mut BenchmarkGroup<WallTime>,
    bench: &str,
    test: &str,
    input_size: u64,
    num_iter: usize,
    persistency_conf: &PersistencyConfig,
) {
    let id = BenchmarkId::new(format!("{}-snap-{}", bench, test), num_iter);
    g.bench_with_input(id, &num_iter, move |b, _| {
        let p = persistency_conf.clone();
        let mut harness = PersistentBenchBuilder::new(
            move || {
                //let mut config = EnvironmentConfig::local(10);
                let mut config = EnvironmentConfig::default();
                config.add_persistency(p.clone());
                StreamEnvironment::new(config)
            },
            |env| {
                run_it(env, bench, input_size, num_iter);
            },
        );

        b.iter_custom(|n| harness.bench(n));
        println!("mean snaps: {:?}; mean redis used memory: {:?}", harness.mean_snap_per_run(), harness.mean_stored_mem_per_run());
    });
}



fn iterations_persistency_bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("iterations_persistency");
    g.sample_size(SAMPLES);
    
    for n_iter in [100, 1000] {
        g.throughput(Throughput::Elements(n_iter as u64));
        let n_tuples = 10_000;
        let pers_conf = persist_none();
        bench_it(&mut g, "it-simple", "only-TSnap-alignblock", n_tuples, n_iter, &pers_conf);
        let mut pers_conf_isa = pers_conf.clone();
        pers_conf_isa.iterations_snapshot_alignment = true;
        bench_it(&mut g, "it-simple", "only-TSnap-isa", n_tuples, n_iter, &pers_conf_isa);

        for interval in [10, 100, 1000].map(Duration::from_millis) {            
            //let test = format!("{interval:?}");
            let pers_conf = persist_interval(interval);
            bench_it(&mut g, "it-simple", &format!("{interval:?}-alignblock"), n_tuples, n_iter, &pers_conf);
            let mut pers_conf_isa = pers_conf.clone();
            pers_conf_isa.iterations_snapshot_alignment = true;
            bench_it(&mut g, "it-simple", &format!("{interval:?}-isa"), n_tuples, n_iter, &pers_conf_isa);
        }
    }

    for n_iter in [10, 100] {
        g.throughput(Throughput::Elements(n_iter as u64));
        let n_tuples = 1_000_000;
        let pers_conf = persist_none();
        bench_it(&mut g, "it-simple", "only-TSnap-alignblock", n_tuples, n_iter, &pers_conf);
        let mut pers_conf_isa = pers_conf.clone();
        pers_conf_isa.iterations_snapshot_alignment = true;
        bench_it(&mut g, "it-simple", "only-TSnap-isa", n_tuples, n_iter, &pers_conf_isa);

        for interval in [10, 100, 1000].map(Duration::from_millis) {            
            //let test = format!("{interval:?}");
            let pers_conf = persist_interval(interval);
            bench_it(&mut g, "it-simple", &format!("{interval:?}-alignblock"), n_tuples, n_iter, &pers_conf);
            let mut pers_conf_isa = pers_conf.clone();
            pers_conf_isa.iterations_snapshot_alignment = true;
            bench_it(&mut g, "it-simple", &format!("{interval:?}-isa"), n_tuples, n_iter, &pers_conf_isa);
        }
    }

    for n_iter in [3, 5, 10, 100] {
        g.throughput(Throughput::Elements(n_iter as u64));
        let n_tuples = 1_000_000;
        let pers_conf = persist_none();
        bench_it(&mut g, "it-preiter", "only-TSnap-alignblock", n_tuples, n_iter, &pers_conf);
        let mut pers_conf_isa = pers_conf.clone();
        pers_conf_isa.iterations_snapshot_alignment = true;
        bench_it(&mut g, "it-preiter", "only-TSnap-isa", n_tuples, n_iter, &pers_conf_isa);

        for interval in [10, 100, 1000].map(Duration::from_millis) {            
            //let test = format!("{interval:?}");
            let pers_conf = persist_interval(interval);
            bench_it(&mut g, "it-preiter", &format!("{interval:?}-alignblock"), n_tuples, n_iter, &pers_conf);
            let mut pers_conf_isa = pers_conf.clone();
            pers_conf_isa.iterations_snapshot_alignment = true;
            bench_it(&mut g, "it-preiter", &format!("{interval:?}-isa"), n_tuples, n_iter, &pers_conf_isa);
        }
    }

}

criterion_group!(benches, iterations_persistency_bench);
criterion_main!(benches);