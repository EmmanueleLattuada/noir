use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
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

fn run_it(env: &mut StreamEnvironment, q: &str, n_tuples: u64, n_iter: usize) {
    match q {
        "it-simple" => it_simple(env, n_tuples, n_iter),
        
        _ => panic!("Invalid bench! {q}"),
    }
}

fn persistency_config_bench(snapshot_frequency_by_item: Option<u64>, snapshot_frequency_by_time: Option<Duration>, isa: bool) -> PersistencyConfig {
    PersistencyConfig { 
        server_addr: String::from(REDIS_BENCH_CONFIGURATION), 
        try_restart: false, 
        clean_on_exit: false, 
        restart_from: None,
        snapshot_frequency_by_item,
        snapshot_frequency_by_time,
        iterations_snapshot_alignment: isa,
    }
}


fn iterations_persistency_bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("iterations_persistency");
    g.sample_size(SAMPLES);
    g.warm_up_time(WARM_UP);
    g.measurement_time(DURATION);


    macro_rules! bench_query {
        ($q:expr, $n_tuples:expr, $n_iter:expr, $name:expr, $p_conf:expr) => {{
            g.bench_with_input(BenchmarkId::new(format!("{}-snap-{}", $q, $name), $n_iter), &$n_iter, |b, _| {
                let mut num_of_snap_avg = 0;
                let mut iter = 0;
                b.iter(|| {
                    let mut config = EnvironmentConfig::local(4);
                    config.add_persistency($p_conf);
                    let mut env = StreamEnvironment::new(config);
                    run_it(&mut env, $q, $n_tuples, $n_iter);
                    env.execute_blocking();
                    let max_snap = noir::persistency::redis_handler::get_max_snapshot_id_and_flushall( String::from(REDIS_BENCH_CONFIGURATION));
                    num_of_snap_avg = ((num_of_snap_avg * iter) + max_snap) / (iter + 1);
                    iter += 1;
                });
                println!("Average number of taken snapshots: {:?}", num_of_snap_avg);
            });
            /*
            g.bench_with_input(BenchmarkId::new(format!("{}-remote-snap-{}", $q, $name), $n_tuples, $n_iter), &$n_tuples, &$n_iter, |b, _| {
                let mut num_of_snap_avg = 0;
                let mut iter = 0;
                b.iter(|| {
                    remote_loopback_deploy(5, 4, Some($p_conf), move |mut env| {
                        run_it(&mut env, $q, $n_tuples, $n_iter);
                    });
                    let max_snap = noir::persistency::redis_handler::get_max_snapshot_id_and_flushall( String::from(REDIS_BENCH_CONFIGURATION));
                    num_of_snap_avg = ((num_of_snap_avg * iter) + max_snap) / (iter + 1);
                    iter += 1;
                });
                println!("Average number of taken snapshots: {:?}", num_of_snap_avg);
            });
            */
        }};
    }

    for n_iter in [10_000, 100_000, 1_000_000] {
        g.throughput(Throughput::Elements(n_iter as u64));
        let n_tuples = 100;
        bench_query!("it-simple", n_tuples, n_iter, "only-TSnap-alignblock", persistency_config_bench(None, None, false));
        bench_query!("it-simple", n_tuples, n_iter, "100Snap-by-item-alignblock", persistency_config_bench(Some((n_iter/(4*100)) as u64), None, false));
        bench_query!("it-simple", n_tuples, n_iter, "only-TSnap-isa", persistency_config_bench(None, None, true));
        bench_query!("it-simple", n_tuples, n_iter, "100Snap-by-item-isa", persistency_config_bench(Some((n_iter/(4*100)) as u64), None, true));
        
    }
    g.finish();

    
}

criterion_group!(benches, iterations_persistency_bench);
criterion_main!(benches);
