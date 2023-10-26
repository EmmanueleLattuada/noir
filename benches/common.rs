#![allow(unused)]

use criterion::{black_box, Bencher};
use noir::config::{ExecutionRuntime, RemoteHostConfig, RemoteRuntimeConfig, PersistencyConfig};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use noir::*;

pub const SAMPLES: usize = 50;
pub const WARM_UP: Duration = Duration::from_secs(10);
pub const DURATION: Duration = Duration::from_secs(30);

static NONCE: AtomicU16 = AtomicU16::new(1);
const PORT_BASE: u16 = 9090;

pub const REDIS_BENCH_CONFIGURATION: &str ="redis://127.0.0.1";

pub fn remote_loopback_deploy(
    num_hosts: CoordUInt,
    cores_per_host: CoordUInt,
    persistency_config: Option<PersistencyConfig>,
    body: impl Fn(&mut StreamEnvironment) + Send + Sync + 'static,
) {
    let mut hosts = vec![];
    for host_id in 0..num_hosts {
        let test_id = NONCE.fetch_add(1, Ordering::SeqCst);
        let [hi, lo] = test_id.to_be_bytes();
        let address = format!("127.{hi}.{lo}.{host_id}");
        hosts.push(RemoteHostConfig {
            address,
            base_port: PORT_BASE,
            num_cores: cores_per_host,
            ssh: Default::default(),
            perf_path: None,
        });
    }

    let runtime = ExecutionRuntime::Remote(RemoteRuntimeConfig {
        hosts,
        tracing_dir: None,
        cleanup_executable: false,
    });

    let mut join_handles = vec![];
    let body = Arc::new(body);
    for host_id in 0..num_hosts {
        let config = EnvironmentConfig {
            runtime: runtime.clone(),
            host_id: Some(host_id),
            skip_single_remote_check: true,
            persistency_configuration: persistency_config.clone(),
        };
        let body = body.clone();
        join_handles.push(
            std::thread::Builder::new()
                .name(format!("lohost-{host_id:02}"))
                .spawn(move || {
                    let mut env = StreamEnvironment::new(config);
                    body(&mut env);
                    env.execute_blocking();
                })
                .unwrap(),
        )
    }
    for (host_id, handle) in join_handles.into_iter().enumerate() {
        handle
            .join()
            .unwrap_or_else(|e| panic!("Remote worker for host {host_id} crashed: {e:?}"));
    }
}

pub struct PersistentBenchBuilder<F, G, R>
where
    F: Fn() -> StreamEnvironment,
    G: Fn(&mut StreamEnvironment) -> R,
{
    make_env: F,
    make_network: G,
    _result: PhantomData<R>,
    snap_count: u64,
    run_count: u64,
}

impl<F, G, R> PersistentBenchBuilder<F, G, R>
where
    F: Fn() -> StreamEnvironment,
    G: Fn(&mut StreamEnvironment) -> R,
{
    pub fn new(make_env: F, make_network: G) -> Self {
        Self {
            make_env,
            make_network,
            _result: Default::default(),
            snap_count: 0,
            run_count: 0,
        }
    }

    pub fn bench(&mut self, n: u64) -> Duration {
        let mut time = Duration::default();
        for _ in 0..n {
            let mut env = (self.make_env)();
            let _result = (self.make_network)(&mut env);
            let start = Instant::now();
            env.execute_blocking();
            time += start.elapsed();
            black_box(_result);
            let max_snap = noir::persistency::redis_handler::get_max_snapshot_id_and_flushall( String::from(REDIS_BENCH_CONFIGURATION));
            self.snap_count += max_snap;
            self.run_count += 1;
        }
        time
    }

    pub fn mean_snap_per_run(&self) -> f32 {
        self.snap_count as f32 / self.run_count as f32
    }
}

pub fn persist_interval(
    interval: Duration,
) -> PersistencyConfig {
    PersistencyConfig {
        server_addr: String::from(REDIS_BENCH_CONFIGURATION),
        try_restart: false,
        clean_on_exit: false,
        restart_from: None,
        snapshot_frequency_by_item: None,
        snapshot_frequency_by_time: Some(interval),
        iterations_snapshot_alignment: false,
    }
}

pub fn persist_count(
    count: u64,
) -> PersistencyConfig {
    PersistencyConfig {
        server_addr: String::from(REDIS_BENCH_CONFIGURATION),
        try_restart: false,
        clean_on_exit: false,
        restart_from: None,
        snapshot_frequency_by_item: Some(count),
        snapshot_frequency_by_time: None,
        iterations_snapshot_alignment: false,
    }
}

pub fn persist_none() -> PersistencyConfig {
    PersistencyConfig {
        server_addr: String::from(REDIS_BENCH_CONFIGURATION),
        try_restart: false,
        clean_on_exit: false,
        restart_from: None,
        snapshot_frequency_by_item: None,
        snapshot_frequency_by_time: None,
        iterations_snapshot_alignment: false,
    }
}
