use itertools::Itertools;

use noir::{operator::source::IteratorSource, StreamEnvironment, prelude::ParallelIteratorSource};

use super::utils::TestHelper;
use serial_test::serial;

fn check_result(n: u64, n_iter: usize, state: Option<Vec<u64>>, items: Option<Vec<u64>>) {
    if let Some(res) = state {
        assert_eq!(res.len(), 1);
        let res = res.into_iter().next().unwrap();

        let mut expected = 0;
        let mut sum: u64 = (0..n).sum();
        for _ in 0..n_iter {
            sum += n * expected;
            expected += sum;
        }

        assert_eq!(res, expected);
    }
    if let Some(mut res) = items {
        res.sort_unstable();

        let mut state = 0;
        let mut items = (0..n).collect_vec();
        for _ in 0..n_iter {
            for item in items.iter_mut() {
                *item += state;
            }
            state += items.iter().sum::<u64>();
        }
        assert_eq!(res, items);
    }
}

#[test]
#[serial]
fn test_iterate_no_blocks_in_between() {
    let body = |mut env: StreamEnvironment| {
        let n = 5u64;
        let n_iter = 5;

        let source = IteratorSource::new(0..n);
        let (state, res) = env
            .stream(source)
            .shuffle()
            // the body of this iteration does not split the block (it's just a map)
            .iterate(
                n_iter,
                0u64,
                |s, state| s.map(move |x| x + *state.get()),
                |delta: &mut u64, x| *delta += x,
                |old_state, delta| *old_state += delta,
                |_state| true,
            );
        let state = state.collect_vec();
        let res = res.collect_vec();
        env.execute_blocking();

        check_result(n, n_iter, state.get(), res.get());
    };

    TestHelper::local_remote_env(body);

    let snap_freq = Some(1);

    let execution_list = vec![
        // Complete execution
        TestHelper::persistency_config_test(false, false, None, snap_freq),
        // Restart from snapshot 1
        TestHelper::persistency_config_test(true, false, Some(1), snap_freq),
        // Restart from snapshot 2
        TestHelper::persistency_config_test(true, false, Some(2), snap_freq),
        // Restart from snapshot 5
        TestHelper::persistency_config_test(true, false, Some(5), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}

#[test]
#[serial]
fn test_iterate_side_input() {
    let body = |mut env: StreamEnvironment| {
        let n = 5u64;
        let n_iter = 5;

        let source = IteratorSource::new(0..n);
        let side = env.stream(IteratorSource::new(0..n));
        let (state, res) = env.stream(source).map(|x| (x, x)).shuffle().iterate(
            n_iter,
            0u64,
            |s, state| {
                s.join(side, |(x, _)| *x, |x| *x)
                    .map(move |(_key, ((x, y), _x))| (x, y + *state.get()))
                    .drop_key()
            },
            |delta: &mut u64, (_x, y)| *delta += y,
            |old_state, delta| *old_state += delta,
            |_state| true,
        );
        let state = state.collect_vec();
        let res = res.map(|(_, y)| y).collect_vec();
        env.execute_blocking();

        check_result(n, n_iter, state.get(), res.get());
    };

    TestHelper::local_remote_env(body);

    let snap_freq = Some(1);

    let execution_list = vec![
        // Complete execution
        TestHelper::persistency_config_test(false, false, None, snap_freq),
        // Restart from snapshot 1, during iterations
        TestHelper::persistency_config_test(true, false, Some(1), snap_freq),
        // Restart from snapshot 2, this should be a Terminate snapshot
        TestHelper::persistency_config_test(true, false, Some(2), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list); 
}

#[test]
#[serial]
fn test_iterate_with_shuffle() {
    let body = |mut env: StreamEnvironment| {
        let n = 5u64;
        let n_iter = 2;

        let source = IteratorSource::new(0..n);
        let (state, res) = env
            .stream(source)
            .shuffle()
            // the body of this iteration will split the block (there is a shuffle)
            .iterate(
                n_iter,
                0u64,
                |s, state| {
                    s.shuffle().map(move |x| {
                        let state = *state.get();
                        x + state
                    })
                },
                |delta: &mut u64, x| *delta += x,
                |old_state, delta| *old_state += delta,
                |state| {
                    println!("XX: End of iteration: state is {state}");
                    true
                },
            );
        let state = state.collect_vec();
        let res = res.collect_vec();
        env.execute_blocking();

        check_result(n, n_iter, state.get(), res.get());
    };

    TestHelper::local_remote_env(body);

    let snap_freq = Some(1);

    let execution_list = vec![
        // Complete execution
        TestHelper::persistency_config_test(false, false, None, snap_freq),
        // Restart from snapshot 1
        TestHelper::persistency_config_test(true, false, Some(1), snap_freq),
        // Restart from snapshot 2
        TestHelper::persistency_config_test(true, false, Some(2), snap_freq),
        // Restart from snapshot 5
        TestHelper::persistency_config_test(true, false, Some(5), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}

#[test]
#[serial]
fn test_iterate_primes() {
    let body = |mut env: StreamEnvironment| {
        let n = 1000u64;
        let n_iter = (n as f64).sqrt().ceil() as usize;

        let source = IteratorSource::new(2..n);
        let (state, primes) = env.stream(source).shuffle().iterate(
            n_iter,
            2,
            |s, state| s.filter(move |x| x == state.get() || x % state.get() != 0),
            |_delta: &mut u64, _x| {},
            |_old_state, _delta| {},
            |state| {
                *state += 1;
                true
            },
        );
        let state = state.collect_vec();
        let primes = primes.collect_vec();
        env.execute_blocking();

        if let Some(state) = state.get() {
            assert_eq!(state.len(), 1);
        }
        if let Some(mut primes) = primes.get() {
            primes.sort_unstable();
            let mut expected = vec![];
            for i in 2..n {
                let sqrt = (n as f64).sqrt().ceil() as u64;
                let mut is_prime = true;
                for j in 2..i.min(sqrt) {
                    if i % j == 0 {
                        is_prime = false;
                        break;
                    }
                }
                if is_prime {
                    expected.push(i);
                }
            }
            assert_eq!(primes, expected);
        }
    };

    TestHelper::local_remote_env(body);

    let snap_freq = Some(20);

    let execution_list = vec![
        // Complete execution
        TestHelper::persistency_config_test(false, false, None, snap_freq),
        // Restart from snapshot 1
        TestHelper::persistency_config_test(true, false, Some(1), snap_freq),
        // Restart from snapshot 2
        TestHelper::persistency_config_test(true, false, Some(2), snap_freq),
        // Restart from snapshot 5
        TestHelper::persistency_config_test(true, false, Some(5), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}



#[test]
#[serial]
fn test_iterate_snapshot_not_alligned() {
    let body = |mut env: StreamEnvironment| {
        let n = 5u64;
        let n_iter = 5;

        let source = ParallelIteratorSource::new(move |id, instances| {
            let chunk_size = (n + instances - 1) / instances;
            let remaining = n - n.min(chunk_size * id);
            let range = remaining.min(chunk_size);
        
            let start = id * chunk_size;
            let stop = id * chunk_size + range;
            start..stop
        });
        let (state, res) = env
            .stream(source)
            .iterate(
                n_iter,
                0u64,
                |s, state| s.map(move |x| x + *state.get()),
                |delta: &mut u64, x| *delta += x,
                |old_state, delta| *old_state += delta,
                |_state| true,
            );
        let state = state.collect_vec();
        let res = res.collect_vec();
        env.execute_blocking();

        check_result(n, n_iter, state.get(), res.get());
    };

    TestHelper::local_remote_env(body);

    let snap_freq = Some(1);

    let execution_list = vec![
        // Complete execution
        TestHelper::persistency_config_test(false, false, None, snap_freq),
        // Restart from snapshot 1
        TestHelper::persistency_config_test(true, false, Some(1), snap_freq),
        // Restart from snapshot 2
        TestHelper::persistency_config_test(true, false, Some(2), snap_freq),
        // Restart from snapshot 5
        TestHelper::persistency_config_test(true, false, Some(5), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}
