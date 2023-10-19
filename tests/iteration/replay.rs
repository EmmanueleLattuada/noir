use noir::StreamEnvironment;
use noir::operator::sink::StreamOutput;
use noir::operator::source::IteratorSource;
use noir::prelude::ParallelIteratorSource;
use serial_test::serial;
use super::utils::TestHelper;

#[test]
#[serial]
fn test_replay_no_blocks_in_between() {
    let body = |mut env: StreamEnvironment| {
        let n = 5u64;
        let n_iter = 5;

        let source = IteratorSource::new(0..n);
        let res = env
            .stream(source)
            .shuffle()
            .map(|x| x)
            // the body of this iteration does not split the block (it's just a map)
            .replay(
                n_iter,
                1,
                |s, state| s.map(move |x| x * *state.get()),
                |delta: &mut u64, x| *delta += x,
                |old_state, delta| *old_state += delta,
                |state| {
                    *state -= 1;
                    true
                },
            )
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            let res = res.into_iter().next().unwrap();

            let mut state = 1;
            for _ in 0..n_iter {
                let s: u64 = (0..n).map(|x| x * state).sum();
                state = state + s - 1;
            }

            assert_eq!(res, state);
        }
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
        // Restart from snapshot  5, from the last iteration
        TestHelper::persistency_config_test(true, false, Some(5), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}

#[test]
#[serial]
fn test_replay_with_shuffle() {
    let body = |mut env: StreamEnvironment| {
        let n = 20u64;
        let n_iter = 5;

        let source = IteratorSource::new(0..n);
        let res = env
            .stream(source)
            .shuffle()
            .map(|x| x)
            // the body of this iteration will split the block (there is a shuffle)
            .replay(
                n_iter,
                1,
                |s, state| s.shuffle().map(move |x| x * *state.get()),
                |delta: &mut u64, x| *delta += x,
                |old_state, delta| *old_state += delta,
                |state| {
                    *state -= 1;
                    true
                },
            )
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            let res = res.into_iter().next().unwrap();

            let mut state = 1;
            for _ in 0..n_iter {
                let s: u64 = (0..n).map(|x| x * state).sum();
                state = state + s - 1;
            }

            assert_eq!(res, state);
        }
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
        // Restart from snapshot 20
        TestHelper::persistency_config_test(true, false, Some(20), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}

fn check_nested_result(res: StreamOutput<Vec<u64>>) {
    if let Some(res) = res.get() {
        assert_eq!(res.len(), 1);
        let res = res.into_iter().next().unwrap();

        let mut expected = 0u64;
        for _ in 0..2 {
            for _ in 0..2 {
                let mut inner = 0;
                for i in 0..10 {
                    inner += i;
                }
                expected += inner;
            }
        }

        assert_eq!(res, expected);
    }
}

#[test]
#[serial]
fn test_replay_nested_no_shuffle() {
    let body = |mut env: StreamEnvironment| {
        let source = IteratorSource::new(0..10u64);
        let stream = env.stream(source).shuffle().replay(
            2,
            0,
            |s, _| {
                s.replay(
                    2,
                    0,
                    |s, _| s.reduce(|x, y| x + y),
                    |update: &mut u64, ele| *update += ele,
                    |state, update| *state += update,
                    |&mut _state| true,
                )
            },
            |update: &mut u64, ele| *update += ele,
            |state, update| *state += update,
            |&mut _state| true,
        );
        let res = stream.collect_vec();
        env.execute_blocking();
        check_nested_result(res);
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
        // Restart from snapshot 10
        TestHelper::persistency_config_test(true, false, Some(10), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}

#[test]
#[serial]
fn test_replay_nested_shuffle_inner() {
    let body = |mut env: StreamEnvironment| {
        let source = IteratorSource::new(0..10u64);
        let stream = env.stream(source).shuffle().replay(
            2,
            0,
            |s, _| {
                s.replay(
                    2,
                    0,
                    |s, _| s.shuffle().reduce(|x, y| x + y),
                    |update: &mut u64, ele| *update += ele,
                    |state, update| *state += update,
                    |&mut _state| true,
                )
            },
            |update: &mut u64, ele| *update += ele,
            |state, update| *state += update,
            |&mut _state| true,
        );
        let res = stream.collect_vec();
        env.execute_blocking();
        check_nested_result(res);
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
        // Restart from snapshot 10
        TestHelper::persistency_config_test(true, false, Some(10), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}

#[test]
#[serial]
fn test_replay_nested_shuffle_outer() {
    let body = |mut env: StreamEnvironment| {
        let source = IteratorSource::new(0..10u64);
        let stream = env.stream(source).shuffle().replay(
            2,
            0,
            |s, _| {
                s.shuffle().replay(
                    2,
                    0,
                    |s, _| s.reduce(|x, y| x + y),
                    |update: &mut u64, ele| *update += ele,
                    |state, update| *state += update,
                    |&mut _state| true,
                )
            },
            |update: &mut u64, ele| *update += ele,
            |state, update| *state += update,
            |&mut _state| true,
        );
        let res = stream.collect_vec();
        env.execute_blocking();
        check_nested_result(res);
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
        // Restart from snapshot 10
        TestHelper::persistency_config_test(true, false, Some(10), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}

#[test]
#[serial]
fn test_replay_nested_shuffle_both() { 
    let body = |mut env: StreamEnvironment| {
        let source = IteratorSource::new(0..10u64);
        let stream = env.stream(source).shuffle().replay(
            2,
            0,
            |s, _| {
                s.shuffle().replay(
                    2,
                    0,
                    |s, _| s.shuffle().reduce(|x, y| x + y),
                    |update: &mut u64, ele| *update += ele,
                    |state, update| *state += update,
                    |&mut _state| true,
                )
            },
            |update: &mut u64, ele| *update += ele,
            |state, update| *state += update,
            |&mut _state| true,
        );
        let res = stream.collect_vec();
        env.execute_blocking();
        check_nested_result(res);
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
        // Restart from snapshot 10, from the last iteration
        TestHelper::persistency_config_test(true, false, Some(10), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
    
}



#[test]
#[serial]
fn test_replay_snapshot_not_alligned() {
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
        let res = env
            .stream(source)
            .map(|x| x)
            // the body of this iteration does not split the block (it's just a map)
            .replay(
                n_iter,
                1,
                |s, state| s.map(move |x| x * *state.get()),
                |delta: &mut u64, x| *delta += x,
                |old_state, delta| *old_state += delta,
                |state| {
                    *state -= 1;
                    true
                },
            )
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            let res = res.into_iter().next().unwrap();

            let mut state = 1;
            for _ in 0..n_iter {
                let s: u64 = (0..n).map(|x| x * state).sum();
                state = state + s - 1;
            }

            assert_eq!(res, state);
        }
    };

    TestHelper::local_remote_env(body);

    let snap_freq = Some(1);

    let execution_list = vec![
        // Complete execution
        TestHelper::persistency_config_test_isa(false, false, None, snap_freq),
        // Restart from snapshot 1
        TestHelper::persistency_config_test_isa(true, false, Some(1), snap_freq),
        // Restart from snapshot 2
        TestHelper::persistency_config_test_isa(true, false, Some(2), snap_freq),
        // Restart from snapshot  5, from the last iteration
        TestHelper::persistency_config_test_isa(true, false, Some(5), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test_isa(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}