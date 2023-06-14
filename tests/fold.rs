use itertools::Itertools;
use serial_test::serial;

use noir::{operator::source::IteratorSource, prelude::Source, StreamEnvironment};
use utils::TestHelper;

mod utils;

#[test]
fn fold_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .fold("".to_string(), |s, n| *s += &n.to_string())
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], "0123456789");
        }
    });
}

#[test]
fn fold_assoc_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .fold_assoc(
                "".to_string(),
                |s, n| *s += &n.to_string(),
                |s1, s2| *s1 += &s2,
            )
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], "0123456789");
        }
    });
}

#[test]
fn fold_shuffled_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .fold(Vec::new(), |v, n| v.push(n))
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            assert_eq!(res.len(), 1);
            res[0].sort_unstable();
            assert_eq!(res[0], (0..10u8).collect_vec());
        }
    });
}

#[test]
fn fold_assoc_shuffled_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .shuffle()
            .fold_assoc(
                Vec::new(),
                |v, n| v.push(n),
                |v1, mut v2| v1.append(&mut v2),
            )
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            assert_eq!(res.len(), 1);
            res[0].sort_unstable();
            assert_eq!(res[0], (0..10u8).collect_vec());
        }
    });
}

#[test]
#[serial]
fn fold_stream_persistency() {
    let body = |mut env: StreamEnvironment| {
        let mut source = IteratorSource::new(0..10u8);
        source.set_snapshot_frequency_by_item(3);
        let res = env
            .stream(source)
            .fold("".to_string(), |s, n| *s += &n.to_string())
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], "0123456789");
        }
    };

    let execution_list = vec![
        // Complete execution
        TestHelper::persistency_config_test(false, false, None),
        // Restart from snapshot 1
        TestHelper::persistency_config_test(true, false, Some(1)),
        // Restart from snapshot 2
        TestHelper::persistency_config_test(true, false, Some(2)),
        // Restart from snapshot 4, the first block has already terminated
        TestHelper::persistency_config_test(true, false, Some(4)),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);   
}


#[test]
#[serial]
fn fold_assoc_shuffled_stream_persistency() {
    let body = |mut env: StreamEnvironment| {
        let mut source = IteratorSource::new(0..10u8);
        source.set_snapshot_frequency_by_item(3);
        let res = env
            .stream(source)
            .shuffle()
            .fold_assoc(
                Vec::new(),
                |v, n| v.push(n),
                |v1, mut v2| v1.append(&mut v2),
            )
            .collect_vec();
        env.execute();
        if let Some(mut res) = res.get() {
            assert_eq!(res.len(), 1);
            res[0].sort_unstable();
            assert_eq!(res[0], (0..10u8).collect_vec());
        }
    };

    let execution_list = vec![
        // Complete execution
        TestHelper::persistency_config_test(false, false, None),
        // Restart from snapshot 1
        TestHelper::persistency_config_test(true, false, Some(1)),
        // Restart from snapshot 2
        TestHelper::persistency_config_test(true, false, Some(2)),
        // Restart from snapshot 4, the first block has already terminated
        TestHelper::persistency_config_test(true, false, Some(4)),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);
}
