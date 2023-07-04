use itertools::Itertools;

use noir::{operator::source::IteratorSource, StreamEnvironment, prelude::Source};
use serial_test::serial;
use utils::TestHelper;

mod utils;

#[test]
fn filter_map_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .filter_map(|x| if x % 2 == 1 { Some(x * 2 + 1) } else { None })
            .collect_vec();
        env.execute_blocking();
        if let Some(res) = res.get() {
            assert_eq!(res, &[3, 7, 11, 15, 19]);
        }
    });
}

#[test]
fn filter_map_keyed_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .filter_map(|(_key, x)| if x < 6 { Some(x * 2 + 1) } else { None })
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, (0..6u8).map(|x| x * 2 + 1).collect_vec());
        }
    });
}


#[test]
#[serial]
fn filter_map_keyed_stream_persistency() {
    let body = |mut env: StreamEnvironment| {
        let mut source = IteratorSource::new(0..10u8);
        source.set_snapshot_frequency_by_item(3);
        let res = env
            .stream(source)
            .group_by(|n| n % 2)
            .filter_map(|(_key, x)| if x < 6 { Some(x * 2 + 1) } else { None })
            .drop_key()
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = res.get() {
            res.sort_unstable();
            assert_eq!(res, (0..6u8).map(|x| x * 2 + 1).collect_vec());
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
