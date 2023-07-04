use std::time::Duration;

use noir::operator::source::IteratorSource;
use noir::operator::window::ProcessingTimeWindow;

use super::utils::TestHelper;

#[test]
fn tumbling_processing_time() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(1..=1000);

        let res = env
            .stream(source)
            .group_by(|x| {
                std::thread::sleep(Duration::from_millis(1));
                x % 2
            })
            .window(ProcessingTimeWindow::tumbling(Duration::from_millis(100)))
            .fold(0, |acc, x| *acc += x)
            .drop_key()
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            eprintln!("{res:?}");
            let sum: i32 = res.into_iter().sum();
            assert_eq!(sum, (1..=1000).sum::<i32>());
        }
    });
}

// TODO: FIX THIS
// processing time windows does not support persistency
/*
#[test]
#[serial]
fn tumbling_processing_time_persistency() {
    let body = |mut env: StreamEnvironment| {
        let mut source = IteratorSource::new(1..=1000);
        source.set_snapshot_frequency_by_item(300);
        let res = env
            .stream(source)
            .group_by(|x| {
                std::thread::sleep(Duration::from_millis(1));
                x % 2
            })
            .window(ProcessingTimeWindow::tumbling(Duration::from_millis(100)))
            .fold(0, |acc, x| *acc += x)
            .drop_key()
            .collect_vec();
        env.execute_blocking();

        if let Some(res) = res.get() {
            eprintln!("{res:?}");
            let sum: i32 = res.into_iter().sum();
            assert_eq!(sum, (1..=1000).sum::<i32>());
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
*/
