
#[cfg(feature = "persist-state")]
use noir::StreamEnvironment;
#[cfg(feature = "persist-state")]
use serial_test::serial;
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

#[cfg(feature = "persist-state")]
#[test]
#[serial]
fn tumbling_processing_time_persistency() {
    let body = |mut env: StreamEnvironment| {
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
    };

    let snap_freq = Some(300);

    let execution_list = vec![
        // Complete execution
        TestHelper::persistency_config_test(false, false, None, snap_freq),
        // Restart from snapshot 1
        TestHelper::persistency_config_test(true, false, Some(1), snap_freq),
        // Restart from snapshot 2
        TestHelper::persistency_config_test(true, false, Some(2), snap_freq),
        // Restart from snapshot 4, the first block has already terminated
        TestHelper::persistency_config_test(true, false, Some(4), snap_freq),
        // Restart from last snapshot, all operators have terminated
        TestHelper::persistency_config_test(true, true, None, snap_freq),
    ];

    TestHelper::local_remote_env_with_persistency(body, execution_list);

}

