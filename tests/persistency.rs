use noir::{StreamEnvironment, prelude::FileSource, BatchMode};
use serial_test::serial;
use tempfile::NamedTempFile;
use std::io::Write;
use utils::TestHelper;

mod utils;

fn write_file(file: &NamedTempFile) {
    write!(file.as_file(), "dog monkey \n").unwrap();
    write!(file.as_file(), "cat \n").unwrap();
    write!(file.as_file(), "horse \n").unwrap();
    write!(file.as_file(), "monkey \n").unwrap();
    write!(file.as_file(), "dog cat \n").unwrap();
    write!(file.as_file(), "horse \n").unwrap();
    write!(file.as_file(), "horse horse \n").unwrap();
    write!(file.as_file(), "cat dog monkey \n").unwrap();
}

#[cfg(feature = "persist-state")]
#[test]
#[serial]
fn word_count_persistency() {
    let body = |mut env: StreamEnvironment| {
        let file = NamedTempFile::new().unwrap();
        write_file(&file);
        let source = FileSource::new(file.path());
        let result = env
            .stream(source)
            .batch_mode(BatchMode::fixed(1024))
            .flat_map(move |line| line.split_whitespace().map(str::to_lowercase).collect::<Vec<String>>() )
            .group_by(|word| word.clone())
            .fold(0, |count, _word| *count += 1)
            .collect_vec();
        env.execute_blocking();
        if let Some(mut res) = result.get() {            
            res.sort_unstable(); // the output order is nondeterministic
            assert_eq!(res, vec![
                (String::from("cat"), 3), 
                (String::from("dog"), 3), 
                (String::from("horse"), 4),
                (String::from("monkey"), 3),
                ]);
        }
    };

    let snap_freq = Some(1); // One every line

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