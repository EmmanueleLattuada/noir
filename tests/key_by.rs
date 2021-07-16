use itertools::Itertools;

use rstream::operator::source::IteratorSource;
use utils::TestHelper;

mod utils;

#[test]
fn key_by_stream() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..10u8);
        let res = env.stream(source).shuffle().key_by(|&n| n).collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..10u8).map(|n| (n, n)).collect_vec();
            assert_eq!(res, expected);
        }
    });
}

#[test]
fn key_by_stream2() {
    TestHelper::local_remote_env(|mut env| {
        let source = IteratorSource::new(0..100u8);
        let res = env
            .stream(source)
            .shuffle()
            .key_by(|&n| n.to_string().chars().next().unwrap())
            .collect_vec();
        env.execute();
        if let Some(res) = res.get() {
            let res = res.into_iter().sorted().collect_vec();
            let expected = (0..100u8)
                .map(|n| (n.to_string().chars().next().unwrap(), n))
                .sorted()
                .collect_vec();
            assert_eq!(res, expected);
        }
    });
}
