//! Utility traits and structures related to the source operators.

use std::time::{Duration, SystemTime};

pub use self::csv::*;
#[cfg(feature = "tokio")]
pub use async_stream::*;
pub use channel::*;
pub use file::*;
pub use iterator::*;
pub use parallel_iterator::*;

use crate::operator::{Data, Operator};

use super::SnapshotId;

#[cfg(feature = "tokio")]
mod async_stream;
mod channel;
mod csv;
mod file;
mod iterator;
mod parallel_iterator;

/// This trait marks all the operators that can be used as sources.
pub trait Source<Out: Data>: Operator<Out> {
    /// The maximum parallelism offered by this operator.
    fn get_max_parallelism(&self) -> Option<usize>;
    /// Set a function to generate snapshot marker based on readed stream items
    fn set_snapshot_frequency_by_item(&mut self, item_interval: u64);
    /// Set a function to generate snapshot marker based on specified time interval
    /// N.B.: The snapshot marker is created at the first call on source next() metod after the time interval is expired
    fn set_snapshot_frequency_by_time(&mut self, time_interval: Duration);
}

#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub (crate) struct SnapshotGenerator {
    snapshot_id: SnapshotId,
    snap_time_interval: Option<Duration>,
    timer: SystemTime,
    snap_item_interval: Option<u64>,
    item_counter: u64,
}

impl SnapshotGenerator {
    pub (crate) fn new() -> Self {
        Self {
            snapshot_id: SnapshotId::new(1),
            snap_time_interval: None,
            timer: SystemTime::now(),
            snap_item_interval: None,
            item_counter: 0, 
        }
    }
    pub (crate) fn get_snapshot_marker(&mut self) -> Option<SnapshotId> {
        let mut res = false;
        if self.snap_time_interval.is_some(){
            res =self.timer.elapsed().unwrap() > self.snap_time_interval.unwrap(); 
        }
        if !res && self.snap_item_interval.is_some(){
            res = self.item_counter == self.snap_item_interval.unwrap();
        }
        self.item_counter = self.item_counter + 1;
        if res {
            let tmp = self.snapshot_id;
            self.item_counter = 0;
            self.timer = SystemTime::now();
            self.snapshot_id = self.snapshot_id + 1;
            return Some(tmp);
        }
        None
    }

    pub (crate) fn set_time_interval(&mut self, time_interval: Duration) {
        self.snap_time_interval = Some(time_interval);
    }

    pub (crate) fn set_item_interval(&mut self, item_interval: u64) {
        self.snap_item_interval = Some(item_interval);
    }

    /// Afeter calling this method the snapshot generation will start from last_snapshot + 1
    pub (crate) fn restart_from(&mut self, last_snapshot: SnapshotId) {
        self.snapshot_id = last_snapshot + 1;
    }

}


#[cfg(test)]
mod tests {
    use std::{time::Duration, thread::sleep};

    use crate::operator::SnapshotId;

    use super::SnapshotGenerator;

    #[test]
    fn test_snapshot_generator_item(){
        let mut g1 = SnapshotGenerator::new();
        g1.set_item_interval(3);

        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), Some(SnapshotId::new(1)));
        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), Some(SnapshotId::new(2)));
    
    }

    #[test]
    fn test_snapshot_generator_time(){
        let mut g1 = SnapshotGenerator::new();
        g1.set_time_interval(Duration::new(2, 0));

        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), None);
        sleep(Duration::new(2, 0));
        assert_eq!(g1.get_snapshot_marker(), Some(SnapshotId::new(1)));
        assert_eq!(g1.get_snapshot_marker(), None);
        sleep(Duration::new(2, 0));
        assert_eq!(g1.get_snapshot_marker(), Some(SnapshotId::new(2)));
    
    }

    #[test]
    fn test_snapshot_generator_item_time(){
        let mut g1 = SnapshotGenerator::new();
        g1.set_time_interval(Duration::new(2, 0));
        g1.set_item_interval(3);

        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), None);
        sleep(Duration::new(2, 0));
        assert_eq!(g1.get_snapshot_marker(), Some(SnapshotId::new(1)));
        assert_eq!(g1.get_snapshot_marker(), None);
        sleep(Duration::new(2, 0));
        assert_eq!(g1.get_snapshot_marker(), Some(SnapshotId::new(2)));
        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), None);
        assert_eq!(g1.get_snapshot_marker(), Some(SnapshotId::new(3)));
    
    }    

}
