//! Utility traits and structures related to the source operators.

#[cfg(feature = "persist-state")]
use std::time::{Duration, Instant};

pub use self::csv::*;
#[cfg(feature = "tokio")]
pub use async_stream::*;
pub use channel::*;
pub use file::*;
pub use iterator::*;
pub use parallel_iterator::*;

use crate::{
    block::Replication,
    operator::{Data, Operator},
};

#[cfg(feature = "persist-state")]
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
    fn replication(&self) -> Replication;
}

#[cfg(feature = "persist-state")]
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub (crate) struct SnapshotGenerator {
    snapshot_id: SnapshotId,
    snap_time_interval: Option<Duration>,
    timer: Instant,
    snap_item_interval: Option<u64>,
    item_counter: u64,
    iter_stack: usize,
}
#[cfg(feature = "persist-state")]
impl SnapshotGenerator {
    pub (crate) fn new() -> Self {
        Self {
            snapshot_id: SnapshotId::new(1),
            snap_time_interval: None,
            timer: Instant::now(),
            snap_item_interval: None,
            item_counter: 0, 
            iter_stack: 0,
        }
    }
    pub (crate) fn get_snapshot_marker(&mut self) -> Option<SnapshotId> {
        let mut res = false;
        if self.snap_time_interval.is_some(){
            // Avoid deadlock with to high snapshot freq, produce at least 1 tuple between 2 snapshot tokens
            res =self.timer.elapsed() > self.snap_time_interval.unwrap() && self.item_counter != 0;
        }
        if !res && self.snap_item_interval.is_some(){
            res = self.item_counter == self.snap_item_interval.unwrap();
        }
        self.item_counter += 1;
        if res {
            let tmp = self.snapshot_id.clone();
            self.item_counter = 0;
            if let Some(sti) = self.snap_time_interval {
                // Avoid timer lag
                let lag = self.timer.elapsed().checked_sub(sti);
                self.timer = Instant::now() - lag.unwrap_or(Duration::default());
            }            
            if self.iter_stack == 0 {
                self.snapshot_id = self.snapshot_id.next();
            } else {
                self.snapshot_id = self.snapshot_id.next_iter(self.iter_stack);
            }
            return Some(tmp);
        }
        None
    }

    pub (crate) fn get_flush_snapshot_marker(&mut self) -> SnapshotId {
        let tmp = self.snapshot_id.clone();
        self.item_counter = 0;
        self.timer = Instant::now();
        if self.iter_stack == 0 {
            self.snapshot_id = self.snapshot_id.next();
        } else {
            self.snapshot_id = self.snapshot_id.next_iter(self.iter_stack);
        }
        tmp
    }

    pub (crate) fn set_iter_stack(&mut self, iter_stack: usize) {
        self.iter_stack = iter_stack;
        self.snapshot_id.iteration_stack = vec![0; self.iter_stack];
    }

    pub (crate) fn set_time_interval(&mut self, time_interval: Duration) {
        self.snap_time_interval = Some(time_interval);
        self.timer = Instant::now();
    }

    pub (crate) fn set_item_interval(&mut self, item_interval: u64) {
        self.snap_item_interval = Some(item_interval);
    }

    /// Afeter calling this method the snapshot generation will start from last_snapshot + 1
    pub (crate) fn restart_from(&mut self, last_snapshot: SnapshotId) {
        if self.iter_stack == 0 {
            self.snapshot_id = last_snapshot.next();
        } else {
            self.snapshot_id = last_snapshot.next_iter(self.iter_stack);
        }
    }

}

#[cfg(feature = "persist-state")]
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

    
    #[test]
    fn test_snapshot_order(){
        let mut snap1 = SnapshotId::new(2);
        snap1.iteration_stack.push(1);
        assert!(snap1 > SnapshotId::new(1));
        assert!(snap1 >= SnapshotId::new(2));
        assert!(snap1 < SnapshotId::new(3));
        assert!(SnapshotId::new(2).check_next(&snap1));
        assert!(snap1.check_next(&SnapshotId::new(3)));
        assert!(!(snap1.check_next(&SnapshotId::new(2))));

        let mut snap2 = SnapshotId::new(2);
        snap2.iteration_stack.push(0);
        snap2.iteration_stack.push(1);        
        assert!(snap2 < snap1);
        assert!(SnapshotId::new(2).check_next(&snap2));
        assert!(snap2.check_next(&snap1));
        assert!(!(snap1.check_next(&snap2)));

        let mut snap3 = SnapshotId::new(2);
        snap3.iteration_stack.push(0);
        snap3.iteration_stack.push(0);
        snap3.iteration_stack.push(1);        
        assert!(snap3 < snap1);
        assert!(snap3 < snap2);
        assert!(SnapshotId::new(2).check_next(&snap3));
        assert!(snap3.check_next(&snap2));
        assert!(!(snap2.check_next(&snap3)));

        let s_next = snap1.next();
        assert_eq!(s_next.id(), snap1.id() + 1);
        assert_eq!(s_next.terminate(), snap1.terminate());
        assert_eq!(s_next.iteration_stack,snap1.iteration_stack);
        let s_next = snap3.next();
        assert_eq!(s_next.id(), snap3.id() + 1);
        assert_eq!(s_next.terminate(), snap3.terminate());
        assert_eq!(s_next.iteration_stack, snap3.iteration_stack);

        let s_next = snap1.next_iter(2);
        assert_eq!(s_next.id(), snap2.id());
        assert_eq!(s_next.terminate(), snap2.terminate());
        assert_eq!(s_next.iteration_stack, vec![1, 1]);
        let s_next = snap1.next_iter(3);
        assert_eq!(s_next.id(), snap3.id());
        assert_eq!(s_next.terminate(), snap3.terminate());
        assert_eq!(s_next.iteration_stack, vec![1, 0, 1]);

        let s_next = snap3.next_iter(2);
        assert_eq!(s_next.id(), snap3.id());
        assert_eq!(s_next.terminate(), snap3.terminate());
        assert_eq!(s_next.iteration_stack, vec![0, 1, 1]);

    
    }  



}
