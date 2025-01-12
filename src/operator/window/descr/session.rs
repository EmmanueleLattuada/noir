use std::time::{Duration, Instant, SystemTime};

use super::super::*;
use crate::operator::{Data, StreamElement};

#[derive(Clone)]
pub struct SessionWindowManager<A>
where
    A: WindowAccumulator,
{
    init: A,
    gap: Duration,
    w: Option<Slot<A>>,
}

#[derive(Clone)]
struct Slot<A> {
    acc: A,
    last: Instant,
}

impl<A> Slot<A> {
    #[inline]
    fn new(acc: A, last: Instant) -> Self {
        Self { acc, last }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SessionWindowManagerState<AS>{
    w: Option<SlotState<AS>>,
}

#[derive(Clone, Serialize, Deserialize)]
struct SlotState<AS> {
    acc: AS,
    last: Duration,
    abs_time: SystemTime,
}

impl<A: WindowAccumulator> WindowManager for SessionWindowManager<A>
where
    A::In: Data,
    A::Out: Data,
{
    type In = A::In;
    type Out = A::Out;
    type Output = Option<WindowResult<A::Out>>;
    type ManagerState = SessionWindowManagerState<A::AccumulatorState>;

    #[inline]
    fn process(&mut self, el: StreamElement<A::In>) -> Self::Output {
        let ts = Instant::now();

        let ret = match &self.w {
            Some(slot) if ts - slot.last > self.gap => {
                let output = self.w.take().unwrap().acc.output();
                Some(WindowResult::Item(output))
            }
            _ => None,
        };

        match el {
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let slot = self
                    .w
                    .get_or_insert_with(|| Slot::new(self.init.clone(), ts));
                slot.acc.process(item);
                slot.last = ts;
                ret
            }
            StreamElement::Terminate | StreamElement::FlushAndRestart => {
                ret.or_else(|| self.w.take().map(|s| WindowResult::Item(s.acc.output())))
            }
            _ => ret,
        }
    }

    fn get_state(&self) -> Self::ManagerState {
        let w = match self.w.clone() {
            Some(slot) => {
                Some(SlotState {
                    acc: slot.acc.get_state(),
                    last: slot.last.elapsed(),
                    abs_time: SystemTime::now(),
                })
            }
            None => None
        };
        SessionWindowManagerState {
            w,
        }
    }

    /// For last of the window it will try to restore the original value by 
    /// calculating the elapsed time to the snapshot and the elapsed time from
    /// the snapshot and now
    fn set_state(&mut self, state: Self::ManagerState) {
        self.w = match state.w {
            Some(slot) => {
                // Unwrap should always succeed, maybe remove default and panic
                let elapsed_abs_time = slot.abs_time.elapsed().unwrap_or(Duration::new(0, 0));
                let mut saved_slot = Slot {
                    acc: self.init.clone(),
                    last: Instant::now().checked_sub(slot.last + elapsed_abs_time).unwrap(),
                };
                saved_slot.acc.set_state(slot.acc);
                Some(saved_slot)
            }
            None => None
        };
    }
}

/// Window that splits after if no element is received for a fixed wall clock duration
#[derive(Clone)]
pub struct SessionWindow {
    gap: Duration,
}

impl SessionWindow {
    #[inline]
    pub fn new(gap_millis: Duration) -> Self {
        assert!(!gap_millis.is_zero(), "window size must be > 0");
        Self { gap: gap_millis }
    }
}

impl<T: Data> WindowDescription<T> for SessionWindow {
    type Manager<A: WindowAccumulator<In = T>> = SessionWindowManager<A>;

    #[inline]
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A> {
        SessionWindowManager {
            init: accumulator,
            gap: self.gap,
            w: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::operator::window::aggr::Fold;

    macro_rules! save_result {
        ($ret:expr, $v:expr) => {{
            let iter = $ret.into_iter().map(|r| r.unwrap_item());
            $v.extend(iter);
        }};
    }

    #[test]
    fn event_time_window() {
        let window = SessionWindow::new(Duration::from_millis(10));

        let fold = Fold::new(Vec::new(), |v, el| v.push(el));
        let mut manager = window.build(fold);

        let mut received = Vec::new();
        for i in 0..100i64 {
            if i == 33 || i == 80 {
                std::thread::sleep(Duration::from_millis(11))
            }
            save_result!(
                manager.process(StreamElement::Timestamped(i, i / 10)),
                received
            );
        }
        save_result!(manager.process(StreamElement::FlushAndRestart), received);

        received.sort();

        let expected: Vec<Vec<_>> =
            vec![(0..33).collect(), (33..80).collect(), (80..100).collect()];
        assert_eq!(received, expected)
    }
}
