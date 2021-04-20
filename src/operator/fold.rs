use std::sync::Arc;

use crate::block::{BlockStructure, NextStrategy, OperatorStructure};
use crate::operator::{Data, EndBlock, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::Stream;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Fold<Out: Data, NewOut: Data, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    fold: Arc<dyn Fn(NewOut, Out) -> NewOut + Send + Sync>,
    init: NewOut,
    accumulator: Option<NewOut>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<Out: Data, NewOut: Data, PreviousOperators: Operator<Out>>
    Fold<Out, NewOut, PreviousOperators>
{
    fn new<F>(prev: PreviousOperators, init: NewOut, fold: F) -> Self
    where
        F: Fn(NewOut, Out) -> NewOut + Send + Sync + 'static,
    {
        Fold {
            prev,
            fold: Arc::new(fold),
            init,
            accumulator: None,
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }
}

impl<Out: Data, NewOut: Data, PreviousOperators> Operator<NewOut>
    for Fold<Out, NewOut, PreviousOperators>
where
    PreviousOperators: Operator<Out> + Send,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::End => self.received_end = true,
                StreamElement::IterEnd => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item(item) => {
                    self.accumulator = Some((self.fold)(
                        self.accumulator.take().unwrap_or_else(|| self.init.clone()),
                        item,
                    ));
                }
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    self.accumulator = Some((self.fold)(
                        self.accumulator.take().unwrap_or_else(|| self.init.clone()),
                        item,
                    ));
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        // If there is an accumulated value, return it
        if let Some(acc) = self.accumulator.take() {
            if let Some(ts) = self.timestamp.take() {
                return StreamElement::Timestamped(acc, ts);
            } else {
                return StreamElement::Item(acc);
            }
        }

        // If watermark were received, send one downstream
        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::IterEnd;
        }

        StreamElement::End
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Fold<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("Fold"))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + Send + 'static,
{
    pub fn fold<NewOut: Data, F>(self, init: NewOut, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(NewOut, Out) -> NewOut + Send + Sync + 'static,
    {
        let mut new_stream = self.add_block(EndBlock::new, NextStrategy::OnlyOne);
        // FIXME: when implementing Stream::max_parallelism use that here
        new_stream.block.scheduler_requirements.max_parallelism(1);
        new_stream.add_operator(|prev| Fold::new(prev, init, f))
    }

    pub fn fold_assoc<NewOut: Data, Local, Global>(
        self,
        init: NewOut,
        local: Local,
        global: Global,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        Local: Fn(NewOut, Out) -> NewOut + Send + Sync + 'static,
        Global: Fn(NewOut, NewOut) -> NewOut + Send + Sync + 'static,
    {
        // Local fold
        let mut second_part = self
            .add_operator(|prev| Fold::new(prev, init.clone(), local))
            .add_block(EndBlock::new, NextStrategy::OnlyOne);

        // Global fold (which is done on only one node)
        // FIXME: when implementing Stream::max_parallelism use that here
        second_part.block.scheduler_requirements.max_parallelism(1);
        second_part.add_operator(|prev| Fold::new(prev, init, global))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::operator::fold::Fold;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_fold_without_timestamps() {
        let fake_operator = FakeOperator::new(0..10u8);
        let mut fold = Fold::new(fake_operator, 0, |a, b| a + b);

        assert_eq!(fold.next(), StreamElement::Item((0..10u8).sum()));
        assert_eq!(fold.next(), StreamElement::End);
    }

    #[test]
    fn test_fold_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(0, Duration::from_secs(1)));
        fake_operator.push(StreamElement::Timestamped(1, Duration::from_secs(2)));
        fake_operator.push(StreamElement::Timestamped(2, Duration::from_secs(3)));
        fake_operator.push(StreamElement::Watermark(Duration::from_secs(4)));

        let mut fold = Fold::new(fake_operator, 0, |a, b| a + b);

        assert_eq!(
            fold.next(),
            StreamElement::Timestamped(0 + 1 + 2, Duration::from_secs(3))
        );
        assert_eq!(
            fold.next(),
            StreamElement::Watermark(Duration::from_secs(4))
        );
        assert_eq!(fold.next(), StreamElement::End);
    }

    #[test]
    fn test_fold_iter_end() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(0));
        fake_operator.push(StreamElement::Item(1));
        fake_operator.push(StreamElement::Item(2));
        fake_operator.push(StreamElement::IterEnd);
        fake_operator.push(StreamElement::Item(3));
        fake_operator.push(StreamElement::Item(4));
        fake_operator.push(StreamElement::Item(5));
        fake_operator.push(StreamElement::IterEnd);

        let mut fold = Fold::new(fake_operator, 0, |a, b| a + b);

        assert_eq!(fold.next(), StreamElement::Item(0 + 1 + 2));
        assert_eq!(fold.next(), StreamElement::IterEnd);
        assert_eq!(fold.next(), StreamElement::Item(3 + 4 + 5));
        assert_eq!(fold.next(), StreamElement::IterEnd);
        assert_eq!(fold.next(), StreamElement::End);
    }
}
