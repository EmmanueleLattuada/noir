use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::network::OperatorCoord;
use crate::operator::{Data, DataKey, Operator, StreamElement, Timestamp};
use crate::scheduler::{ExecutionMetadata, OperatorId};
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone)]
pub struct AddTimestamp<Out: Data, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    prev: OperatorChain,
    operator_coord : OperatorCoord,
    timestamp_gen: TimestampGen,
    watermark_gen: WatermarkGen,
    pending_watermark: Option<Timestamp>,
    _out: PhantomData<Out>,
}

impl<Out: Data, TimestampGen, WatermarkGen, OperatorChain> Display
    for AddTimestamp<Out, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> AddTimestamp", self.prev)
    }
}

impl<Out: Data, TimestampGen, WatermarkGen, OperatorChain>
    AddTimestamp<Out, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    fn new(prev: OperatorChain, timestamp_gen: TimestampGen, watermark_gen: WatermarkGen) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0,0,0,op_id),

            timestamp_gen,
            watermark_gen,
            pending_watermark: None,
            _out: Default::default(),
        }
    }
}

impl<Out: Data, TimestampGen, WatermarkGen, OperatorChain> Operator<Out>
    for AddTimestamp<Out, TimestampGen, WatermarkGen, OperatorChain>
where
    OperatorChain: Operator<Out>,
    TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
    WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        if let Some(ts) = self.pending_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        let elem = self.prev.next();
        match elem {
            StreamElement::Item(item) => {
                let ts = (self.timestamp_gen)(&item);
                let watermark = (self.watermark_gen)(&item, &ts);
                self.pending_watermark = watermark;
                StreamElement::Timestamped(item, ts)
            }
            StreamElement::FlushAndRestart
            | StreamElement::FlushBatch
            | StreamElement::Terminate => elem,
            // TODO: handle snapshot marker
            StreamElement::Snapshot(_) => {
                panic!("Snapshot not supported for add_timestamps operator")
            }
            _ => panic!("AddTimestamp received invalid variant: {}", elem.variant()),
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("AddTimestamp");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev
            .structure()
            .add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

#[derive(Clone)]
pub struct DropTimestamp<Out: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    operator_coord: OperatorCoord,
    _out: PhantomData<Out>,
}

impl<Out: Data, OperatorChain> Display for DropTimestamp<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> DropTimestamp", self.prev)
    }
}

impl<Out: Data, OperatorChain> DropTimestamp<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn new(prev: OperatorChain) -> Self {
        let op_id = prev.get_op_id() + 1;
        Self {
            prev,
            // This will be set in setup method
            operator_coord: OperatorCoord::new(0, 0, 0, op_id),
            _out: Default::default(),
        }
    }
}

impl<Out: Data, OperatorChain> Operator<Out> for DropTimestamp<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        self.operator_coord.block_id = metadata.coord.block_id;
        self.operator_coord.host_id = metadata.coord.host_id;
        self.operator_coord.replica_id = metadata.coord.replica_id;
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Watermark(_) => continue,
                StreamElement::Timestamped(item, _) => return StreamElement::Item(item),
                // TODO: handle snapshot marker
                StreamElement::Snapshot(_) => {
                    panic!("Snapshot not supported for collect operator")
                }
                el => return el,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("DropTimestamp");
        let op_id = self.operator_coord.operator_id;
        operator.subtitle = format!("op id: {op_id}");
        self.prev
            .structure()
            .add_operator(operator)
    }

    fn get_op_id(&self) -> OperatorId {
        self.operator_coord.operator_id
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Given a stream without timestamps nor watermarks, tag each item with a timestamp and insert
    /// watermarks.
    ///
    /// The two functions given to this operator are the following:
    /// - `timestamp_gen` returns the timestamp assigned to the provided element of the stream
    /// - `watermark_gen` returns an optional watermark to add after the provided element
    ///
    /// Note that the two functions **must** follow the watermark semantics.
    /// TODO: link to watermark semantics
    ///
    /// ## Example
    ///
    /// In this example the stream contains the integers from 0 to 9, each will be tagged with a
    /// timestamp with the value of the item as milliseconds, and after each even number a watermark
    /// will be inserted.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// use noir::operator::Timestamp;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    ///
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s.add_timestamps(
    ///     |&n| n,
    ///     |&n, &ts| if n % 2 == 0 { Some(ts) } else { None }
    /// );
    /// ```
    pub fn add_timestamps<TimestampGen, WatermarkGen>(
        self,
        timestamp_gen: TimestampGen,
        watermark_gen: WatermarkGen,
    ) -> Stream<Out, impl Operator<Out>>
    where
        TimestampGen: FnMut(&Out) -> Timestamp + Clone + Send + 'static,
        WatermarkGen: FnMut(&Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
    {
        self.add_operator(|prev| AddTimestamp::new(prev, timestamp_gen, watermark_gen))
    }

    pub fn drop_timestamps(self) -> Stream<Out, impl Operator<Out>> {
        self.add_operator(|prev| DropTimestamp::new(prev))
    }
}

impl<Key, Out, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    Key: DataKey,
    Out: Data,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Given a keyed stream without timestamps nor watermarks, tag each item with a timestamp and insert
    /// watermarks.
    ///
    /// The two functions given to this operator are the following:
    /// - `timestamp_gen` returns the timestamp assigned to the provided element of the stream
    /// - `watermark_gen` returns an optional watermark to add after the provided element
    ///
    /// Note that the two functions **must** follow the watermark semantics.
    /// TODO: link to watermark semantics
    ///
    /// ## Example
    ///
    /// In this example the stream contains the integers from 0 to 9 and group them by parity, each will be tagged with a
    /// timestamp with the value of the item as milliseconds, and after each even number a watermark
    /// will be inserted.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// use noir::operator::Timestamp;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    ///
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s
    ///     .group_by(|i| i % 2)
    ///     .add_timestamps(
    ///     |&(_k, n)| n,
    ///     |&(_k, n), &ts| if n % 2 == 0 { Some(ts) } else { None }
    /// );
    /// ```
    pub fn add_timestamps<TimestampGen, WatermarkGen>(
        self,
        timestamp_gen: TimestampGen,
        watermark_gen: WatermarkGen,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        TimestampGen: FnMut(&KeyValue<Key, Out>) -> Timestamp + Clone + Send + 'static,
        WatermarkGen:
            FnMut(&KeyValue<Key, Out>, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
    {
        self.add_operator(|prev| AddTimestamp::new(prev, timestamp_gen, watermark_gen))
    }

    pub fn drop_timestamps(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        self.add_operator(|prev| DropTimestamp::new(prev))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::add_timestamps::AddTimestamp;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn add_timestamps() {
        let fake_operator = FakeOperator::new(0..10u64);

        let mut oper = AddTimestamp::new(
            fake_operator,
            |n| *n as i64,
            |n, ts| {
                if n % 2 == 0 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        for i in 0..5u64 {
            let t = i * 2;
            assert_eq!(oper.next(), StreamElement::Timestamped(t, t as i64));
            assert_eq!(oper.next(), StreamElement::Watermark(t as i64));
            assert_eq!(oper.next(), StreamElement::Timestamped(t + 1, t as i64 + 1));
        }
        assert_eq!(oper.next(), StreamElement::Terminate);
    }
}
