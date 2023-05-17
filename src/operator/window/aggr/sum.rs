use std::ops::AddAssign;

use super::{super::*, Fold};
use crate::operator::{ExchangeData, ExchangeDataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowBuilder<Out>,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: ExchangeDataKey,
    Out: ExchangeData,
{
    pub fn sum<NewOut: ExchangeData + Default + AddAssign<Out>>(
        self,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>> {
        let acc = Fold::new(NewOut::default(), |sum, x| *sum += x);
        self.add_window_operator("WindowSum", acc)
    }
}
