use super::super::*;
use crate::operator::{ExchangeData, ExchangeDataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

#[derive(Clone)]
pub(crate) struct First<T>(Option<T>);

impl<T: ExchangeData> WindowAccumulator for First<T> {
    type In = T;
    type Out = T;
    type AccumulatorState = Option<T>;

    #[inline]
    fn process(&mut self, el: Self::In) {
        if self.0.is_none() {
            self.0 = Some(el);
        }
    }

    #[inline]
    fn output(self) -> Self::Out {
        self.0
            .expect("First::output() called before any element was processed")
    }

    fn get_state(&self) -> Self::AccumulatorState {
        self.0.clone()
    }

    fn set_state(&mut self, state: Self::AccumulatorState) {
        self.0 = state;
    }
}

#[derive(Clone)]
pub(crate) struct Last<T>(Option<T>);

impl<T: ExchangeData> WindowAccumulator for Last<T> {
    type In = T;
    type Out = T;

    type AccumulatorState = Option<T>;

    #[inline]
    fn process(&mut self, el: Self::In) {
        self.0 = Some(el);
    }

    #[inline]
    fn output(self) -> Self::Out {
        self.0
            .expect("First::output() called before any element was processed")
    }

    fn get_state(&self) -> Self::AccumulatorState {
        self.0.clone()
    }

    fn set_state(&mut self, state: Self::AccumulatorState) {
        self.0 = state;
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<(Key, Out)> + 'static,
    Key: ExchangeDataKey,
    Out: ExchangeData,
{
    pub fn first(self) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>> {
        let acc = First(None);
        self.add_window_operator("WindowFirst", acc)
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<(Key, Out)> + 'static,
    Key: ExchangeDataKey,
    Out: ExchangeData,
{
    pub fn last(self) -> KeyedStream<Key, Out, impl Operator<(Key, Out)>> {
        let acc = Last(None);
        self.add_window_operator("WindowLast", acc)
    }
}
