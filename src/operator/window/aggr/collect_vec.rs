use super::super::*;
use crate::operator::Operator;
use crate::stream::{KeyValue, KeyedStream, WindowedStream};

#[derive(Clone)]
struct CollectVec<I, O, F>
where
    F: Fn(Vec<I>) -> O,
{
    vec: Vec<I>,
    f: F,
    _o: PhantomData<O>,
}

impl<I, O, F> WindowAccumulator for CollectVec<I, O, F>
where
    F: Fn(Vec<I>) -> O + Send + Clone + 'static,
    I: ExchangeData,
    O: Clone + Send + 'static,
{
    type In = I;

    type Out = O;
    type AccumulatorState = Vec<I>;

    #[inline]
    fn process(&mut self, el: Self::In) {
        self.vec.push(el);
    }

    #[inline]
    fn output(self) -> Self::Out {
        (self.f)(self.vec)
    }

    fn get_state(&self) -> Self::AccumulatorState {
        self.vec.clone()
    }

    fn set_state(&mut self, state: Self::AccumulatorState) {
        self.vec = state;
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowBuilder<Out>,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: ExchangeDataKey,
    Out: ExchangeData + Ord,
{
    /// Prefer other aggregators if possible as they don't save all elements
    pub fn map<NewOut: ExchangeData, F: Fn(Vec<Out>) -> NewOut + Send + Clone + 'static>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>> {
        let acc = CollectVec::<Out, NewOut, _> {
            vec: Default::default(),
            f,
            _o: PhantomData,
        };
        self.add_window_operator("WindowMap", acc)
    }
}
