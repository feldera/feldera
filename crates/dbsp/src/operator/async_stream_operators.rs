//! This module contains several wrappers that make it easier to implement splitter operators
//! that produce outputs over multiple steps. Such operators generally need to be able to stop
//! generating outputs at any point and preserve their state until the next step. This can be
//! highly error prone (this is known as the stack ripping problem).
//!
//! It's much easier to implement such operators as streaming operators using `async_stream`
//! crate's `stream!` macro, which allows to yield outputs at any point in the code.
//!
//! We define traits for such operators whose eval method returns `futures::Stream` and
//! implement wrappers that allow using them as regular binary, ternary, quaternary,
//! and n-ary operators.
//!
//! # Assumptions
//!
//! These wrappers assume that the inner operator's `eval` method can only produce multiple inputs
//! after a `flush` call; otherwise, if `flush` is invoked while the output stream is active, it will
//! cause ownership conflict and panic.

use std::{borrow::Cow, marker::PhantomData, pin::Pin, rc::Rc, sync::Arc};

use crate::{
    circuit::{
        metadata::{OperatorLocation, OperatorMeta},
        operator_traits::{
            BinaryOperator, NaryOperator, Operator, QuaternaryOperator, TernaryOperator,
        },
        GlobalNodeId,
    },
    Error, Position, Scope,
};
use feldera_storage::{FileCommitter, StoragePath};
use futures::Stream as AsyncStream;
use futures_util::StreamExt;

pub trait StreamingBinaryOperator<I1, I2, O>: Operator {
    fn eval(
        self: Rc<Self>,
        lhs: &I1,
        rhs: &I2,
    ) -> impl AsyncStream<Item = (O, bool, Option<Position>)> + 'static;
}

pub struct StreamingBinaryWrapper<I1, I2, O, Op> {
    operator: Rc<Op>,
    stream: Option<Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>>,
    progress: Option<Position>,
    phantom: PhantomData<fn(&I1, &I2, &O)>,
}

impl<I1, I2, O, Op> StreamingBinaryWrapper<I1, I2, O, Op> {
    pub fn new(operator: Op) -> Self {
        Self {
            operator: Rc::new(operator),
            stream: None,
            progress: None,
            phantom: PhantomData,
        }
    }
}

impl<I1, I2, O, Op> Operator for StreamingBinaryWrapper<I1, I2, O, Op>
where
    I1: 'static,
    I2: 'static,
    O: 'static,
    Op: StreamingBinaryOperator<I1, I2, O> + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn location(&self) -> OperatorLocation {
        self.operator.location()
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        Rc::get_mut(&mut self.operator).unwrap().init(global_id);
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        self.operator.metadata(meta);
    }

    fn clock_start(&mut self, scope: Scope) {
        Rc::get_mut(&mut self.operator).unwrap().clock_start(scope);
    }

    fn clock_end(&mut self, scope: Scope) {
        Rc::get_mut(&mut self.operator).unwrap().clock_end(scope);
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn is_input(&self) -> bool {
        self.operator.is_input()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .register_ready_callback(cb);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    #[allow(unused_variables)]
    fn checkpoint(
        &mut self,
        base: &StoragePath,
        persistent_id: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .checkpoint(base, persistent_id, files)
    }

    #[allow(unused_variables)]
    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .restore(base, persistent_id)
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().clear_state()
    }

    fn start_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().start_replay()
    }

    fn is_replay_complete(&self) -> bool {
        self.operator.is_replay_complete()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().end_replay()
    }

    fn flush(&mut self) {
        assert!(self.stream.is_none(), "flush called while stream is active");
        Rc::get_mut(&mut self.operator).unwrap().flush();
    }

    fn is_flush_complete(&self) -> bool {
        self.stream.is_none()
    }

    fn flush_progress(&self) -> Option<Position> {
        self.progress.clone()
    }
}

impl<I1, I2, O, Op> BinaryOperator<I1, I2, O> for StreamingBinaryWrapper<I1, I2, O, Op>
where
    I1: 'static,
    I2: 'static,
    O: 'static,
    Op: StreamingBinaryOperator<I1, I2, O> + 'static,
{
    async fn eval(&mut self, lhs: &I1, rhs: &I2) -> O {
        if self.stream.is_none() {
            self.stream = Some(Box::pin(self.operator.clone().eval(lhs, rhs))
                as Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>);
        }

        let stream = self.stream.as_mut().unwrap();

        let Some((output, complete, progress)) = stream.next().await else {
            panic!("StreamingBinaryOperator unexpectedly reached end of stream");
        };

        self.progress = progress;

        if complete {
            self.stream = None;
            output
        } else {
            output
        }
    }
}

pub trait StreamingTernaryOperator<I1, I2, I3, O>: Operator
where
    I1: Clone,
    I2: Clone,
    I3: Clone,
{
    fn eval(
        self: Rc<Self>,
        i1: Cow<'_, I1>,
        i2: Cow<'_, I2>,
        i3: Cow<'_, I3>,
    ) -> impl AsyncStream<Item = (O, bool, Option<Position>)> + 'static;
}

pub struct StreamingTernaryWrapper<I1, I2, I3, O, Op> {
    operator: Rc<Op>,
    stream: Option<Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>>,
    progress: Option<Position>,
    phantom: PhantomData<fn(&I1, &I2, &I3, &O)>,
}

impl<I1, I2, I3, O, Op> StreamingTernaryWrapper<I1, I2, I3, O, Op> {
    pub fn new(operator: Op) -> Self {
        Self {
            operator: Rc::new(operator),
            stream: None,
            progress: None,
            phantom: PhantomData,
        }
    }
}

impl<I1, I2, I3, O, Op> Operator for StreamingTernaryWrapper<I1, I2, I3, O, Op>
where
    I1: Clone + 'static,
    I2: Clone + 'static,
    I3: Clone + 'static,
    O: 'static,
    Op: StreamingTernaryOperator<I1, I2, I3, O> + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn location(&self) -> OperatorLocation {
        self.operator.location()
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        Rc::get_mut(&mut self.operator).unwrap().init(global_id);
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        self.operator.metadata(meta);
    }

    fn clock_start(&mut self, scope: Scope) {
        Rc::get_mut(&mut self.operator).unwrap().clock_start(scope);
    }

    fn clock_end(&mut self, scope: Scope) {
        Rc::get_mut(&mut self.operator).unwrap().clock_end(scope);
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn is_input(&self) -> bool {
        self.operator.is_input()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .register_ready_callback(cb);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    #[allow(unused_variables)]
    fn checkpoint(
        &mut self,
        base: &StoragePath,
        persistent_id: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .checkpoint(base, persistent_id, files)
    }

    #[allow(unused_variables)]
    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .restore(base, persistent_id)
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().clear_state()
    }

    fn start_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().start_replay()
    }

    fn is_replay_complete(&self) -> bool {
        self.operator.is_replay_complete()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().end_replay()
    }

    fn flush(&mut self) {
        assert!(self.stream.is_none(), "flush called while stream is active");
        Rc::get_mut(&mut self.operator).unwrap().flush();
    }

    fn is_flush_complete(&self) -> bool {
        self.stream.is_none()
    }

    fn flush_progress(&self) -> Option<Position> {
        self.progress.clone()
    }
}

impl<I1, I2, I3, O, Op> TernaryOperator<I1, I2, I3, O>
    for StreamingTernaryWrapper<I1, I2, I3, O, Op>
where
    I1: Clone + 'static,
    I2: Clone + 'static,
    I3: Clone + 'static,
    O: 'static,
    Op: StreamingTernaryOperator<I1, I2, I3, O> + 'static,
{
    async fn eval(&mut self, i1: Cow<'_, I1>, i2: Cow<'_, I2>, i3: Cow<'_, I3>) -> O {
        if self.stream.is_none() {
            self.stream = Some(Box::pin(self.operator.clone().eval(i1, i2, i3))
                as Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>);
        }

        let stream = self.stream.as_mut().unwrap();

        let Some((output, complete, progress)) = stream.next().await else {
            panic!("StreamingTernaryOperator unexpectedly reached end of stream");
        };

        self.progress = progress;

        if complete {
            self.stream = None;
            output
        } else {
            output
        }
    }
}

pub trait StreamingQuaternaryOperator<I1, I2, I3, I4, O>: Operator
where
    I1: Clone,
    I2: Clone,
    I3: Clone,
    I4: Clone,
{
    fn eval(
        self: Rc<Self>,
        i1: Cow<'_, I1>,
        i2: Cow<'_, I2>,
        i3: Cow<'_, I3>,
        i4: Cow<'_, I4>,
    ) -> impl AsyncStream<Item = (O, bool, Option<Position>)> + 'static;
}

pub struct StreamingQuaternaryWrapper<I1, I2, I3, I4, O, Op> {
    operator: Rc<Op>,
    stream: Option<Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>>,
    progress: Option<Position>,
    phantom: PhantomData<fn(&I1, &I2, &I3, &I4, &O)>,
}

impl<I1, I2, I3, I4, O, Op> StreamingQuaternaryWrapper<I1, I2, I3, I4, O, Op> {
    pub fn new(operator: Op) -> Self {
        Self {
            operator: Rc::new(operator),
            stream: None,
            progress: None,
            phantom: PhantomData,
        }
    }
}

impl<I1, I2, I3, I4, O, Op> Operator for StreamingQuaternaryWrapper<I1, I2, I3, I4, O, Op>
where
    I1: Clone + 'static,
    I2: Clone + 'static,
    I3: Clone + 'static,
    I4: Clone + 'static,
    O: 'static,
    Op: StreamingQuaternaryOperator<I1, I2, I3, I4, O> + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn location(&self) -> OperatorLocation {
        self.operator.location()
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        Rc::get_mut(&mut self.operator).unwrap().init(global_id);
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        self.operator.metadata(meta);
    }

    fn clock_start(&mut self, scope: Scope) {
        Rc::get_mut(&mut self.operator).unwrap().clock_start(scope);
    }

    fn clock_end(&mut self, scope: Scope) {
        Rc::get_mut(&mut self.operator).unwrap().clock_end(scope);
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn is_input(&self) -> bool {
        self.operator.is_input()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .register_ready_callback(cb);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    #[allow(unused_variables)]
    fn checkpoint(
        &mut self,
        base: &StoragePath,
        persistent_id: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .checkpoint(base, persistent_id, files)
    }

    #[allow(unused_variables)]
    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .restore(base, persistent_id)
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().clear_state()
    }

    fn start_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().start_replay()
    }

    fn is_replay_complete(&self) -> bool {
        self.operator.is_replay_complete()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().end_replay()
    }

    fn flush(&mut self) {
        assert!(self.stream.is_none(), "flush called while stream is active");
        Rc::get_mut(&mut self.operator).unwrap().flush();
    }

    fn is_flush_complete(&self) -> bool {
        self.stream.is_none()
    }

    fn flush_progress(&self) -> Option<Position> {
        self.progress.clone()
    }
}

impl<I1, I2, I3, I4, O, Op> QuaternaryOperator<I1, I2, I3, I4, O>
    for StreamingQuaternaryWrapper<I1, I2, I3, I4, O, Op>
where
    I1: Clone + 'static,
    I2: Clone + 'static,
    I3: Clone + 'static,
    I4: Clone + 'static,
    O: 'static,
    Op: StreamingQuaternaryOperator<I1, I2, I3, I4, O> + 'static,
{
    async fn eval(
        &mut self,
        i1: Cow<'_, I1>,
        i2: Cow<'_, I2>,
        i3: Cow<'_, I3>,
        i4: Cow<'_, I4>,
    ) -> O {
        if self.stream.is_none() {
            self.stream = Some(Box::pin(self.operator.clone().eval(i1, i2, i3, i4))
                as Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>);
        }

        let stream = self.stream.as_mut().unwrap();

        let Some((output, complete, progress)) = stream.next().await else {
            panic!("StreamingQuaternaryOperator unexpectedly reached end of stream");
        };

        self.progress = progress;

        if complete {
            self.stream = None;
            output
        } else {
            output
        }
    }
}

pub trait StreamingNaryOperator<I, O>: Operator {
    fn eval<'a, Iter>(
        self: Rc<Self>,
        inputs: Iter,
    ) -> impl AsyncStream<Item = (O, bool, Option<Position>)> + 'static
    where
        I: Clone + 'static,
        Iter: Iterator<Item = Cow<'a, I>>;
}

pub struct StreamingNaryWrapper<I, O, Op> {
    operator: Rc<Op>,
    stream: Option<Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>>,
    progress: Option<Position>,
    phantom: PhantomData<fn(&I, &O)>,
}

impl<I, O, Op> StreamingNaryWrapper<I, O, Op> {
    pub fn new(operator: Op) -> Self {
        Self {
            operator: Rc::new(operator),
            stream: None,
            progress: None,
            phantom: PhantomData,
        }
    }
}

impl<I, O, Op> Operator for StreamingNaryWrapper<I, O, Op>
where
    I: 'static,
    O: 'static,
    Op: StreamingNaryOperator<I, O> + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn location(&self) -> OperatorLocation {
        self.operator.location()
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        Rc::get_mut(&mut self.operator).unwrap().init(global_id);
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        self.operator.metadata(meta);
    }

    fn clock_start(&mut self, scope: Scope) {
        Rc::get_mut(&mut self.operator).unwrap().clock_start(scope);
    }

    fn clock_end(&mut self, scope: Scope) {
        Rc::get_mut(&mut self.operator).unwrap().clock_end(scope);
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn is_input(&self) -> bool {
        self.operator.is_input()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .register_ready_callback(cb);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    #[allow(unused_variables)]
    fn checkpoint(
        &mut self,
        base: &StoragePath,
        persistent_id: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .checkpoint(base, persistent_id, files)
    }

    #[allow(unused_variables)]
    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .restore(base, persistent_id)
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().clear_state()
    }

    fn start_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().start_replay()
    }

    fn is_replay_complete(&self) -> bool {
        self.operator.is_replay_complete()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().end_replay()
    }

    fn flush(&mut self) {
        assert!(self.stream.is_none(), "flush called while stream is active");
        Rc::get_mut(&mut self.operator).unwrap().flush();
    }

    fn is_flush_complete(&self) -> bool {
        self.stream.is_none()
    }

    fn flush_progress(&self) -> Option<Position> {
        self.progress.clone()
    }
}

impl<I, O, Op> NaryOperator<I, O> for StreamingNaryWrapper<I, O, Op>
where
    I: Clone + 'static,
    O: 'static,
    Op: StreamingNaryOperator<I, O> + 'static,
{
    async fn eval<'a, Iter>(&mut self, inputs: Iter) -> O
    where
        Iter: Iterator<Item = Cow<'a, I>>,
    {
        if self.stream.is_none() {
            self.stream = Some(Box::pin(self.operator.clone().eval(inputs))
                as Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>);
        }

        let stream = self.stream.as_mut().unwrap();

        let Some((output, complete, progress)) = stream.next().await else {
            panic!("StreamingNaryOperator unexpectedly reached end of stream");
        };

        self.progress = progress;

        if complete {
            self.stream = None;
            output
        } else {
            output
        }
    }
}
