//! This module contains several wrappers that make it easier to implement splitter operators
//! that produce outputs over multiple steps. Such operators generally need to be able to stop
//! generating outputs at any point and preserve their state until the next step. This can be
//! highly error prone (this is known as the stack ripping problem).
//!
//! It's much easier to implement such operators as streaming operators using `async_stream`
//! crate's `stream!` macro, which allows yielding outputs at any point in the code.
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

use std::{any::Any, borrow::Cow, marker::PhantomData, pin::Pin, rc::Rc, sync::Arc};

use crate::{
    Error, Position, Scope,
    circuit::{
        GlobalNodeId, OwnershipPreference,
        metadata::{OperatorLocation, OperatorMeta},
        operator_traits::{
            BinaryOperator, NaryOperator, Operator, QuaternaryOperator, TernaryOperator,
            TernarySinkOperator,
        },
    },
};
use feldera_storage::{FileCommitter, StoragePath};
use futures::Stream as AsyncStream;
use futures_util::StreamExt;

pub trait StreamingBinaryOperator<I1, I2, O>: Operator
where
    I1: Clone,
    I2: Clone,
{
    /// Starts the stream that consumes one pair of inputs.
    ///
    /// The inputs arrive owned or borrowed as the circuit decides, guided by
    /// the preferences the wrapper was given (see
    /// [`StreamingBinaryWrapper::with_preferences`]).  An operator that needs
    /// an input owned asks for it that way and may refuse a borrowed one.
    fn eval(
        self: Rc<Self>,
        lhs: Cow<'_, I1>,
        rhs: Cow<'_, I2>,
    ) -> impl AsyncStream<Item = (O, bool, Option<Position>)> + 'static;
}

pub struct StreamingBinaryWrapper<I1, I2, O, Op> {
    operator: Rc<Op>,
    stream: Option<Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>>,
    progress: Option<Position>,
    /// What the operator asks of each input's ownership.
    preferences: (OwnershipPreference, OwnershipPreference),
    phantom: PhantomData<fn(&I1, &I2, &O)>,
}

impl<I1, I2, O, Op> StreamingBinaryWrapper<I1, I2, O, Op> {
    pub fn new(operator: Op) -> Self {
        Self::with_preferences(
            operator,
            (
                OwnershipPreference::INDIFFERENT,
                OwnershipPreference::INDIFFERENT,
            ),
        )
    }

    /// Wraps `operator`, asking the circuit for its inputs as `preferences`
    /// say (see [`OwnershipPreference`]).
    pub fn with_preferences(
        operator: Op,
        preferences: (OwnershipPreference, OwnershipPreference),
    ) -> Self {
        Self {
            operator: Rc::new(operator),
            stream: None,
            progress: None,
            preferences,
            phantom: PhantomData,
        }
    }
}

impl<I1, I2, O, Op> StreamingBinaryWrapper<I1, I2, O, Op>
where
    I1: Clone + 'static,
    I2: Clone + 'static,
    O: 'static,
    Op: StreamingBinaryOperator<I1, I2, O> + 'static,
{
    /// Feeds one pair of inputs to the operator's stream, starting a stream if
    /// the last one has ended.
    async fn eval_cow(&mut self, lhs: Cow<'_, I1>, rhs: Cow<'_, I2>) -> O {
        let stream = self.stream.get_or_insert_with(|| {
            Box::pin(self.operator.clone().eval(lhs, rhs))
                as Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>
        });
        let Some((output, complete, progress)) = stream.next().await else {
            panic!("StreamingBinaryOperator unexpectedly reached end of stream");
        };
        self.progress = progress;
        if complete {
            self.stream = None;
        }
        output
    }
}

impl<I1, I2, O, Op> Operator for StreamingBinaryWrapper<I1, I2, O, Op>
where
    I1: Clone + 'static,
    I2: Clone + 'static,
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

    fn start_sync_replay(&mut self, trace: Box<dyn Any>) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .start_sync_replay(trace)
    }

    fn swap_state(&mut self, other: &mut Self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .swap_state(Rc::get_mut(&mut other.operator).unwrap())
    }

    fn is_replay_complete(&self) -> bool {
        self.operator.is_replay_complete()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().end_replay()
    }

    fn start_transaction(&mut self) {
        Rc::get_mut(&mut self.operator).unwrap().start_transaction();
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
    I1: Clone + 'static,
    I2: Clone + 'static,
    O: 'static,
    Op: StreamingBinaryOperator<I1, I2, O> + 'static,
{
    async fn eval(&mut self, lhs: &I1, rhs: &I2) -> O {
        self.eval_cow(Cow::Borrowed(lhs), Cow::Borrowed(rhs)).await
    }

    async fn eval_owned(&mut self, lhs: I1, rhs: I2) -> O {
        self.eval_cow(Cow::Owned(lhs), Cow::Owned(rhs)).await
    }

    async fn eval_owned_and_ref(&mut self, lhs: I1, rhs: &I2) -> O {
        self.eval_cow(Cow::Owned(lhs), Cow::Borrowed(rhs)).await
    }

    async fn eval_ref_and_owned(&mut self, lhs: &I1, rhs: I2) -> O {
        self.eval_cow(Cow::Borrowed(lhs), Cow::Owned(rhs)).await
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        self.preferences
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

    fn start_sync_replay(&mut self, trace: Box<dyn Any>) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .start_sync_replay(trace)
    }

    fn swap_state(&mut self, other: &mut Self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .swap_state(Rc::get_mut(&mut other.operator).unwrap())
    }

    fn is_replay_complete(&self) -> bool {
        self.operator.is_replay_complete()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().end_replay()
    }

    fn start_transaction(&mut self) {
        Rc::get_mut(&mut self.operator).unwrap().start_transaction();
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
        let stream = self.stream.get_or_insert_with(|| {
            Box::pin(self.operator.clone().eval(i1, i2, i3))
                as Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>
        });

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

    fn start_sync_replay(&mut self, trace: Box<dyn Any>) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .start_sync_replay(trace)
    }

    fn swap_state(&mut self, other: &mut Self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .swap_state(Rc::get_mut(&mut other.operator).unwrap())
    }

    fn is_replay_complete(&self) -> bool {
        self.operator.is_replay_complete()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().end_replay()
    }

    fn start_transaction(&mut self) {
        Rc::get_mut(&mut self.operator).unwrap().start_transaction();
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
        let stream = self.stream.get_or_insert_with(|| {
            Box::pin(self.operator.clone().eval(i1, i2, i3, i4))
                as Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>
        });

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

    fn start_sync_replay(&mut self, trace: Box<dyn Any>) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .start_sync_replay(trace)
    }

    fn swap_state(&mut self, other: &mut Self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .swap_state(Rc::get_mut(&mut other.operator).unwrap())
    }

    fn is_replay_complete(&self) -> bool {
        self.operator.is_replay_complete()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().end_replay()
    }

    fn start_transaction(&mut self) {
        Rc::get_mut(&mut self.operator).unwrap().start_transaction();
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
        let stream = self.stream.get_or_insert_with(|| {
            Box::pin(self.operator.clone().eval(inputs))
                as Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>
        });

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

pub trait StreamingTernarySinkOperator<I1, I2, I3>: Operator
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
    ) -> impl AsyncStream<Item = (bool, Option<Position>)> + 'static;
}

pub struct StreamingTernarySinkWrapper<I1, I2, I3, Op> {
    operator: Rc<Op>,
    stream: Option<Pin<Box<dyn AsyncStream<Item = (bool, Option<Position>)>>>>,
    progress: Option<Position>,
    phantom: PhantomData<fn(&I1, &I2, &I3)>,
}

impl<I1, I2, I3, Op> StreamingTernarySinkWrapper<I1, I2, I3, Op> {
    pub fn new(operator: Op) -> Self {
        Self {
            operator: Rc::new(operator),
            stream: None,
            progress: None,
            phantom: PhantomData,
        }
    }
}

impl<I1, I2, I3, Op> Operator for StreamingTernarySinkWrapper<I1, I2, I3, Op>
where
    I1: Clone + 'static,
    I2: Clone + 'static,
    I3: Clone + 'static,
    Op: StreamingTernarySinkOperator<I1, I2, I3> + 'static,
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

    fn start_sync_replay(&mut self, trace: Box<dyn Any>) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .start_sync_replay(trace)
    }

    fn swap_state(&mut self, other: &mut Self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator)
            .unwrap()
            .swap_state(Rc::get_mut(&mut other.operator).unwrap())
    }

    fn is_replay_complete(&self) -> bool {
        self.operator.is_replay_complete()
    }

    fn end_replay(&mut self) -> Result<(), Error> {
        Rc::get_mut(&mut self.operator).unwrap().end_replay()
    }

    fn start_transaction(&mut self) {
        Rc::get_mut(&mut self.operator).unwrap().start_transaction();
    }

    fn flush(&mut self) {
        assert!(self.stream.is_none(), "flush called while stream is active");
        Rc::get_mut(&mut self.operator).unwrap().flush();
    }

    fn is_flush_complete(&self) -> bool {
        // println!(
        //     "{} is_flush_complete: {:?}",
        //     Runtime::worker_index(),
        //     self.stream.is_none()
        // );
        //self.stream.is_none()

        // Unlike all other operators, this operator doesn't assume that is_flush_complete is equivalent
        // to self.stream.is_none(). This is based on how this operator is used with RebalancingExchangeSender.
        // operator.
        // TODO: change other operators to forward is_flush_complete to their inner operator too.
        self.operator.is_flush_complete()
    }

    fn flush_progress(&self) -> Option<Position> {
        self.progress.clone()
    }
}

impl<I1, I2, I3, Op> TernarySinkOperator<I1, I2, I3> for StreamingTernarySinkWrapper<I1, I2, I3, Op>
where
    I1: Clone + 'static,
    I2: Clone + 'static,
    I3: Clone + 'static,
    Op: StreamingTernarySinkOperator<I1, I2, I3> + 'static,
{
    async fn eval(&mut self, i1: Cow<'_, I1>, i2: Cow<'_, I2>, i3: Cow<'_, I3>) {
        let stream = self.stream.get_or_insert_with(|| {
            Box::pin(self.operator.clone().eval(i1, i2, i3))
                as Pin<Box<dyn AsyncStream<Item = (bool, Option<Position>)>>>
        });

        let Some((complete, progress)) = stream.next().await else {
            panic!("StreamingTernarySinkOperator unexpectedly reached end of stream");
        };

        self.progress = progress;

        if complete {
            self.stream = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    /// Reports, per input, whether it arrived owned.
    struct Probe;

    impl Operator for Probe {
        fn name(&self) -> Cow<'static, str> {
            Cow::Borrowed("Probe")
        }

        fn fixedpoint(&self, _scope: Scope) -> bool {
            true
        }
    }

    impl StreamingBinaryOperator<u32, u32, (bool, bool)> for Probe {
        fn eval(
            self: Rc<Self>,
            lhs: Cow<'_, u32>,
            rhs: Cow<'_, u32>,
        ) -> impl AsyncStream<Item = ((bool, bool), bool, Option<Position>)> + 'static {
            let owned = (matches!(lhs, Cow::Owned(_)), matches!(rhs, Cow::Owned(_)));
            futures::stream::once(async move { (owned, true, None) })
        }
    }

    type ProbeWrapper = StreamingBinaryWrapper<u32, u32, (bool, bool), Probe>;

    /// The wrapper reports the preferences it was given and hands each input
    /// on with the ownership the circuit chose.
    #[test]
    fn the_wrapper_keeps_preferences_and_ownership() {
        let preferences = (
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::STRONGLY_PREFER_OWNED,
        );
        let mut wrapper = ProbeWrapper::with_preferences(Probe, preferences);
        assert_eq!(wrapper.input_preference(), preferences);
        assert_eq!(
            ProbeWrapper::new(Probe).input_preference(),
            (
                OwnershipPreference::INDIFFERENT,
                OwnershipPreference::INDIFFERENT
            )
        );
        block_on(async {
            assert_eq!(wrapper.eval(&1, &2).await, (false, false));
            assert_eq!(wrapper.eval_owned(1, 2).await, (true, true));
            assert_eq!(wrapper.eval_owned_and_ref(1, &2).await, (true, false));
            assert_eq!(wrapper.eval_ref_and_owned(&1, 2).await, (false, true));
        });
    }
}
