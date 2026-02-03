use crate::{
    OutputHandle,
    trace::{Batch, merge_batches},
};

impl<T> OutputHandle<T>
where
    T: Batch<Time = ()> + Send,
{
    /// See [`OutputHandle::consolidate`].
    pub fn dyn_consolidate(&self, factories: &T::Factories) -> T {
        merge_batches(factories, self.take_from_all(), &None, &None)
    }
}
