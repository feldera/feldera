use crate::{trace::BatchReaderFactories, typed_batch::Spine, Batch, Circuit, Stream};

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    pub fn accumulate(&self) -> Stream<C, Option<Spine<B>>> {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, B::R>();

        let result = self.inner().dyn_accumulate(&factories);

        unsafe { result.transmute_payload() }
    }
}
