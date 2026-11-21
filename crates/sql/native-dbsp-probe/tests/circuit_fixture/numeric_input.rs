use dbsp::{DBData, OrdZSet, RootCircuit, Stream, operator::InputHandle, utils::Tup2};
use feldera_native_probe::{
    ArchiveField, ArchiveInteger, ArchiveLayout, ArchivedBatch, CompiledArchive,
};
use std::sync::{Arc, Weak};

pub(super) struct NumericBatch<const N: usize> {
    descriptor: Arc<CompiledArchive>,
    archive: ArchivedBatch,
    weights: Vec<i64>,
}

impl<const N: usize> std::fmt::Debug for NumericBatch<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NumericBatch")
            .field("columns", &N)
            .field("rows", &self.weights.len())
            .finish()
    }
}

impl<const N: usize> NumericBatch<N> {
    pub(super) fn new(
        fields: [(ArchiveInteger, bool); N],
        rows: impl IntoIterator<Item = ([Option<i128>; N], i64)>,
    ) -> Self {
        // Use fixed slots at the input boundary. The compiled layout validates
        // payload widths and NULL tags independently of any Rust tuple type.
        let fields = fields
            .into_iter()
            .enumerate()
            .map(|(index, (integer, nullable))| ArchiveField {
                integer,
                payload_offset: index * 32 + 16,
                null_tag_offset: nullable.then_some(index * 32),
            })
            .collect();
        let layout = ArchiveLayout::new(N * 32, fields).unwrap();
        let descriptor = Arc::new(CompiledArchive::compile(layout).unwrap());
        let (values, weights): (Vec<_>, Vec<_>) = rows
            .into_iter()
            .map(|(values, weight)| (Vec::from(values), weight))
            .unzip();
        let archive = descriptor.encode_wide(&values).unwrap();
        Self {
            descriptor,
            archive,
            weights,
        }
    }

    pub(super) fn owner(&self) -> Weak<CompiledArchive> {
        Arc::downgrade(&self.descriptor)
    }

    fn convert<T: DBData>(&self, decode: &impl Fn([Option<i128>; N]) -> T) -> OrdZSet<T> {
        let updates = self.archive.rows().zip(&self.weights).map(|(row, weight)| {
            let fields = self.descriptor.decode_row(row).unwrap().try_into().unwrap();
            Tup2(decode(fields), *weight)
        });
        OrdZSet::from_keys((), updates.collect())
    }
}

pub(super) type NumericInput<const N: usize> = InputHandle<Option<Arc<NumericBatch<N>>>>;

pub(super) fn source<const N: usize, T: DBData>(
    circuit: &RootCircuit,
    decode: impl Fn([Option<i128>; N]) -> T + 'static,
) -> (Stream<RootCircuit, OrdZSet<T>>, NumericInput<N>) {
    let (input, handle) = circuit.add_input_stream::<Option<Arc<NumericBatch<N>>>>();
    let input = input.apply(move |batch| {
        batch
            .as_ref()
            .map(|batch| batch.convert(&decode))
            .unwrap_or_else(OrdZSet::empty)
    });
    (input, handle)
}
