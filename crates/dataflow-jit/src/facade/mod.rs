mod tests;

use crate::{
    codegen::{
        json::{call_deserialize_fn, DeserializeJsonFn, JsonDeserConfig},
        CodegenConfig, NativeLayout, NativeLayoutCache, VTable,
    },
    dataflow::{CompiledDataflow, JitHandle, RowInput, RowOutput},
    ir::{
        literal::{NullableConstant, RowLiteral, StreamCollection},
        nodes::StreamLayout,
        pretty::{Arena, Pretty, DEFAULT_WIDTH},
        ColumnType, Constant, Graph, GraphExt, LayoutId, NodeId, RowLayout, Validator,
    },
    row::{row_from_literal, Row, UninitRow},
    thin_str::ThinStrRef,
};
use chrono::{TimeZone, Utc};
use cranelift_module::FuncId;
use csv::StringRecord;
use dbsp::{
    trace::{BatchReader, Cursor},
    CollectionHandle, DBSPHandle, Error, Runtime,
};
use rust_decimal::Decimal;
use serde_json::{Deserializer, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    error,
    io::Read,
    mem::transmute,
    ops::Not,
    path::Path,
    thread,
    time::Instant,
};

// TODO: A lot of this still needs fleshing out, mainly the little tweaks that
// users may want to add to parsing and how to do that ergonomically.
// We also need checks to make sure that the type is being fully initialized, as
// well as support for parsing maps from csv

pub struct Demands {
    #[allow(clippy::type_complexity)]
    csv: BTreeMap<LayoutId, Vec<(usize, usize, Option<String>)>>,
    json_deser: BTreeMap<LayoutId, JsonDeserConfig>,
}

impl Demands {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            csv: BTreeMap::new(),
            json_deser: BTreeMap::new(),
        }
    }

    pub fn add_csv_deserialize(
        &mut self,
        layout: LayoutId,
        column_mappings: Vec<(usize, usize, Option<String>)>,
    ) {
        let displaced = self.csv.insert(layout, column_mappings);
        assert_eq!(displaced, None);
    }

    pub fn add_json_deserialize(&mut self, layout: LayoutId, mappings: JsonDeserConfig) {
        let displaced = self.json_deser.insert(layout, mappings);
        assert_eq!(displaced, None);
    }

    // TODO: Return result
    fn validate(&self) {
        let mut destination_columns = BTreeSet::new();
        for (&layout_id, csv_columns) in &self.csv {
            for &(csv_column, row_column, ref fmt) in csv_columns {
                if !destination_columns.insert(row_column) {
                    panic!(
                        "multiple csv columns write to the same row column for \
                         layout {layout_id}, `[{csv_column}, {row_column}, {fmt:?}]`"
                    );
                }
            }

            destination_columns.clear();
        }
    }
}

#[derive(Clone)]
pub struct JsonSetHandle {
    handle: CollectionHandle<Row, i32>,
    deserialize_fn: DeserializeJsonFn,
    vtable: &'static VTable,
}

impl JsonSetHandle {
    pub fn new(
        handle: CollectionHandle<Row, i32>,
        deserialize_fn: DeserializeJsonFn,
        vtable: &'static VTable,
    ) -> Self {
        Self {
            handle,
            deserialize_fn,
            vtable,
        }
    }

    pub fn push(&self, key: &[u8], weight: i32) -> Result<(), Box<dyn error::Error>> {
        let value: Value = serde_json::from_slice(key)?;
        let key = unsafe {
            let mut uninit = UninitRow::new(self.vtable);
            call_deserialize_fn(self.deserialize_fn, uninit.as_mut_ptr(), &value)?;
            uninit.assume_init()
        };

        self.handle.push(key, weight);

        Ok(())
    }

    pub fn clear_input(&self) {
        self.handle.clear_input();
    }
}

pub struct DbspCircuit {
    jit: JitHandle,
    runtime: DBSPHandle,
    /// The input handles of all source nodes, will be `None` if the source is
    /// unused
    inputs: BTreeMap<NodeId, (Option<RowInput>, StreamLayout)>,
    /// The output handles of all sink nodes, will be `None` if the sink is
    /// unreachable
    outputs: BTreeMap<NodeId, (Option<RowOutput>, StreamLayout)>,
    csv_demands: BTreeMap<LayoutId, FuncId>,
    json_deser_demands: BTreeMap<LayoutId, FuncId>,
    layout_cache: NativeLayoutCache,
}

impl DbspCircuit {
    pub fn new(
        mut graph: Graph,
        optimize: bool,
        workers: usize,
        config: CodegenConfig,
        demands: Demands,
    ) -> Self {
        let arena = Arena::<()>::new();
        tracing::trace!(
            "created circuit from graph:\n{}",
            Pretty::pretty(&graph, &arena, graph.layout_cache()).pretty(DEFAULT_WIDTH),
        );

        let sources = graph.source_nodes();
        let sinks = graph.sink_nodes();

        {
            demands.validate();

            let mut validator = Validator::new(graph.layout_cache().clone());
            validator
                .validate_graph(&graph)
                .expect("failed to validate graph before optimization");

            if optimize {
                graph.optimize();
                tracing::trace!("optimized graph for dbsp circuit: {graph:#?}");

                validator
                    .validate_graph(&graph)
                    .expect("failed to validate graph after optimization");

                tracing::trace!(
                    "optimized graph:\n{}",
                    Pretty::pretty(&graph, &arena, graph.layout_cache()).pretty(DEFAULT_WIDTH),
                );
            }
        }

        let (mut json_deser_demands, mut csv_demands) = (BTreeMap::new(), BTreeMap::new());
        let (dataflow, jit, layout_cache) = CompiledDataflow::new(&graph, config, |codegen| {
            json_deser_demands = demands
                .json_deser
                .into_iter()
                .map(|(layout, mappings)| {
                    debug_assert_eq!(layout, mappings.layout);
                    let from_json = codegen.deserialize_json(&mappings);
                    (layout, from_json)
                })
                .collect();

            csv_demands = demands
                .csv
                .into_iter()
                .map(|(layout, mappings)| {
                    let from_csv = codegen.codegen_layout_from_csv(layout, &mappings);
                    (layout, from_csv)
                })
                .collect();
        });

        let (runtime, (inputs, outputs)) =
            Runtime::init_circuit(workers, move |circuit| dataflow.construct(circuit))
                .expect("failed to construct runtime");

        // Account for unused sources
        let mut inputs: BTreeMap<_, _> = inputs
            .into_iter()
            .map(|(id, (input, layout))| (id, (Some(input), layout)))
            .collect();
        for (source, layout) in sources {
            inputs.entry(source).or_insert((None, layout));
        }

        // Account for unreachable sinks
        let mut outputs: BTreeMap<_, _> = outputs
            .into_iter()
            .map(|(id, (output, layout))| (id, (Some(output), layout)))
            .collect();
        for (sink, layout) in sinks {
            outputs.entry(sink).or_insert((None, layout));
        }

        Self {
            jit,
            runtime,
            inputs,
            outputs,
            csv_demands,
            json_deser_demands,
            layout_cache,
        }
    }

    pub fn step(&mut self) -> Result<(), Error> {
        tracing::info!("stepping circuit");
        let start = Instant::now();

        let result = self.runtime.step();

        let elapsed = start.elapsed();
        tracing::info!(
            "step took {elapsed:#?} and finished {}successfully",
            if result.is_err() { "un" } else { "" },
        );

        result
    }

    pub fn kill(self) -> thread::Result<()> {
        tracing::trace!("killing circuit");
        let result = self.runtime.kill();

        drop(self.inputs);
        drop(self.outputs);
        unsafe { self.jit.free_memory() };

        result
    }

    /// Creates a new [`JsonSetHandle`] for ingesting json
    ///
    /// Returns [`None`] if the target source node is unreachable
    ///
    /// # Safety
    ///
    /// The produced `JsonSetHandle` must be dropped before the [`DbspCircuit`]
    /// that created it, using the handle after the parent circuit has shut down
    /// is undefined behavior
    // TODO: We should probably wrap the innards of `DbspCircuit` in a struct
    // and arc and handles should hold a reference to that (maybe even a weak ref).
    // Alternatively we could use lifetimes, but I'm not 100% sure how that would
    // interact with consumers
    pub unsafe fn json_input_set(&mut self, target: NodeId) -> Option<JsonSetHandle> {
        let (input, layout) = self.inputs.get(&target).unwrap_or_else(|| {
            panic!("attempted to append to {target}, but {target} is not a source node or doesn't exist");
        });
        let layout = layout.as_set().unwrap_or_else(|| {
            panic!(
                "called `DbspCircuit::json_input_set()` on node {target} which is a map, not a set",
            )
        });

        let handle = input.as_ref()?.as_set().unwrap().clone();
        let vtable = unsafe { &*self.jit.vtables()[&layout] };
        let deserialize_fn = unsafe {
            transmute::<_, DeserializeJsonFn>(
                self.jit
                    .jit
                    .get_finalized_function(self.json_deser_demands[&layout]),
            )
        };

        Some(JsonSetHandle::new(handle, deserialize_fn, vtable))
    }

    pub fn append_input(&mut self, target: NodeId, data: &StreamCollection) {
        let (input, layout) = self.inputs.get_mut(&target).unwrap_or_else(|| {
            panic!("attempted to append to {target}, but {target} is not a source node or doesn't exist");
        });

        if let Some(input) = input {
            match data {
                StreamCollection::Set(set) => {
                    tracing::trace!("appending a set with {} values to {target}", set.len());

                    let key_layout = layout.unwrap_set();
                    let key_vtable = unsafe { &*self.jit.vtables()[&key_layout] };
                    let key_layout = self.layout_cache.layout_of(key_layout);

                    let mut batch = Vec::with_capacity(set.len());
                    for (literal, diff) in set {
                        let key = unsafe { row_from_literal(literal, key_vtable, &key_layout) };
                        batch.push((key, *diff));
                    }

                    input.as_set_mut().unwrap().append(&mut batch);
                }

                StreamCollection::Map(map) => {
                    tracing::trace!("appending a map with {} values to {target}", map.len());

                    let (key_layout, value_layout) = layout.unwrap_map();
                    let (key_vtable, value_vtable) = unsafe {
                        (
                            &*self.jit.vtables()[&key_layout],
                            &*self.jit.vtables()[&value_layout],
                        )
                    };
                    let (key_layout, value_layout) = (
                        self.layout_cache.layout_of(key_layout),
                        self.layout_cache.layout_of(value_layout),
                    );

                    let mut batch = Vec::with_capacity(map.len());
                    for (key_literal, value_literal, diff) in map {
                        let key = unsafe { row_from_literal(key_literal, key_vtable, &key_layout) };
                        let value =
                            unsafe { row_from_literal(value_literal, value_vtable, &value_layout) };
                        batch.push((key, (value, *diff)));
                    }

                    input.as_map_mut().unwrap().append(&mut batch);
                }
            }

        // If the source is unused, do nothing
        } else {
            tracing::info!("appended csv file to source {target} which is unused, doing nothing");
        }
    }

    // TODO: We probably want other ways to ingest json, e.g. `&[u8]`, `R: Read`,
    // etc.
    pub fn append_json_input<R>(
        &mut self,
        target: NodeId,
        json: R,
    ) -> Result<(), Box<dyn error::Error>>
    where
        R: Read,
    {
        let (input, layout) = self.inputs.get_mut(&target).unwrap_or_else(|| {
            panic!("attempted to append to {target}, but {target} is not a source node or doesn't exist");
        });

        if let Some(input) = input {
            let start = Instant::now();

            let records = match *layout {
                StreamLayout::Set(key_layout) => {
                    let key_vtable = unsafe { &*self.jit.vtables()[&key_layout] };
                    let deserialize_json = unsafe {
                        transmute::<_, DeserializeJsonFn>(
                            self.jit
                                .jit
                                .get_finalized_function(self.json_deser_demands[&key_layout]),
                        )
                    };

                    let mut batch = Vec::new();
                    let stream = Deserializer::from_reader(json).into_iter::<Value>();
                    for value in stream {
                        let value = value?;
                        let mut row = UninitRow::new(key_vtable);
                        unsafe { call_deserialize_fn(deserialize_json, row.as_mut_ptr(), &value)? }

                        batch.push((unsafe { row.assume_init() }, 1));
                    }

                    let records = batch.len();
                    input.as_set_mut().unwrap().append(&mut batch);
                    records
                }

                StreamLayout::Map(..) => todo!(),
            };

            let elapsed = start.elapsed();
            tracing::info!("ingested {records} records for {target} in {elapsed:#?}");

        // If the source is unused, do nothing
        } else {
            tracing::info!("appended json to source {target} which is unused, doing nothing");
        }

        Ok(())
    }

    pub fn append_json_record(
        &mut self,
        target: NodeId,
        record: &[u8],
    ) -> Result<(), Box<dyn error::Error>> {
        let (input, layout) = self.inputs.get_mut(&target).unwrap_or_else(|| {
            panic!("attempted to append to {target}, but {target} is not a source node or doesn't exist");
        });

        if let Some(input) = input {
            let start = Instant::now();

            match *layout {
                StreamLayout::Set(key_layout) => {
                    let key_vtable = unsafe { &*self.jit.vtables()[&key_layout] };
                    let deserialize_json = unsafe {
                        transmute::<_, DeserializeJsonFn>(
                            self.jit
                                .jit
                                .get_finalized_function(self.json_deser_demands[&key_layout]),
                        )
                    };

                    let value = serde_json::from_slice::<Value>(record)?;
                    let mut row = UninitRow::new(key_vtable);
                    unsafe { call_deserialize_fn(deserialize_json, row.as_mut_ptr(), &value)? }

                    input
                        .as_set_mut()
                        .unwrap()
                        .push(unsafe { row.assume_init() }, 1);
                }

                StreamLayout::Map(..) => todo!(),
            }

            let elapsed = start.elapsed();
            tracing::info!("ingested 1 record for {target} in {elapsed:#?}");

        // If the source is unused, do nothing
        } else {
            tracing::info!("appended json to source {target} which is unused, doing nothing");
        }

        Ok(())
    }

    pub fn append_csv_input(&mut self, target: NodeId, path: &Path) {
        let (input, layout) = self.inputs.get_mut(&target).unwrap_or_else(|| {
            panic!("attempted to append to {target}, but {target} is not a source node or doesn't exist");
        });

        if let Some(input) = input {
            let mut csv = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_path(path)
                .unwrap();

            let start = Instant::now();

            let records = match *layout {
                StreamLayout::Set(key_layout) => {
                    let key_vtable = unsafe { &*self.jit.vtables()[&key_layout] };
                    let marshall_csv = unsafe {
                        transmute::<_, unsafe extern "C" fn(*mut u8, *const StringRecord)>(
                            self.jit
                                .jit
                                .get_finalized_function(self.csv_demands[&key_layout]),
                        )
                    };

                    let (mut batch, mut buf) = (Vec::new(), StringRecord::new());
                    while csv.read_record(&mut buf).unwrap() {
                        let mut row = UninitRow::new(key_vtable);
                        unsafe { marshall_csv(row.as_mut_ptr(), &buf) };
                        batch.push((unsafe { row.assume_init() }, 1));
                    }

                    let records = batch.len();
                    input.as_set_mut().unwrap().append(&mut batch);
                    records
                }

                StreamLayout::Map(..) => todo!(),
            };

            let elapsed = start.elapsed();
            tracing::info!("ingested {records} records for {target} in {elapsed:#?}");

        // If the source is unused, do nothing
        } else {
            tracing::info!("appended csv file to source {target} which is unused, doing nothing");
        }
    }

    pub fn consolidate_output(&mut self, output: NodeId) -> StreamCollection {
        let (output, layout) = self.outputs.get(&output).unwrap_or_else(|| {
            panic!("attempted to consolidate data from {output}, but {output} is not a sink node or doesn't exist");
        });

        if let Some(output) = output {
            match output {
                RowOutput::Set(output) => {
                    let key_layout = layout.unwrap_set();
                    let (native_key_layout, key_layout) = self.layout_cache.get_layouts(key_layout);

                    let set = output.consolidate();
                    // println!("{set}");
                    let mut contents = Vec::with_capacity(set.len());

                    let mut cursor = set.cursor();
                    while cursor.key_valid() {
                        let diff = cursor.weight();
                        let key = cursor.key();

                        let key =
                            unsafe { row_literal_from_row(key, &native_key_layout, &key_layout) };
                        contents.push((key, diff));

                        cursor.step_key();
                    }

                    StreamCollection::Set(contents)
                }

                RowOutput::Map(output) => {
                    let (key_layout, value_layout) = layout.unwrap_map();
                    let (native_key_layout, key_layout) = self.layout_cache.get_layouts(key_layout);
                    let (native_value_layout, value_layout) =
                        self.layout_cache.get_layouts(value_layout);

                    let map = output.consolidate();
                    let mut contents = Vec::with_capacity(map.len());

                    let mut cursor = map.cursor();
                    while cursor.key_valid() {
                        let diff = cursor.weight();
                        let key = cursor.key();

                        let key_literal =
                            unsafe { row_literal_from_row(key, &native_key_layout, &key_layout) };

                        while cursor.val_valid() {
                            let value = cursor.val();
                            let value_literal = unsafe {
                                row_literal_from_row(value, &native_value_layout, &value_layout)
                            };

                            cursor.step_val();

                            if cursor.val_valid() {
                                contents.push((key_literal.clone(), value_literal, diff));

                            // Don't clone the key value if this is the last
                            // value
                            } else {
                                contents.push((key_literal, value_literal, diff));
                                break;
                            }
                        }

                        cursor.step_key();
                    }

                    StreamCollection::Map(contents)
                }
            }

        // The output is unreachable so we always return an empty stream
        } else {
            tracing::info!(
                "consolidating output from an unreachable sink, returning an empty stream",
            );
            StreamCollection::empty(*layout)
        }
    }
}

unsafe fn row_literal_from_row(row: &Row, native: &NativeLayout, layout: &RowLayout) -> RowLiteral {
    let mut literal = Vec::with_capacity(layout.len());
    for column in 0..layout.len() {
        let value = if layout.column_nullable(column) {
            NullableConstant::Nullable(
                row.column_is_null(column, native)
                    .not()
                    .then(|| unsafe { constant_from_column(column, row, native, layout) }),
            )
        } else {
            NullableConstant::NonNull(unsafe { constant_from_column(column, row, native, layout) })
        };

        literal.push(value);
    }

    RowLiteral::new(literal)
}

unsafe fn constant_from_column(
    column: usize,
    row: &Row,
    native: &NativeLayout,
    layout: &RowLayout,
) -> Constant {
    let ptr = unsafe { row.as_ptr().add(native.offset_of(column) as usize) };

    match layout.column_type(column) {
        ColumnType::Unit => Constant::Unit,
        ColumnType::U8 => Constant::U8(ptr.cast::<u8>().read()),
        ColumnType::I8 => Constant::I8(ptr.cast::<i8>().read()),
        ColumnType::U16 => Constant::U16(ptr.cast::<u16>().read()),
        ColumnType::I16 => Constant::I16(ptr.cast::<i16>().read()),
        ColumnType::U32 => Constant::U32(ptr.cast::<u32>().read()),
        ColumnType::I32 => Constant::I32(ptr.cast::<i32>().read()),
        ColumnType::U64 => Constant::U64(ptr.cast::<u64>().read()),
        ColumnType::I64 => Constant::I64(ptr.cast::<i64>().read()),
        ColumnType::Usize => Constant::Usize(ptr.cast::<usize>().read()),
        ColumnType::Isize => Constant::Isize(ptr.cast::<isize>().read()),
        ColumnType::F32 => Constant::F32(ptr.cast::<f32>().read()),
        ColumnType::F64 => Constant::F64(ptr.cast::<f64>().read()),
        ColumnType::Bool => Constant::Bool(ptr.cast::<bool>().read()),

        ColumnType::Date => Constant::Date(
            Utc.timestamp_opt(ptr.cast::<i32>().read() as i64 * 86400, 0)
                .unwrap()
                .date_naive(),
        ),
        ColumnType::Timestamp => Constant::Timestamp(
            Utc.timestamp_millis_opt(ptr.cast::<i64>().read())
                .unwrap()
                .naive_utc(),
        ),

        ColumnType::String => Constant::String(ptr.cast::<ThinStrRef>().read().to_string()),

        ColumnType::Decimal => Constant::Decimal(Decimal::deserialize(
            ptr.cast::<u128>().read().to_le_bytes(),
        )),

        ColumnType::Ptr => todo!(),
    }
}
