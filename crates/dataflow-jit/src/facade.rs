use crate::{
    codegen::{CodegenConfig, NativeLayout, NativeLayoutCache},
    dataflow::{CompiledDataflow, JitHandle, RowInput, RowOutput},
    ir::{
        literal::{NullableConstant, RowLiteral, StreamCollection},
        nodes::StreamLayout,
        ColumnType, Constant, Graph, GraphExt, NodeId, RowLayout, Validator,
    },
    row::{row_from_literal, Row},
    thin_str::ThinStrRef,
};
use dbsp::{
    trace::{BatchReader, Cursor},
    DBSPHandle, Error, Runtime,
};
use std::{collections::BTreeMap, ops::Not, thread, time::Instant};

pub struct DbspCircuit {
    jit: JitHandle,
    runtime: DBSPHandle,
    inputs: BTreeMap<NodeId, (RowInput, StreamLayout)>,
    outputs: BTreeMap<NodeId, (RowOutput, StreamLayout)>,
    layout_cache: NativeLayoutCache,
}

impl DbspCircuit {
    pub fn new(mut graph: Graph, optimize: bool, workers: usize, config: CodegenConfig) -> Self {
        {
            let mut validator = Validator::new(graph.layout_cache().clone());
            validator
                .validate_graph(&graph)
                .expect("failed to validate graph before optimization");

            if optimize {
                graph.optimize();
                validator
                    .validate_graph(&graph)
                    .expect("failed to validate graph after optimization");
            }
        }

        let (dataflow, jit, layout_cache) = CompiledDataflow::new(&graph, config);

        let (runtime, (inputs, outputs)) =
            Runtime::init_circuit(workers, move |circuit| dataflow.construct(circuit))
                .expect("failed to construct runtime");

        Self {
            jit,
            runtime,
            inputs,
            outputs,
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

    pub fn append_input(&mut self, target: NodeId, data: &StreamCollection) {
        let (input, layout) = self.inputs.get_mut(&target).unwrap();

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
    }

    pub fn consolidate_output(&mut self, output: NodeId) -> StreamCollection {
        let (output, layout) = &self.outputs[&output];

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

                    let key = unsafe { row_literal_from_row(key, &native_key_layout, &key_layout) };
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

                        // Don't clone the key value if this is the last value
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

        // FIXME: Date & timestamp constants
        ColumnType::Date => Constant::I32(ptr.cast::<i32>().read()),
        ColumnType::Timestamp => Constant::I64(ptr.cast::<i64>().read()),

        ColumnType::String => Constant::String(ptr.cast::<ThinStrRef>().read().to_string()),
        ColumnType::Ptr => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        codegen::CodegenConfig,
        ir::{
            literal::{NullableConstant, RowLiteral, StreamCollection},
            Constant, NodeId,
        },
        sql_graph::SqlGraph,
        utils, DbspCircuit,
    };
    use chrono::{NaiveDate, NaiveDateTime};
    use csv::ReaderBuilder;
    use std::path::Path;

    #[test]
    fn time_series_enrich_e2e() {
        utils::test_logger();

        // Deserialize the graph from json
        let graph = serde_json::from_str::<SqlGraph>(TIME_SERIES_ENRICH_SRC)
            .unwrap()
            .rematerialize();

        // Create the circuit
        let mut circuit = DbspCircuit::new(graph, true, 1, CodegenConfig::debug());

        // Ingest data
        circuit.append_input(TRANSACTIONS_ID, &transactions());
        circuit.append_input(DEMOGRAPHICS_ID, &demographics());

        // Step the circuit
        circuit.step().unwrap();

        // TODO: Inspect outputs

        let _output = circuit.consolidate_output(SINK_ID);

        // Shut down the circuit
        circuit.kill().unwrap();
    }

    const PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../demo/project_demo01-TimeSeriesEnrich",
    );

    fn transactions() -> StreamCollection {
        let tsx_path = Path::new(PATH).join("transactions_20K.csv");
        println!("{}", tsx_path.display());

        let mut transactions = Vec::new();

        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_path(tsx_path)
            .unwrap();
        for tsx in reader.records() {
            let tsx = tsx.unwrap();

            let trans_date_trans_time = NaiveDateTime::parse_from_str(tsx.get(0).unwrap(), "%F %T")
                .unwrap()
                .timestamp_millis();
            let cc_num = tsx.get(1).unwrap().parse::<u64>().unwrap() as f64;

            let row = vec![
                NullableConstant::NonNull(Constant::I64(trans_date_trans_time)),
                NullableConstant::NonNull(Constant::F64(cc_num)),
                NullableConstant::Nullable(Some(Constant::String(tsx.get(2).unwrap().to_owned()))),
                NullableConstant::Nullable(Some(Constant::String(tsx.get(3).unwrap().to_owned()))),
                NullableConstant::Nullable(Some(Constant::F64(
                    tsx.get(4).unwrap().parse().unwrap(),
                ))),
                NullableConstant::Nullable(Some(Constant::String(tsx.get(5).unwrap().to_owned()))),
                NullableConstant::Nullable(Some(Constant::I32(
                    tsx.get(6).unwrap().parse().unwrap(),
                ))),
                NullableConstant::Nullable(Some(Constant::F64(
                    tsx.get(7).unwrap().parse().unwrap(),
                ))),
                NullableConstant::Nullable(Some(Constant::F64(
                    tsx.get(8).unwrap().parse().unwrap(),
                ))),
                NullableConstant::Nullable(Some(Constant::I32(
                    tsx.get(9).unwrap().parse().unwrap(),
                ))),
            ];

            transactions.push((RowLiteral::new(row), 1));
        }

        tracing::info!("loaded {} transactions", transactions.len());
        StreamCollection::Set(transactions)
    }

    fn demographics() -> StreamCollection {
        let demo_path = Path::new(PATH).join("demographics.csv");

        let mut demographics = Vec::new();

        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_path(demo_path)
            .unwrap();
        for demo in reader.records() {
            let demo = demo.unwrap();

            let cc_num = demo.get(0).unwrap().parse::<u64>().unwrap() as f64;
            let dob = NaiveDate::parse_from_str(demo.get(11).unwrap(), "%F")
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .timestamp_millis()
                / (86400 * 1000);

            let row = vec![
                NullableConstant::NonNull(Constant::F64(cc_num)),
                NullableConstant::Nullable(Some(Constant::String(demo.get(1).unwrap().to_owned()))),
                NullableConstant::Nullable(Some(Constant::String(demo.get(2).unwrap().to_owned()))),
                NullableConstant::Nullable(Some(Constant::String(demo.get(3).unwrap().to_owned()))),
                NullableConstant::Nullable(Some(Constant::String(demo.get(4).unwrap().to_owned()))),
                NullableConstant::Nullable(Some(Constant::String(demo.get(5).unwrap().to_owned()))),
                NullableConstant::Nullable(Some(Constant::I32(
                    demo.get(6).unwrap().parse().unwrap(),
                ))),
                NullableConstant::Nullable(Some(Constant::F64(
                    demo.get(7).unwrap().parse().unwrap(),
                ))),
                NullableConstant::Nullable(Some(Constant::F64(
                    demo.get(8).unwrap().parse().unwrap(),
                ))),
                NullableConstant::Nullable(Some(Constant::I32(
                    demo.get(9).unwrap().parse().unwrap(),
                ))),
                NullableConstant::Nullable(Some(Constant::String(
                    demo.get(10).unwrap().to_owned(),
                ))),
                NullableConstant::Nullable(Some(Constant::I32(dob as i32))),
            ];

            demographics.push((RowLiteral::new(row), 1));
        }

        tracing::info!("loaded {} demographics", demographics.len());
        StreamCollection::Set(demographics)
    }

    const TRANSACTIONS_ID: NodeId = NodeId::new(54);
    const DEMOGRAPHICS_ID: NodeId = NodeId::new(68);
    const SINK_ID: NodeId = NodeId::new(273);

    const TIME_SERIES_ENRICH_SRC: &str = r#"{
    "nodes": {
        "54": {
            "Source": {
                "layout": 1,
                "table": "TRANSACTIONS"
            }
        },
        "68": {
            "Source": {
                "layout": 2,
                "table": "DEMOGRAPHICS"
            }
        },
        "248": {
            "IndexWith": {
                "input": 54,
                "index_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 1,
                            "flags": "input"
                        },
                        {
                            "id": 2,
                            "layout": 3,
                            "flags": "output"
                        },
                        {
                            "id": 3,
                            "layout": 1,
                            "flags": "output"
                        }
                    ],
                    "ret": "Unit",
                    "entry_block": 1,
                    "blocks": {
                        "1": {
                            "id": 1,
                            "body": [
                                [
                                    4,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 1,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    5,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 3,
                                            "column": 0,
                                            "value": {
                                                "Expr": 4
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    6,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 0,
                                            "column_type": "Timestamp"
                                        }
                                    }
                                ],
                                [
                                    7,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 0,
                                            "value": {
                                                "Expr": 6
                                            },
                                            "value_type": "Timestamp"
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 1,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 1,
                                            "value": {
                                                "Expr": 8
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 2,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 4,
                                            "column": 2
                                        }
                                    }
                                ],
                                [
                                    200,
                                    {
                                        "Copy": {
                                            "value": 10,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    12,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 2,
                                            "value": {
                                                "Expr": 200
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    13,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 2,
                                            "is_null": {
                                                "Expr": 11
                                            }
                                        }
                                    }
                                ],
                                [
                                    14,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 3,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    15,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 4,
                                            "column": 3
                                        }
                                    }
                                ],
                                [
                                    201,
                                    {
                                        "Copy": {
                                            "value": 14,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    16,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 3,
                                            "value": {
                                                "Expr": 201
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    17,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 3,
                                            "is_null": {
                                                "Expr": 15
                                            }
                                        }
                                    }
                                ],
                                [
                                    18,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 4,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    19,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 4,
                                            "column": 4
                                        }
                                    }
                                ],
                                [
                                    20,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 4,
                                            "value": {
                                                "Expr": 18
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    21,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 4,
                                            "is_null": {
                                                "Expr": 19
                                            }
                                        }
                                    }
                                ],
                                [
                                    22,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 5,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    23,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 4,
                                            "column": 5
                                        }
                                    }
                                ],
                                [
                                    202,
                                    {
                                        "Copy": {
                                            "value": 22,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    24,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 5,
                                            "value": {
                                                "Expr": 202
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    25,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 5,
                                            "is_null": {
                                                "Expr": 23
                                            }
                                        }
                                    }
                                ],
                                [
                                    26,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 6,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    27,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 4,
                                            "column": 6
                                        }
                                    }
                                ],
                                [
                                    28,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 6,
                                            "value": {
                                                "Expr": 26
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    29,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 6,
                                            "is_null": {
                                                "Expr": 27
                                            }
                                        }
                                    }
                                ],
                                [
                                    30,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 7,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    31,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 4,
                                            "column": 7
                                        }
                                    }
                                ],
                                [
                                    32,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 7,
                                            "value": {
                                                "Expr": 30
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    33,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 7,
                                            "is_null": {
                                                "Expr": 31
                                            }
                                        }
                                    }
                                ],
                                [
                                    34,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 8,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    35,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 4,
                                            "column": 8
                                        }
                                    }
                                ],
                                [
                                    36,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 8,
                                            "value": {
                                                "Expr": 34
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    37,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 8,
                                            "is_null": {
                                                "Expr": 35
                                            }
                                        }
                                    }
                                ],
                                [
                                    38,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 4,
                                            "column": 9,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    39,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 4,
                                            "column": 9
                                        }
                                    }
                                ],
                                [
                                    40,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 9,
                                            "value": {
                                                "Expr": 38
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    41,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 1,
                                            "column": 9,
                                            "is_null": {
                                                "Expr": 39
                                            }
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Return": {
                                    "value": {
                                        "Imm": "Unit"
                                    }
                                }
                            }
                        }
                    }
                },
                "key_layout": 5,
                "value_layout": 1
            }
        },
        "254": {
            "IndexWith": {
                "input": 68,
                "index_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 2,
                            "flags": "input"
                        },
                        {
                            "id": 2,
                            "layout": 3,
                            "flags": "output"
                        },
                        {
                            "id": 3,
                            "layout": 2,
                            "flags": "output"
                        }
                    ],
                    "ret": "Unit",
                    "entry_block": 1,
                    "blocks": {
                        "1": {
                            "id": 1,
                            "body": [
                                [
                                    4,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 0,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    5,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 3,
                                            "column": 0,
                                            "value": {
                                                "Expr": 4
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    6,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 0,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    7,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 0,
                                            "value": {
                                                "Expr": 6
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 1
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 1,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    300,
                                    {
                                        "Copy": {
                                            "value": 8,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 1,
                                            "value": {
                                                "Expr": 300
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 1,
                                            "is_null": {
                                                "Expr": 9
                                            }
                                        }
                                    }
                                ],
                                [
                                    12,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 2,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    13,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 2
                                        }
                                    }
                                ],
                                [
                                    301,
                                    {
                                        "Copy": {
                                            "value": 12,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    14,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 2,
                                            "value": {
                                                "Expr": 301
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    15,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 2,
                                            "is_null": {
                                                "Expr": 13
                                            }
                                        }
                                    }
                                ],
                                [
                                    16,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 3,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    17,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 3
                                        }
                                    }
                                ],
                                [
                                    302,
                                    {
                                        "Copy": {
                                            "value": 16,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    18,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 3,
                                            "value": {
                                                "Expr": 302
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    19,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 3,
                                            "is_null": {
                                                "Expr": 17
                                            }
                                        }
                                    }
                                ],
                                [
                                    20,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 4,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    21,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 4
                                        }
                                    }
                                ],
                                [
                                    303,
                                    {
                                        "Copy": {
                                            "value": 20,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    22,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 4,
                                            "value": {
                                                "Expr": 303
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    23,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 4,
                                            "is_null": {
                                                "Expr": 21
                                            }
                                        }
                                    }
                                ],
                                [
                                    24,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 5,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    25,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 5
                                        }
                                    }
                                ],
                                [
                                    305,
                                    {
                                        "Copy": {
                                            "value": 24,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    26,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 5,
                                            "value": {
                                                "Expr": 305
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    27,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 5,
                                            "is_null": {
                                                "Expr": 25
                                            }
                                        }
                                    }
                                ],
                                [
                                    28,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 6,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    29,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 6
                                        }
                                    }
                                ],
                                [
                                    30,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 6,
                                            "value": {
                                                "Expr": 28
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    31,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 6,
                                            "is_null": {
                                                "Expr": 29
                                            }
                                        }
                                    }
                                ],
                                [
                                    32,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 7,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    33,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 7
                                        }
                                    }
                                ],
                                [
                                    34,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 7,
                                            "value": {
                                                "Expr": 32
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    35,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 7,
                                            "is_null": {
                                                "Expr": 33
                                            }
                                        }
                                    }
                                ],
                                [
                                    36,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 8,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    37,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 8
                                        }
                                    }
                                ],
                                [
                                    38,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 8,
                                            "value": {
                                                "Expr": 36
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    39,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 8,
                                            "is_null": {
                                                "Expr": 37
                                            }
                                        }
                                    }
                                ],
                                [
                                    40,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 9,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    41,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 9
                                        }
                                    }
                                ],
                                [
                                    42,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 9,
                                            "value": {
                                                "Expr": 40
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    43,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 9,
                                            "is_null": {
                                                "Expr": 41
                                            }
                                        }
                                    }
                                ],
                                [
                                    44,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 10,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    45,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 10
                                        }
                                    }
                                ],
                                [
                                    310,
                                    {
                                        "Copy": {
                                            "value": 44,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    46,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 10,
                                            "value": {
                                                "Expr": 310
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    47,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 10,
                                            "is_null": {
                                                "Expr": 45
                                            }
                                        }
                                    }
                                ],
                                [
                                    48,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 6,
                                            "column": 11,
                                            "column_type": "Date"
                                        }
                                    }
                                ],
                                [
                                    49,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 6,
                                            "column": 11
                                        }
                                    }
                                ],
                                [
                                    50,
                                    {
                                        "Store": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 11,
                                            "value": {
                                                "Expr": 48
                                            },
                                            "value_type": "Date"
                                        }
                                    }
                                ],
                                [
                                    51,
                                    {
                                        "SetNull": {
                                            "target": 3,
                                            "target_layout": 2,
                                            "column": 11,
                                            "is_null": {
                                                "Expr": 49
                                            }
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Return": {
                                    "value": {
                                        "Imm": "Unit"
                                    }
                                }
                            }
                        }
                    }
                },
                "key_layout": 5,
                "value_layout": 2
            }
        },
        "262": {
            "JoinCore": {
                "lhs": 248,
                "rhs": 254,
                "join_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 3,
                            "flags": "input"
                        },
                        {
                            "id": 2,
                            "layout": 1,
                            "flags": "input"
                        },
                        {
                            "id": 3,
                            "layout": 2,
                            "flags": "input"
                        },
                        {
                            "id": 4,
                            "layout": 7,
                            "flags": "output"
                        }
                    ],
                    "ret": "Unit",
                    "entry_block": 1,
                    "blocks": {
                        "1": {
                            "id": 1,
                            "body": [
                                [
                                    5,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 0,
                                            "column_type": "Timestamp"
                                        }
                                    }
                                ],
                                [
                                    6,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 0,
                                            "value": {
                                                "Expr": 5
                                            },
                                            "value_type": "Timestamp"
                                        }
                                    }
                                ],
                                [
                                    7,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 1,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 1,
                                            "value": {
                                                "Expr": 7
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 2,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 4,
                                            "column": 2
                                        }
                                    }
                                ],
                                [
                                    310,
                                    {
                                        "Copy": {
                                            "value": 9,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 2,
                                            "value": {
                                                "Expr": 310
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    12,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 2,
                                            "is_null": {
                                                "Expr": 10
                                            }
                                        }
                                    }
                                ],
                                [
                                    13,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 3,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    14,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 4,
                                            "column": 3
                                        }
                                    }
                                ],
                                [
                                    311,
                                    {
                                        "Copy": {
                                            "value": 13,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    15,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 3,
                                            "value": {
                                                "Expr": 311
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    16,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 3,
                                            "is_null": {
                                                "Expr": 14
                                            }
                                        }
                                    }
                                ],
                                [
                                    17,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 4,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    18,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 4,
                                            "column": 4
                                        }
                                    }
                                ],
                                [
                                    19,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 4,
                                            "value": {
                                                "Expr": 17
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    20,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 4,
                                            "is_null": {
                                                "Expr": 18
                                            }
                                        }
                                    }
                                ],
                                [
                                    21,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 5,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    22,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 4,
                                            "column": 5
                                        }
                                    }
                                ],
                                [
                                    312,
                                    {
                                        "Copy": {
                                            "value": 21,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    23,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 5,
                                            "value": {
                                                "Expr": 312
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    24,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 5,
                                            "is_null": {
                                                "Expr": 22
                                            }
                                        }
                                    }
                                ],
                                [
                                    25,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 6,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    26,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 4,
                                            "column": 6
                                        }
                                    }
                                ],
                                [
                                    27,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 6,
                                            "value": {
                                                "Expr": 25
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    28,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 6,
                                            "is_null": {
                                                "Expr": 26
                                            }
                                        }
                                    }
                                ],
                                [
                                    29,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 7,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    30,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 4,
                                            "column": 7
                                        }
                                    }
                                ],
                                [
                                    31,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 7,
                                            "value": {
                                                "Expr": 29
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    32,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 7,
                                            "is_null": {
                                                "Expr": 30
                                            }
                                        }
                                    }
                                ],
                                [
                                    33,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 8,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    34,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 4,
                                            "column": 8
                                        }
                                    }
                                ],
                                [
                                    35,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 8,
                                            "value": {
                                                "Expr": 33
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    36,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 8,
                                            "is_null": {
                                                "Expr": 34
                                            }
                                        }
                                    }
                                ],
                                [
                                    37,
                                    {
                                        "Load": {
                                            "source": 2,
                                            "source_layout": 4,
                                            "column": 9,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    38,
                                    {
                                        "IsNull": {
                                            "target": 2,
                                            "target_layout": 4,
                                            "column": 9
                                        }
                                    }
                                ],
                                [
                                    39,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 9,
                                            "value": {
                                                "Expr": 37
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    40,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 9,
                                            "is_null": {
                                                "Expr": 38
                                            }
                                        }
                                    }
                                ],
                                [
                                    41,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 0,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    42,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 10,
                                            "value": {
                                                "Expr": 41
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    43,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 1,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    44,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 1
                                        }
                                    }
                                ],
                                [
                                    320,
                                    {
                                        "Copy": {
                                            "value": 43,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    45,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 11,
                                            "value": {
                                                "Expr": 320
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    46,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 11,
                                            "is_null": {
                                                "Expr": 44
                                            }
                                        }
                                    }
                                ],
                                [
                                    47,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 2,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    48,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 2
                                        }
                                    }
                                ],
                                [
                                    400,
                                    {
                                        "Copy": {
                                            "value": 47,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    49,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 12,
                                            "value": {
                                                "Expr": 400
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    50,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 12,
                                            "is_null": {
                                                "Expr": 48
                                            }
                                        }
                                    }
                                ],
                                [
                                    51,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 3,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    52,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 3
                                        }
                                    }
                                ],
                                [
                                    401,
                                    {
                                        "Copy": {
                                            "value": 51,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    53,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 13,
                                            "value": {
                                                "Expr": 401
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    54,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 13,
                                            "is_null": {
                                                "Expr": 52
                                            }
                                        }
                                    }
                                ],
                                [
                                    55,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 4,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    56,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 4
                                        }
                                    }
                                ],
                                [
                                    402,
                                    {
                                        "Copy": {
                                            "value": 55,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    57,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 14,
                                            "value": {
                                                "Expr": 402
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    58,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 14,
                                            "is_null": {
                                                "Expr": 56
                                            }
                                        }
                                    }
                                ],
                                [
                                    59,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 5,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    60,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 5
                                        }
                                    }
                                ],
                                [
                                    410,
                                    {
                                        "Copy": {
                                            "value": 59,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    61,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 15,
                                            "value": {
                                                "Expr": 410
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    62,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 15,
                                            "is_null": {
                                                "Expr": 60
                                            }
                                        }
                                    }
                                ],
                                [
                                    63,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 6,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    64,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 6
                                        }
                                    }
                                ],
                                [
                                    65,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 16,
                                            "value": {
                                                "Expr": 63
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    66,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 16,
                                            "is_null": {
                                                "Expr": 64
                                            }
                                        }
                                    }
                                ],
                                [
                                    67,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 7,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    68,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 7
                                        }
                                    }
                                ],
                                [
                                    69,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 17,
                                            "value": {
                                                "Expr": 67
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    70,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 17,
                                            "is_null": {
                                                "Expr": 68
                                            }
                                        }
                                    }
                                ],
                                [
                                    71,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 8,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    72,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 8
                                        }
                                    }
                                ],
                                [
                                    73,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 18,
                                            "value": {
                                                "Expr": 71
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    74,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 18,
                                            "is_null": {
                                                "Expr": 72
                                            }
                                        }
                                    }
                                ],
                                [
                                    75,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 9,
                                            "column_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    76,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 9
                                        }
                                    }
                                ],
                                [
                                    77,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 19,
                                            "value": {
                                                "Expr": 75
                                            },
                                            "value_type": "I32"
                                        }
                                    }
                                ],
                                [
                                    78,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 19,
                                            "is_null": {
                                                "Expr": 76
                                            }
                                        }
                                    }
                                ],
                                [
                                    79,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 10,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    80,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 10
                                        }
                                    }
                                ],
                                [
                                    500,
                                    {
                                        "Copy": {
                                            "value": 79,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    81,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 20,
                                            "value": {
                                                "Expr": 500
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    82,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 20,
                                            "is_null": {
                                                "Expr": 80
                                            }
                                        }
                                    }
                                ],
                                [
                                    83,
                                    {
                                        "Load": {
                                            "source": 3,
                                            "source_layout": 6,
                                            "column": 11,
                                            "column_type": "Date"
                                        }
                                    }
                                ],
                                [
                                    84,
                                    {
                                        "IsNull": {
                                            "target": 3,
                                            "target_layout": 6,
                                            "column": 11
                                        }
                                    }
                                ],
                                [
                                    85,
                                    {
                                        "Store": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 21,
                                            "value": {
                                                "Expr": 83
                                            },
                                            "value_type": "Date"
                                        }
                                    }
                                ],
                                [
                                    86,
                                    {
                                        "SetNull": {
                                            "target": 4,
                                            "target_layout": 7,
                                            "column": 21,
                                            "is_null": {
                                                "Expr": 84
                                            }
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Return": {
                                    "value": {
                                        "Imm": "Unit"
                                    }
                                }
                            }
                        }
                    }
                },
                "value_layout": 8,
                "key_layout": 7,
                "output_kind": "Set"
            }
        },
        "271": {
            "Map": {
                "input": 262,
                "map_fn": {
                    "args": [
                        {
                            "id": 1,
                            "layout": 7,
                            "flags": "input"
                        },
                        {
                            "id": 2,
                            "layout": 9,
                            "flags": "output"
                        }
                    ],
                    "ret": "Unit",
                    "entry_block": 1,
                    "blocks": {
                        "1": {
                            "id": 1,
                            "body": [
                                [
                                    3,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 10,
                                            "column": 0,
                                            "column_type": "Timestamp"
                                        }
                                    }
                                ],
                                [
                                    4,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 9,
                                            "column": 0,
                                            "value": {
                                                "Expr": 3
                                            },
                                            "value_type": "Timestamp"
                                        }
                                    }
                                ],
                                [
                                    5,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 10,
                                            "column": 1,
                                            "column_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    6,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 9,
                                            "column": 1,
                                            "value": {
                                                "Expr": 5
                                            },
                                            "value_type": "F64"
                                        }
                                    }
                                ],
                                [
                                    7,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 10,
                                            "column": 11,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    8,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 10,
                                            "column": 11
                                        }
                                    }
                                ],
                                [
                                    400,
                                    {
                                        "Copy": {
                                            "value": 7,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    9,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 9,
                                            "column": 2,
                                            "value": {
                                                "Expr": 400
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    10,
                                    {
                                        "SetNull": {
                                            "target": 2,
                                            "target_layout": 9,
                                            "column": 2,
                                            "is_null": {
                                                "Expr": 8
                                            }
                                        }
                                    }
                                ],
                                [
                                    11,
                                    {
                                        "Load": {
                                            "source": 1,
                                            "source_layout": 10,
                                            "column": 14,
                                            "column_type": "String"
                                        }
                                    }
                                ],
                                [
                                    12,
                                    {
                                        "IsNull": {
                                            "target": 1,
                                            "target_layout": 10,
                                            "column": 14
                                        }
                                    }
                                ],
                                [
                                    401,
                                    {
                                        "Copy": {
                                            "value": 11,
                                            "value_ty": "String"
                                        }
                                    }
                                ],
                                [
                                    13,
                                    {
                                        "Store": {
                                            "target": 2,
                                            "target_layout": 9,
                                            "column": 3,
                                            "value": {
                                                "Expr": 401
                                            },
                                            "value_type": "String"
                                        }
                                    }
                                ],
                                [
                                    14,
                                    {
                                        "SetNull": {
                                            "target": 2,
                                            "target_layout": 9,
                                            "column": 3,
                                            "is_null": {
                                                "Expr": 12
                                            }
                                        }
                                    }
                                ]
                            ],
                            "terminator": {
                                "Return": {
                                    "value": {
                                        "Imm": "Unit"
                                    }
                                }
                            }
                        }
                    }
                },
                "input_layout": {
                    "Set": 7
                },
                "output_layout": {
                    "Set": 9
                }
            }
        },
        "273": {
            "Sink": {
                "input": 271,
                "query": "CREATE VIEW `TRANSACTIONS_WITH_DEMOGRAPHICS` AS\r\nSELECT `TRANSACTIONS`.`TRANS_DATE_TRANS_TIME`, `TRANSACTIONS`.`CC_NUM`, `DEMOGRAPHICS`.`FIRST`, `DEMOGRAPHICS`.`CITY`\r\nFROM `TRANSACTIONS`\r\nINNER JOIN `DEMOGRAPHICS` ON `TRANSACTIONS`.`CC_NUM` = `DEMOGRAPHICS`.`CC_NUM`"
            }
        }
    },
    "layouts": {
        "8": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "Unit"
                }
            ]
        },
        "2": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "Date"
                }
            ]
        },
        "9": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "Timestamp"
                },
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                }
            ]
        },
        "10": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "Timestamp"
                },
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "Date"
                }
            ]
        },
        "1": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "Timestamp"
                },
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                }
            ]
        },
        "4": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "Timestamp"
                },
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                }
            ]
        },
        "6": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "Date"
                }
            ]
        },
        "7": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "Timestamp"
                },
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": false,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "F64"
                },
                {
                    "nullable": true,
                    "ty": "I32"
                },
                {
                    "nullable": true,
                    "ty": "String"
                },
                {
                    "nullable": true,
                    "ty": "Date"
                }
            ]
        },
        "3": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "F64"
                }
            ]
        },
        "5": {
            "columns": [
                {
                    "nullable": false,
                    "ty": "F64"
                }
            ]
        }
    }
}
"#;
}
