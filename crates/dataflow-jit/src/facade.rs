#![allow(dead_code)]

use crate::{
    codegen::{CodegenConfig, NativeLayoutCache},
    dataflow::{CompiledDataflow, JitHandle, RowInput, RowOutput},
    ir::{Graph, GraphExt, NodeId, Validator},
};
use dbsp::{DBSPHandle, Runtime};
use std::collections::BTreeMap;

pub struct DbspCircuit {
    jit: JitHandle,
    runtime: DBSPHandle,
    inputs: BTreeMap<NodeId, RowInput>,
    outputs: BTreeMap<NodeId, RowOutput>,
    layout_cache: NativeLayoutCache,
}

impl DbspCircuit {
    pub fn new<F>(build: F, optimize: bool, workers: usize, config: CodegenConfig) -> Self
    where
        F: FnOnce() -> Graph,
    {
        let mut graph = build();

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
}
