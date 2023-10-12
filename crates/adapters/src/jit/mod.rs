//! Support for JIT-compiled pipelines.

mod catalog;
mod csv;
pub mod deinput;
mod json;
pub mod schema;
pub mod seroutput;

use crate::Catalog;
use pipeline_types::format::json::JsonFlavor;
pub use schema::ProgramSchema;
use std::{collections::HashMap, path::PathBuf};

use dataflow_jit::{
    codegen::CodegenConfig,
    dataflow::RowOutput,
    facade::Demands,
    ir::{DemandId, Graph, GraphExt, NodeId},
    DbspCircuit,
};

use crate::{CircuitCatalog, ControllerError, DbspCircuitHandle};

use self::{
    deinput::DeZSetHandles,
    json::{build_json_deser_config, build_json_ser_config},
    seroutput::SerZSetHandle,
};

/// Config options for compiling and running circuits with JIT.
pub struct CircuitConfig {
    optimize: bool,
    workers: usize,
    release: bool,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            optimize: true,
            workers: 4,
            release: true,
        }
    }
}

impl CircuitConfig {
    pub fn optimize(mut self, optimize: bool) -> Self {
        self.optimize = optimize;
        self
    }

    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    pub fn release(mut self, release: bool) -> Self {
        self.release = release;
        self
    }
}

impl DbspCircuitHandle for DbspCircuit {
    fn step(&mut self) -> Result<(), ControllerError> {
        DbspCircuit::step(self).map_err(ControllerError::dbsp_error)
    }

    fn enable_cpu_profiler(&mut self) -> Result<(), ControllerError> {
        DbspCircuit::enable_cpu_profiler(self).map_err(ControllerError::dbsp_error)
    }

    fn dump_profile(&mut self, dir_path: &str) -> Result<PathBuf, ControllerError> {
        DbspCircuit::dump_profile(self, dir_path).map_err(ControllerError::dbsp_error)
    }

    fn kill(self: Box<Self>) -> std::thread::Result<()> {
        DbspCircuit::kill(*self)
    }
}

/// JIT-compile and instantiate a circuit.
///
/// Generates serializers and deserializers for all supported formats.
// TODO: In the future, when we support configurable (de)serializers we
// may need to JIT them on demand.
#[allow(clippy::type_complexity)]
pub fn start_circuit(
    schema: &ProgramSchema,
    graph: Graph,
    config: CircuitConfig,
) -> Result<(Box<dyn DbspCircuitHandle>, Box<dyn CircuitCatalog>), ControllerError> {
    // For each input node in schema, find layout id,
    // create json and csv demands.
    let source_names: HashMap<_, _> = graph
        .source_nodes()
        .iter()
        .filter_map(|&(node, layout)| {
            graph.nodes()[&node]
                .as_source()
                .and_then(|source| source.name())
                .map(|source| (source.to_owned(), (node, layout.unwrap_set())))
        })
        .collect();

    // println!("sink_names: {source_names:?}");

    let mut demands = Demands::new();
    let mut default_json_input_demands: HashMap<NodeId, DemandId> = HashMap::new();
    let mut debezium_mysql_json_input_demands: HashMap<NodeId, DemandId> = HashMap::new();
    for table_schema in schema.inputs.iter() {
        let (node, layout) = source_names.get(&table_schema.name).ok_or_else(|| ControllerError::schema_validation_error(&format!("program schema specifies input table '{}', which does not exist in the dataflow graph", &table_schema.name)))?;

        let default_json_config =
            build_json_deser_config(*layout, table_schema, &JsonFlavor::Default);
        let debezium_mysql_json_config =
            build_json_deser_config(*layout, table_schema, &JsonFlavor::DebeziumMySql);
        default_json_input_demands.insert(*node, demands.add_json_deserialize(default_json_config));
        debezium_mysql_json_input_demands.insert(
            *node,
            demands.add_json_deserialize(debezium_mysql_json_config),
        );

        // let csv_config = build_csv_deser_config(table_schema);
        // demands.add_csv_deserialize(*layout, csv_config);
    }

    let sink_names: HashMap<_, _> = graph
        .sink_nodes()
        .iter()
        .filter_map(|&(node, layout)| {
            graph.nodes()[&node]
                .as_sink()
                .map(|sink| sink.name())
                .map(|sink| (sink.to_owned(), (node, layout.unwrap_set())))
        })
        .collect();

    let mut json_output_demands: HashMap<NodeId, DemandId> = HashMap::new();
    for table_schema in schema.outputs.iter() {
        let (node, layout) = sink_names.get(&table_schema.name).ok_or_else(|| ControllerError::schema_validation_error(&format!("program schema specifies output view '{}', which does not exist in the dataflow graph", &table_schema.name)))?;

        println!("table_name: {}, layout: {}", &table_schema.name, layout);
        let json_config = build_json_ser_config(*layout, table_schema);
        json_output_demands.insert(*node, demands.add_json_serialize(json_config));

        // let csv_config = build_csv_ser_config(table_schema);
        // demands.add_csv_serialize(*layout, csv_config);
    }

    let mut circuit = DbspCircuit::new(
        graph,
        config.optimize,
        config.workers,
        if config.release {
            CodegenConfig::release()
        } else {
            CodegenConfig::debug()
        },
        demands,
    );

    let mut catalog = Catalog::new();

    for table_schema in schema.inputs.iter() {
        let node_id = source_names[&table_schema.name].0;

        // FIXME: This is unsafe. The correct fix is to make sure `endpoint.disconnect`
        // returns after all endpoint threads have terminated.
        let default_json =
            unsafe { circuit.json_input_set(node_id, default_json_input_demands[&node_id]) }
                .ok_or_else(|| {
                    ControllerError::jit_error(&format!(
                        "JsonSetHandle[Default] not found (table name: '{}', node id: {})",
                        table_schema.name, node_id,
                    ))
                })?;
        let debezium_mysql_json =
            unsafe { circuit.json_input_set(node_id, debezium_mysql_json_input_demands[&node_id]) }
                .ok_or_else(|| {
                    ControllerError::jit_error(&format!(
                        "JsonSetHandle[DebeziumMySQL] not found (table name: '{}', node id: {})",
                        table_schema.name, node_id,
                    ))
                })?;

        catalog.register_input_collection_handle(
            &table_schema.name,
            DeZSetHandles::new(default_json, debezium_mysql_json),
        )
    }

    for table_schema in schema.outputs.iter() {
        let (node_id, layout_id) = sink_names[&table_schema.name];

        let (output, _layout) = circuit.outputs.get(&node_id).ok_or_else(|| {
            ControllerError::jit_error(&format!(
                "output handle not found (view name: {}, node id: {node_id})",
                table_schema.name
            ))
        })?;

        let output = output.clone().ok_or_else(|| {
            // I don't think it should be possible to create an unreachable view.
            ControllerError::jit_error(&format!("output node {node_id} is unreachable"))
        })?;

        let zset_handle = match output {
            RowOutput::Set(output) => output,
            RowOutput::Map(_) => {
                return Err(ControllerError::jit_error(&format!(
                    "unexpected indexed output collection (view name: {}, node id: {node_id})",
                    table_schema.name
                )));
            }
        };

        // FIXME: This is unsafe. The correct fix is to make sure `endpoint.disconnect`
        // returns after all endpoint threads have terminated.
        let json =
            unsafe { circuit.serialization_function(json_output_demands[&node_id], layout_id) }
                .ok_or_else(|| {
                    ControllerError::jit_error(&format!(
                "JSON serialization function not found (view name: '{}', layout id: {layout_id})",
                table_schema.name,
            ))
                })?;

        catalog.register_output_collection_handle(
            &table_schema.name,
            Box::new(SerZSetHandle::new(zset_handle.clone(), json)),
        )
    }

    Ok((Box::new(circuit), Box::new(catalog)))
}
