//! Adapt the versioned S3 checkpoint to the public Gen-2 and profiler contracts.

use feldera_sql_ir::{Circuit, Field};
use serde_json::{json, Value};
use std::{error::Error, ffi::OsStr};

pub enum Output {
    Circuit,
    Dataflow,
    Diagnostics,
    Artifacts,
}

impl Output {
    pub fn from_flag(flag: &OsStr) -> Option<Self> {
        match flag.to_str()? {
            "--s3-to-gen2" => Some(Self::Circuit),
            "--s3-to-dataflow" => Some(Self::Dataflow),
            "--s3-to-diagnostics" => Some(Self::Diagnostics),
            "--s3-to-artifacts" => Some(Self::Artifacts),
            _ => None,
        }
    }

    pub fn emit(&self, input: &str) -> Result<Value, Box<dyn Error>> {
        self.emit_checkpoint(feldera_sql_ir::parse_circuit_envelope(input.as_bytes())?)
    }

    pub(super) fn emit_checkpoint(&self, mut checkpoint: Value) -> Result<Value, Box<dyn Error>> {
        if checkpoint["stage"] != "S3-subset" || checkpoint["schema_version"] != 4 {
            return Err("expected a version 4 S3 checkpoint".into());
        }
        if matches!(self, Self::Diagnostics) {
            return crate::diagnostics::render(
                &checkpoint["diagnostics"]["messages"],
                &checkpoint["sources"]["lines"],
            );
        }
        let gen2 = checkpoint["options"]["ioOptions"]["gen2"]
            .as_bool()
            .ok_or("missing S3 engine mode")?;
        if matches!(self, Self::Circuit) && !gen2 {
            return Err("expected Gen-2 compiler options in the S3 checkpoint".into());
        }
        let graph = Circuit::from_checkpoint_json(checkpoint["circuit"].take())?;
        if matches!(self, Self::Circuit) {
            return Ok(graph.to_gen2_json()?);
        }

        if !checkpoint["calcite_plan"].is_object()
            || !checkpoint["sources"]["lines"]
                .as_array()
                .is_some_and(|lines| lines.iter().all(Value::is_string))
        {
            return Err("S3 dataflow requires a relational plan and source lines".into());
        }
        let diagnostics = if matches!(self, Self::Artifacts) {
            crate::diagnostics::render(
                &checkpoint["diagnostics"]["messages"],
                &checkpoint["sources"]["lines"],
            )?
        } else {
            Value::Null
        };
        let dataflow = json!({
            "calcite_plan": &checkpoint["calcite_plan"],
            "mir": graph.mir()?,
            "sources": &checkpoint["sources"]["lines"],
        });
        if matches!(self, Self::Dataflow) {
            return Ok(dataflow);
        }
        let Some(Field::Data(schema @ Value::Object(_))) = graph.root().field("metadata") else {
            return Err("S3 circuit requires schema metadata".into());
        };
        let mut result = json!({"schema":schema, "dataflow":dataflow, "diagnostics":diagnostics});
        if gen2 {
            result["circuit"] = graph.to_gen2_json()?;
        } else {
            checkpoint["circuit"] = graph.to_json();
            result["checkpoint"] = checkpoint;
        }
        Ok(result)
    }
}
