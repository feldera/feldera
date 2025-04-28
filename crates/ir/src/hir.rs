use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

pub type Hir = HashMap<String, CalciteRels>;

/// The Calcite plan representation of a dataflow graph.
#[derive(Debug, Deserialize)]
pub struct CalciteRels {
    pub rels: Vec<Rel>,
}

#[derive(Debug, Deserialize)]
pub struct Rel {
    pub id: usize,
    #[serde(default)]
    pub inputs: Vec<usize>,
    #[serde(rename = "relOp")]
    pub rel_op: String,

    /// This is a vector where the elements concatenated form a fully qualified table name.
    ///
    /// e.g., usually is of the form `[$namespace, $table] / [schema, table]`
    #[serde(default)]
    pub table: Option<Vec<String>>,

    #[serde(default)]
    pub condition: Option<Condition>,

    #[serde(default)]
    #[serde(rename = "joinType")]
    pub join_type: Option<String>,

    #[serde(default)]
    pub exprs: Option<Vec<Operand>>,

    #[serde(default)]
    pub fields: Option<Vec<String>>,

    #[serde(default)]
    pub all: Option<bool>,

    #[serde(default)]
    pub aggs: Option<Vec<Value>>,

    #[serde(default)]
    pub group: Option<Vec<usize>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct Condition {
    pub op: Option<Op>,
    pub operands: Option<Vec<Operand>>,
    #[serde(default)]
    pub literal: bool,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct Op {
    pub kind: String,
    pub name: String,
    pub syntax: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct Operand {
    pub input: Option<usize>,
    pub name: Option<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum CalciteId {
    Partial {
        partial: usize,
    },
    Final {
        #[serde(rename = "final")]
        final_: usize,
    },
    And {
        and: Vec<CalciteId>,
    },
    Seq {
        seq: Vec<CalciteId>,
    },
    Null,
}

impl CalciteId {
    #[allow(unused)]
    fn contains(&self, id: usize) -> bool {
        match self {
            CalciteId::Partial { partial } => *partial == id,
            CalciteId::Final { final_ } => *final_ == id,
            CalciteId::And { and } => and.iter().any(|cid| cid.contains(id)),
            CalciteId::Seq { seq } => seq.iter().any(|cid| cid.contains(id)),
            CalciteId::Null => false,
        }
    }
}

impl From<CalciteId> for Vec<usize> {
    fn from(val: CalciteId) -> Self {
        match val {
            CalciteId::Partial { partial } => vec![partial],
            CalciteId::Final { final_ } => vec![final_],
            CalciteId::And { and } => and.into_iter().flat_map(Into::<Vec<usize>>::into).collect(),
            CalciteId::Seq { seq } => seq.into_iter().flat_map(Into::<Vec<usize>>::into).collect(),
            CalciteId::Null => vec![],
        }
    }
}
