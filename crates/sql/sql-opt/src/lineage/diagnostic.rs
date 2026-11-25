//! Preserve the source value that the live compiler prints in a failed lineage cast.
use super::{AnalysisError, LineageDiagnostic, Source, report::TableColumn};
use feldera_sql_ir::{Error, NodeId};
use serde::Serialize;
use std::{collections::HashMap, ptr};

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedSource {
    Tuple,
    Ref,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReportValue {
    Unknown,
    Constant { literal: u64 },
    Columns { columns: Vec<TableColumn> },
    Ref { value: usize },
    Tuple { fields: Vec<usize> },
}

impl Source {
    /// Children precede their parents. Reused values need only one entry, and
    /// nested rows do not consume the JSON serializer's recursion budget.
    pub(super) fn wrong_cast(&self, expected: ExpectedSource) -> AnalysisError<'static> {
        let mut values = Vec::new();
        let mut offsets = HashMap::new();
        let mut work = vec![(self, false)];
        while let Some((source, expanded)) = work.pop() {
            let key = ptr::from_ref(source);
            if offsets.contains_key(&key) {
                continue;
            }
            let value = match source {
                Self::Unknown => ReportValue::Unknown,
                Self::Constant(NodeId::Inner(literal)) => {
                    ReportValue::Constant { literal: *literal }
                }
                Self::Tables(columns) => ReportValue::Columns {
                    columns: columns.iter().cloned().collect(),
                },
                Self::Ref(inner) if expanded => ReportValue::Ref {
                    value: offsets[&ptr::from_ref(inner.as_ref())],
                },
                Self::Tuple(fields) if expanded => ReportValue::Tuple {
                    fields: fields
                        .iter()
                        .map(|field| offsets[&ptr::from_ref(field)])
                        .collect(),
                },
                Self::Ref(inner) => {
                    work.push((source, true));
                    work.push((inner, false));
                    continue;
                }
                Self::Tuple(fields) => {
                    work.push((source, true));
                    work.extend(fields.iter().rev().map(|field| (field, false)));
                    continue;
                }
                Self::Constant(NodeId::Outer(_)) | Self::Column(_) => {
                    return Error::new("expected a table lineage value").into();
                }
            };
            offsets.insert(key, values.len());
            values.push(value);
        }
        AnalysisError::Diagnostic(LineageDiagnostic::SourceCast { expected, values })
    }
}
