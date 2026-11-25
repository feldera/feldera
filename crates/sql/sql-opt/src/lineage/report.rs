//! Trace copied values to SQL table columns for the correlated-column report.
use super::diagnostic::ExpectedSource;
use super::{AnalysisError, Interpreter, LineageDiagnostic, Merge, Source};
use feldera_sql_expr::FlatmapColumns;
use feldera_sql_ir::{
    BodyOrder, CircuitEditor, Error, Field, Node, NodeId, OperatorKind as Op, OutputPort, TypeKind,
};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TableColumn {
    table: String,
    column: String,
}

/// The JVM renders literals from their live nodes in SQL syntax.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Finding {
    Correlated {
        columns: Vec<TableColumn>,
    },
    Compared {
        columns: Vec<TableColumn>,
        comparison: u64,
        literal: u64,
    },
}

impl Finding {
    pub(super) fn comparison(
        columns: &BTreeSet<TableColumn>,
        comparison: NodeId,
        literal: NodeId,
    ) -> Result<Self, Error> {
        let (NodeId::Inner(comparison), NodeId::Inner(literal)) = (comparison, literal) else {
            return Err(Error::new(
                "expected an expression and a literal in a comparison",
            ));
        };
        Ok(Self::Compared {
            columns: columns.iter().cloned().collect(),
            comparison,
            literal,
        })
    }
}

fn table_row(op: &Node) -> Result<Source, Error> {
    let table = op.relation_name("tableName")?;
    let Some(Field::Object(metadata)) = op.field("metadata") else {
        return Err(Error::new("missing table metadata for lineage"));
    };
    let Some(Field::Array(fields)) = metadata.get("columnMetadata") else {
        return Err(Error::new("missing table columns for lineage"));
    };
    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        let name = match field {
            Field::Object(field) => field.get("name"),
            _ => None,
        };
        let Some(Field::Object(identifier)) = name else {
            return Err(Error::new("missing column identifier"));
        };
        let Some(Field::Data(name)) = identifier.get("name") else {
            return Err(Error::new("missing column name"));
        };
        let column = name
            .as_str()
            .ok_or_else(|| Error::new("invalid column name"))?
            .to_owned();
        columns.push(Source::Tables(Rc::new(BTreeSet::from([TableColumn {
            table: table.into(),
            column,
        }]))));
    }
    Ok(Source::Tuple(columns.into()))
}

struct Report<'a> {
    edit: &'a CircuitEditor,
    outputs: HashMap<OutputPort, Source>,
    findings: Vec<Finding>,
}

impl<'a> Report<'a> {
    fn read(&self, input: OutputPort) -> Result<Source, AnalysisError<'a>> {
        self.outputs
            .get(&input)
            .cloned()
            .ok_or(AnalysisError::Diagnostic(LineageDiagnostic::MissingInput(
                input,
            )))
    }

    fn borrow(&self, port: OutputPort) -> Result<Source, AnalysisError<'a>> {
        let value = self.read(port)?;
        if matches!(value, Source::Unknown) {
            return Ok(value);
        }
        let ty = self
            .edit
            .type_id(self.edit.node(port.operator)?.edge("outputType")?)?;
        if self.edit.type_kind(ty)? == TypeKind::ZSet {
            return Ok(Source::Ref(Rc::new(value)));
        }
        let Source::Tuple(pair) = value else {
            return Err(AnalysisError::Diagnostic(LineageDiagnostic::ShapeAssertion));
        };
        let [key, value] = pair.as_ref() else {
            return Err(AnalysisError::Diagnostic(LineageDiagnostic::ShapeAssertion));
        };
        Ok(Source::Tuple(
            [
                Source::Ref(Rc::new(key.clone())),
                Source::Ref(Rc::new(value.clone())),
            ]
            .into(),
        ))
    }

    fn unary_closure(
        &mut self,
        function: NodeId,
        input: OutputPort,
    ) -> Result<Source, AnalysisError<'a>> {
        let input = self.borrow(input)?;
        let parameters = self.edit.node(function)?.edges("parameters")?;
        let [parameter] = parameters.as_slice() else {
            return Err(AnalysisError::Diagnostic(LineageDiagnostic::ShapeAssertion));
        };
        self.closure(function, HashMap::from([(*parameter, input)]))
    }

    fn closure(
        &mut self,
        function: NodeId,
        declarations: super::DeclarationValues,
    ) -> Result<Source, AnalysisError<'a>> {
        let mut interpreter = Interpreter::new(self.edit, false);
        interpreter.track_constants = true;
        interpreter.comparisons = Some(Vec::new());
        let source = interpreter.eval(function, declarations);
        self.findings
            .extend(interpreter.comparisons.into_iter().flatten());
        source
    }

    fn join(
        &mut self,
        id: NodeId,
        op: &Node,
        left: OutputPort,
        right: OutputPort,
    ) -> Result<(), AnalysisError<'a>> {
        fn split(source: Source) -> Result<(Option<Source>, Source), AnalysisError<'static>> {
            match source {
                Source::Unknown => Ok((None, Source::Unknown)),
                Source::Tuple(pair) if pair.len() == 2 => {
                    Ok((Some(pair[0].clone()), pair[1].clone()))
                }
                _ => Err(source.wrong_cast(ExpectedSource::Tuple)),
            }
        }
        let left = self.borrow(left)?;
        let right = self.borrow(right)?;
        let (lk, left) = split(left)?;
        let (rk, right) = split(right)?;
        let key = match (lk, rk) {
            (Some(l), Some(r)) => l.combine(&r, self.edit, Merge::JoinSources)?,
            (l, r) => l.or(r).unwrap_or_default(),
        };
        let function = op.edge("function")?;
        let parameters = self.edit.node(function)?.edges("parameters")?;
        // Join closures may have extra parameters. Only the first three have lineage.
        if parameters.len() < 3 {
            return Err(AnalysisError::Diagnostic(
                LineageDiagnostic::JoinParameterIndex(parameters.len()),
            ));
        }
        let bindings = parameters
            .into_iter()
            .zip([key.clone(), left, right])
            .collect();
        let result = self.closure(function, bindings)?;
        self.set(id, result)?;
        if let Source::Ref(inner) = key {
            if let Source::Tuple(fields) = inner.as_ref() {
                for field in fields.iter() {
                    if let Source::Tables(columns) = field {
                        if columns.len() > 1 {
                            self.findings.push(Finding::Correlated {
                                columns: columns.iter().cloned().collect(),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn flatmap(&self, function: &Node, port: OutputPort) -> Result<Source, AnalysisError<'a>> {
        let source = self.borrow(port)?;
        if matches!(source, Source::Unknown) || function.class() != "DBSPFlatmap" {
            return Ok(Source::Unknown);
        }
        let Source::Ref(input) = source else {
            return Err(source.wrong_cast(ExpectedSource::Ref));
        };
        let Source::Tuple(row) = input.as_ref() else {
            return Err(input.wrong_cast(ExpectedSource::Tuple));
        };
        let columns = FlatmapColumns::read(function)?;
        // Read every passthrough field before the output shuffle discards or repeats any.
        let mut fields = Vec::with_capacity(columns.passthrough.len());
        for index in columns.passthrough {
            fields.push(row.get(index).ok_or(AnalysisError::Diagnostic(
                LineageDiagnostic::FieldIndex {
                    index,
                    size: row.len(),
                },
            ))?);
        }
        let result: Vec<_> = columns
            .output
            .into_iter()
            .map(|i| fields.get(i).copied().cloned().unwrap_or_default())
            .collect();
        Ok(if result.iter().all(|s| matches!(s, Source::Unknown)) {
            Source::Unknown
        } else {
            Source::Tuple(result.into())
        })
    }

    fn visit(&mut self, id: NodeId) -> Result<(), AnalysisError<'a>> {
        let node = self.edit.node(id)?;
        let inputs: Vec<_> = node.inputs().collect();
        let input = |n: usize| {
            inputs
                .get(n)
                .copied()
                .ok_or_else(|| Error::new("missing lineage input"))
        };
        let Some(kind) = node.operator_kind() else {
            return Ok(());
        };
        let result = match kind {
            Op::SourceMultiset | Op::SourceMap | Op::SourceLazyMap | Op::ViewDeclaration => {
                table_row(node)?
            }
            Op::Map | Op::MapIndex | Op::Deindex => {
                let function = node.edge("function")?;
                if self.edit.node(function)?.class() == "DBSPClosureExpression" {
                    self.unary_closure(function, input(0)?)?
                } else {
                    Source::Unknown
                }
            }
            Op::Filter => {
                self.unary_closure(node.edge("function")?, input(0)?)?;
                self.read(input(0)?)?
            }
            Op::Negate
            | Op::Positive
            | Op::StreamDistinct
            | Op::View
            | Op::Integrate
            | Op::Noop
            | Op::Differentiate => self.read(input(0)?)?,
            Op::ChainAggregate
            | Op::IndexedTopK
            | Op::Rank
            | Op::RowNumber
            | Op::StreamAggregate
            | Op::AggregateZero
            | Op::Lag
            | Op::PartitionedRollingAggregate => match self.borrow(input(0)?)? {
                Source::Tuple(pair) if pair.len() == 2 => {
                    Source::Tuple([pair[0].clone(), Source::Unknown].into())
                }
                _ => Source::Unknown,
            },
            Op::FlatMap => self.flatmap(self.edit.node(node.edge("function")?)?, input(0)?)?,
            Op::Sum | Op::AtomicSum | Op::Subtract => {
                let mut value = self.read(input(0)?)?;
                for &port in &inputs[1..] {
                    value = value.combine(&self.read(port)?, self.edit, Merge::CommonSources)?;
                }
                value
            }
            Op::Hop => match self.read(input(0)?)? {
                Source::Unknown => Source::Unknown,
                Source::Tuple(fields) => {
                    let mut fields = fields.to_vec();
                    fields.extend([Source::Unknown, Source::Unknown]);
                    Source::Tuple(fields.into())
                }
                source => return Err(source.wrong_cast(ExpectedSource::Tuple)),
            },
            Op::StreamJoin
            | Op::StreamJoinIndex
            | Op::Join
            | Op::JoinIndex
            | Op::LeftJoin
            | Op::LeftJoinIndex
            | Op::LeftJoinFilterMap
            | Op::AsofJoin
            | Op::ConcreteAsofJoin
            | Op::JoinFilterMap => return self.join(id, node, input(0)?, input(1)?),
            Op::Window | Op::StreamAntiJoin => self.borrow(input(0)?)?,
            Op::Constant | Op::Now => Source::Unknown,
            // The reference visitor publishes no value for these operators. A
            // subsequent read of such a port follows the original diagnostic path.
            _ => return Ok(()),
        };
        self.set(id, result)
    }

    fn set(&mut self, id: NodeId, result: Source) -> Result<(), AnalysisError<'a>> {
        if let Source::Tuple(fields) = &result {
            let output = self.edit.node(id)?.edge("outputType")?;
            let row = match self.edit.type_kind(self.edit.type_id(output)?)? {
                TypeKind::ZSet => self.edit.node(output)?.edge("elementType")?,
                _ => output,
            };
            let width = match self.edit.type_kind(self.edit.type_id(row)?)? {
                TypeKind::IndexedZSet if row == output => Some(2),
                TypeKind::Tuple | TypeKind::RawTuple => {
                    Some(self.edit.node(row)?.edges("tupFields")?.len())
                }
                _ => None,
            };
            if width != Some(fields.len()) {
                return Err(AnalysisError::Diagnostic(LineageDiagnostic::OutputWidth {
                    operator: id,
                    fields: fields.len(),
                }));
            }
        }
        self.outputs.insert(
            OutputPort {
                operator: id,
                number: 0,
            },
            result,
        );
        Ok(())
    }
}

/// Findings from all operators and operands visited before the first diagnostic.
pub struct LineageReport<'a> {
    pub findings: Vec<Finding>,
    pub diagnostic: Option<LineageDiagnostic<'a>>,
}

pub fn checked_lineage_report(
    edit: &CircuitEditor,
) -> Result<LineageReport<'_>, AnalysisError<'_>> {
    let mut report = Report {
        edit,
        outputs: HashMap::new(),
        findings: Vec::new(),
    };
    for id in edit.circuit().operator_order(BodyOrder::Postorder)? {
        match report.visit(id) {
            Ok(()) => {}
            Err(AnalysisError::Diagnostic(diagnostic)) => {
                return Ok(LineageReport {
                    findings: report.findings,
                    diagnostic: Some(diagnostic),
                });
            }
            Err(error) => return Err(error),
        }
    }
    Ok(LineageReport {
        findings: report.findings,
        diagnostic: None,
    })
}

/// Detached snapshots may lack the JDK conversion used to compare timestamp literals.
pub fn lineage_report(edit: &CircuitEditor) -> Result<Option<LineageReport<'_>>, Error> {
    match checked_lineage_report(edit) {
        Ok(report) => Ok(Some(report)),
        Err(AnalysisError::Reference(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub fn correlated_columns(edit: &CircuitEditor) -> Result<Option<Vec<Finding>>, Error> {
    Ok(lineage_report(edit)?
        .and_then(|report| report.diagnostic.is_none().then_some(report.findings)))
}
