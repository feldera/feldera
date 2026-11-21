//! Expand a hopping window into the map that finds its starts and the flat-map
//! that emits one row for each start.
use crate::rebuild::{input_type, rebuild_input_closures};
use crate::stream_rewrites::body_members;
use feldera_sql_ir::{
    BinaryOp, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field,
    InnerKind, NodeId, OperatorKind as Op, OutputPort, TypeId, TypeKind as Ty,
};
use std::collections::BTreeMap;

fn edge(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

fn row_field(
    edit: &mut CircuitEditor,
    row: ExpressionId,
    index: usize,
) -> Result<ExpressionId, Error> {
    let value = edit.dereference(row)?;
    edit.tuple_field(value, index)
}

fn collection(edit: &mut CircuitEditor, element: TypeId, kind: Ty) -> Result<TypeId, Error> {
    let (name, code) = match kind {
        Ty::Array => ("Array", "ARRAY"),
        Ty::Vec => ("Vec", "USER"),
        _ => return Err(Error::new("expected an array or vector")),
    };
    let id = edit.inner_at(
        element.into(),
        InnerKind::Type(kind),
        [
            ("typeArgs", Field::Array(vec![edge(element)])),
            ("name", Field::Data(name.into())),
            ("code", Field::Data(code.into())),
            ("mayBeNull", Field::Data(false.into())),
        ],
    )?;
    edit.type_id(id)
}

fn bind(
    edit: &mut CircuitEditor,
    name: &str,
    value: ExpressionId,
) -> Result<(NodeId, ExpressionId), Error> {
    let ty = edit.expression_type(value)?;
    let statement = edit.empty_origin_node(
        value.into(),
        InnerKind::Other("DBSPLetStatement".into()),
        [
            ("type", edge(ty)),
            ("initializer", edge(value)),
            ("mutable", Field::Data(false.into())),
            ("variable", Field::Data(name.into())),
        ],
    )?;
    Ok((statement, edit.named_variable(ty, name)?))
}

struct Hop {
    site: NodeId,
    width: usize,
    timestamp: usize,
    row_type: TypeId,
    output_row: TypeId,
    start_type: TypeId,
    interval: ExpressionId,
    size: ExpressionId,
    offset: ExpressionId,
}

impl Hop {
    fn read(edit: &CircuitEditor, site: NodeId, input: OutputPort) -> Result<Self, Error> {
        let node = edit.node(site)?;
        let stream = input_type(edit, input)?;
        let row_type = edit.type_id(edit.node(stream.into())?.edge("elementType")?)?;
        let width = edit.node(row_type.into())?.edges("tupFields")?.len();
        let output = edit.node(node.edge("outputType")?)?;
        let output_row = edit.type_id(output.edge("elementType")?)?;
        let fields = edit.node(output_row.into())?.edges("tupFields")?;
        if fields.len() != width + 2 {
            return Err(Error::new(
                "HOP output requires the input row and two window bounds",
            ));
        }
        let timestamp = node
            .data("timestampIndex")
            .and_then(|v| v.as_u64())
            .and_then(|n| usize::try_from(n).ok())
            .filter(|&n| n < width)
            .ok_or_else(|| Error::new("HOP requires a timestamp column in its input"))?;
        Ok(Self {
            site,
            width,
            timestamp,
            row_type,
            output_row,
            start_type: edit.type_id(fields[width])?,
            interval: edit.expression(node.edge("interval")?)?,
            size: edit.expression(node.edge("size")?)?,
            offset: edit.expression(node.edge("start")?)?,
        })
    }

    fn map(
        &self,
        edit: &mut CircuitEditor,
        next: &mut u64,
    ) -> Result<(ExpressionId, TypeId), Error> {
        let reference = edit.reference_type(self.row_type)?;
        let row = edit.variable(reference, next)?;
        let mut fields = Vec::with_capacity(self.width + 1);
        for i in 0..self.width {
            let value = row_field(edit, row, i)?;
            fields.push(edit.clone_if_needed(value)?);
        }
        let timestamp = row_field(edit, row, self.timestamp)?;
        let args = [timestamp, self.interval, self.size, self.offset];
        let mut name = String::from("hop");
        for arg in args {
            let ty = edit.expression_type(arg)?;
            let kind = edit.type_kind(ty)?;
            let suffix = match kind {
                Ty::Timestamp => "Timestamp",
                Ty::TimestampTz => "TimestampTz",
                Ty::Date => "Date",
                Ty::Time => "Time",
                Ty::ShortInterval => "ShortInterval",
                Ty::LongInterval => "LongInterval",
                _ => return Err(Error::new("invalid HOP operand type")),
            };
            name.push('_');
            name.push_str(suffix);
            if !matches!(kind, Ty::ShortInterval | Ty::LongInterval) && edit.nullable(ty)? {
                name.push('N');
            }
        }
        let array = collection(edit, self.start_type, Ty::Array)?;
        let path = edit.function_path(self.site, &name)?;
        fields.push(edit.make_expr(
            self.site,
            Expr::Apply,
            array,
            [
                ("function", edge(path)),
                (
                    "arguments",
                    Field::Array(args.into_iter().map(edge).collect()),
                ),
            ],
        )?);
        let body = edit.tuple_expr(self.site, false, &fields)?;
        Ok((
            edit.closure_from_variables(body, &[row])?,
            edit.expression_type(body)?,
        ))
    }

    fn flat_map(
        &self,
        edit: &mut CircuitEditor,
        mapped: TypeId,
        next: &mut u64,
    ) -> Result<ExpressionId, Error> {
        let reference = edit.reference_type(mapped)?;
        let data = edit.variable(reference, next)?;
        let start = edit.variable(self.start_type, next)?;
        let mut statements = Vec::with_capacity(self.width + 2);
        let mut fields = Vec::with_capacity(self.width + 2);
        for i in 0..self.width {
            let value = row_field(edit, data, i)?;
            let value = edit.clone_if_needed(value)?;
            let (statement, var) = bind(edit, &format!("x{i}"), value)?;
            statements.push(edge(statement));
            fields.push(edit.clone_value(var)?);
        }
        let array = row_field(edit, data, self.width)?;
        let array_ref = edit.reference_type(edit.expression_type(array)?)?;
        let borrowed = edit.make_expr(
            data.into(),
            Expr::Borrow,
            array_ref,
            [
                ("expression", edge(array)),
                ("mut", Field::Data(false.into())),
            ],
        )?;
        let (statement, array) = bind(edit, "array", borrowed)?;
        statements.push(edge(statement));
        let array = edit.dereference(array)?;
        let vec = collection(edit, self.start_type, Ty::Vec)?;
        let data_vec = edit.make_expr(
            array.into(),
            Expr::Deref,
            vec,
            [("expression", edge(array))],
        )?;
        let owned = edit.clone_value(data_vec)?;
        let (statement, items) = bind(edit, "array_clone", owned)?;
        statements.push(edge(statement));
        let iter = edit.iterator_method(items.into(), "into_iter", items, &[])?;
        fields.push(start);
        fields.push(edit.binary_expression(
            self.site,
            self.start_type,
            BinaryOp::Add,
            start,
            self.size,
        )?);
        let tuple = edit.tuple_expr(self.site, false, &fields)?;
        let produce = edit.closure_from_variables(tuple, &[start])?;
        let rows = edit.iterator_method(self.site, "map", iter, &[produce])?;
        let ty = edit.expression_type(rows)?;
        let block = edit.empty_origin_node(
            self.site,
            InnerKind::Expression(Expr::Block),
            [
                ("contents", Field::Array(statements)),
                ("lastExpression", edge(rows)),
                ("type", edge(ty)),
            ],
        )?;
        edit.closure_from_variables(edit.expression(block)?, &[data])
    }
}

pub fn expand_hop(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let sites: Vec<_> = body_members(&circuit)?
        .into_iter()
        .filter(|&(_, id)| circuit.node(id).and_then(|n| n.operator_kind()) == Some(Op::Hop))
        .collect();
    if sites.is_empty() {
        return Ok(circuit);
    }
    let mut origins = circuit.intermediate_origins()?;
    let sites = sites
        .into_iter()
        .map(|(body, id)| Ok((body, id, origins.derive::<1>(id)?.map(|[origin]| origin))))
        .collect::<Result<Vec<_>, Error>>()?;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut placements = Vec::new();
    let mut counter = *next;
    for (body, site, origin) in sites {
        let input = edit
            .node(site)?
            .inputs()
            .next()
            .ok_or_else(|| Error::new("HOP requires an input"))?;
        let hop = Hop::read(&edit, site, input)?;
        let (map_fn, row) = hop.map(&mut edit, &mut counter)?;
        let output = edit.zset_type(row)?;
        let map = edit.append_operator(
            body,
            site,
            Op::Map,
            &[input],
            [("outputType", edge(output)), ("function", edge(map_fn))],
        )?;
        edit.set_intermediate_origin(map, origin)?;
        let flat_fn = hop.flat_map(&mut edit, row, &mut counter)?;
        let output = edit.zset_type(hop.output_row)?;
        let flat = edit.append_operator(
            body,
            site,
            Op::FlatMap,
            &[OutputPort {
                operator: map,
                number: 0,
            }],
            [("outputType", edge(output)), ("function", edge(flat_fn))],
        )?;
        placements.push((body, map, site));
        replacements.insert(
            site,
            OutputPort {
                operator: flat,
                number: 0,
            },
        );
    }
    // Build all expressions while their original provenance definitions are in scope.
    // Each appended map/flat-map pair is then the last pair in its body as it moves.
    for (body, map, site) in placements.into_iter().rev() {
        edit.place_before(body, map, site)?;
    }
    let result = rebuild_input_closures(edit.finish_forwarding(&replacements)?, &mut counter)?;
    *next = counter;
    Ok(result)
}
