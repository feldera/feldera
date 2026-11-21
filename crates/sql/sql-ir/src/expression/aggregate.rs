//! Combine separately computed aggregates with a common grouping key.

use super::{CircuitEditor, Error, Expr, ExpressionId, Field, InnerKind, NodeId, Ty, TypeId};
use crate::{OperatorKind as Op, OutputPort};
use feldera_sql_source::SourceRange;

/// Relational origin at the call site. Public observations omit the source of
/// an outer operator, so callers may supply its range from the compiler prefix.
#[derive(Clone, Copy, Debug)]
pub struct OperatorSite {
    pub operator: NodeId,
    pub position: Option<SourceRange>,
}

use super::RefParameter;

impl CircuitEditor {
    fn read_fields(
        &mut self,
        param: &RefParameter,
        copy_path: bool,
    ) -> Result<Vec<ExpressionId>, Error> {
        let fields = self.node(param.argument.0)?.edges("tupFields")?;
        let mut result = Vec::with_capacity(fields.len());
        for i in 0..fields.len() {
            // flatten(key) deep-copies the path for each key field. The value
            // arguments share the variable and each field has its own deref.
            let path = if copy_path {
                self.derive_inner(param.path.0, [])?
            } else {
                param.path.0
            };
            let deref = self.make_expr(
                path,
                Expr::Deref,
                param.argument,
                [("expression", Field::Reference(path))],
            )?;
            let field = self.tuple_field(deref, i)?;
            result.push(self.clone_if_needed(field)?);
        }
        Ok(result)
    }

    /// Return the combined indexed aggregate in the specified circuit body.
    /// Two inputs use a stream join. A star join consumes deltas, so larger
    /// groups differentiate each input and integrate the result.
    pub fn combine_aggregate_list(
        &mut self,
        body: NodeId,
        site: OperatorSite,
        key: TypeId,
        aggregates: &[NodeId],
        next: &mut u64,
    ) -> Result<NodeId, Error> {
        if aggregates.is_empty() {
            return Err(Error::new("an aggregate list needs at least one input"));
        }
        if let [only] = aggregates {
            return self
                .node(*only)?
                .operator_kind()
                .map(|_| *only)
                .ok_or_else(|| Error::new("expected an aggregate operator"));
        }
        let members = self.node(body)?.edges("allOperators")?;
        let mut collections = Vec::with_capacity(aggregates.len());
        let mut rows = Vec::with_capacity(aggregates.len());
        for &aggregate in aggregates {
            if !members.contains(&aggregate) {
                return Err(Error::new("aggregate input is outside its circuit body"));
            }
            let collection = self.type_id(self.node(aggregate)?.edge("outputType")?)?;
            if self.type_kind(collection)? != Ty::IndexedZSet {
                return Err(Error::new("aggregate input requires an indexed ZSet"));
            }
            let input_key = self.type_id(self.node(collection.0)?.edge("keyType")?)?;
            if !self.same_type(input_key, key)? {
                return Err(Error::new("aggregate inputs have different grouping keys"));
            }
            let row = self.type_id(self.node(collection.0)?.edge("elementType")?)?;
            if self.type_kind(row)? != Ty::Tuple {
                return Err(Error::new("aggregate values require SQL tuples"));
            }
            collections.push(collection);
            rows.push(row);
        }
        self.reserve_outer_ids(if aggregates.len() == 2 {
            1
        } else {
            aggregates
                .len()
                .checked_add(2)
                .ok_or_else(|| Error::new("aggregate list too large"))?
        })?;
        let mut next_name = *next;
        let kp = self.ref_parameter(key, &mut next_name)?;
        let keys = self.read_fields(&kp, true)?;
        let mut params = vec![kp];
        let mut values = Vec::new();
        let mut value_types = Vec::new();
        for &row in &rows {
            let p = self.ref_parameter(row, &mut next_name)?;
            value_types.extend(self.node(row.0)?.edges("tupFields")?);
            values.extend(self.read_fields(&p, false)?);
            params.push(p);
        }
        let key_tuple = self.tuple_expr(key.0, false, &keys)?;
        let value_tuple = self.tuple_expr(key.0, false, &values)?;
        let pair = self.tuple_expr(key.0, true, &[key_tuple, value_tuple])?;
        let closure = self.closure(pair, &params)?;
        let elements = self.tuple_type(key.0, false, value_types)?;
        let mut output = self.inner_at(
            site.operator,
            InnerKind::Type(Ty::IndexedZSet),
            [
                ("keyType", Field::Reference(key.0)),
                ("elementType", Field::Reference(elements.0)),
                ("name", Field::Data("IndexedWSet".into())),
                ("code", Field::Data("INDEXED_ZSET".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        if self.node(output)?.field("position").is_none() {
            if let Some(range) = site.position.filter(|range| range.is_valid()) {
                output = self.derive_inner(
                    output,
                    [("position", Field::Data(serde_json::json!(range)))],
                )?;
            }
        }
        let port = |operator| OutputPort {
            operator,
            number: 0,
        };
        let mut inputs = aggregates.iter().copied().map(port).collect::<Vec<_>>();
        let mut fields = vec![
            ("outputType", Field::Reference(output)),
            ("function", Field::Reference(closure.0)),
        ];
        let result = if aggregates.len() == 2 {
            fields.push(("balanced", Field::Data(false.into())));
            self.append_operator(body, site.operator, Op::StreamJoinIndex, &inputs, fields)?
        } else {
            for (input, ty) in inputs.iter_mut().zip(collections) {
                *input = port(self.append_operator(
                    body,
                    site.operator,
                    Op::Differentiate,
                    &[*input],
                    [("outputType", Field::Reference(ty.0))],
                )?);
            }
            let join =
                self.append_operator(body, site.operator, Op::StarJoinIndex, &inputs, fields)?;
            self.append_operator(
                body,
                site.operator,
                Op::Integrate,
                &[port(join)],
                [("outputType", Field::Reference(output))],
            )?
        };
        *next = next_name;
        Ok(result)
    }
}
