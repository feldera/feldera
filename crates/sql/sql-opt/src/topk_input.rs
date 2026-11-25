//! Build the indexed projection and rebound TopK over its narrower input.
use crate::{append_input_projection, project_topk_comparators};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind, OutputPort};
use serde_json::json;

/// The ordering and output closure of a TopK after projecting its input or output.
pub struct TopK<'a> {
    pub producer: ExpressionId,
    pub rank: NodeId,
    pub equality: NodeId,
    pub limit: NodeId,
    pub numbering: &'a str,
}

impl TopK<'_> {
    pub fn append(
        &self,
        edit: &mut CircuitEditor,
        body: NodeId,
        site: NodeId,
        input: OutputPort,
        collection: NodeId,
    ) -> Result<OutputPort, Error> {
        let key = edit.type_id(edit.node(collection)?.edge("keyType")?)?;
        let function = edit.expression_type(self.producer)?;
        let value = edit.type_id(edit.node(function.into())?.edge("resultType")?)?;
        let output = edit.indexed_zset_type(collection, key, value)?;
        let operator = edit.append_operator(
            body,
            site,
            OperatorKind::IndexedTopK,
            &[input],
            [
                ("outputType", Field::Reference(output.into())),
                ("function", Field::Reference(self.rank)),
                ("limit", Field::Reference(self.limit)),
                ("numbering", Field::Data(self.numbering.into())),
                ("outputProducer", Field::Reference(self.producer.into())),
                ("equalityComparator", Field::Reference(self.equality)),
            ],
        )?;
        Ok(OutputPort {
            operator,
            number: 0,
        })
    }
}

pub struct ProjectedTopK<'a> {
    pub projection: ExpressionId,
    pub producer: ExpressionId,
    pub rank: NodeId,
    pub equality: NodeId,
    pub limit: NodeId,
    pub numbering: &'a str,
    pub printed_row: &'a str,
    pub used: &'a [bool],
    pub projection_width: usize,
}

impl ProjectedTopK<'_> {
    pub fn append(
        &self,
        edit: &mut CircuitEditor,
        body: NodeId,
        site: NodeId,
        input: OutputPort,
    ) -> Result<(), Error> {
        let projected = append_input_projection(
            edit,
            body,
            site,
            input,
            OperatorKind::MapIndex,
            self.projection,
        )?;
        edit.annotate(
            projected.operator,
            json!({"class": "IsProjection", "outputSize": self.projection_width}),
        )?;
        let collection = edit.node(projected.operator)?.edge("outputType")?;
        let row = edit.type_id(edit.node(collection)?.edge("elementType")?)?;
        let ties = edit.node(self.equality)?.edge("comparator")?;
        let orders =
            project_topk_comparators(edit, [self.rank, ties], row, self.printed_row, self.used)?;
        let &[rank, ties] = orders.as_slice() else {
            return Err(Error::new("TopK requires rank and tie comparators"));
        };
        let equality = edit.derive_inner(
            self.equality,
            [("comparator", Field::Reference(ties.into()))],
        )?;
        TopK {
            producer: self.producer,
            rank: rank.into(),
            equality,
            limit: self.limit,
            numbering: self.numbering,
        }
        .append(edit, body, site, projected, collection)?;
        Ok(())
    }
}
