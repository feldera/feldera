//! A DBSP closure parameter borrows an anonymous tuple. Each field read has
//! its own dereference node and shares the variable binding with other reads.
use super::{
    source_table::{Ids, RowLayout},
    view::reference,
    Error,
};
use serde_json::{json, Value};

pub(super) struct RowParameter {
    name: String,
    ty: Value,
    tuple: Value,
    variable: Value,
    fields: Vec<Value>,
}

impl RowParameter {
    pub(super) fn new(ids: &mut Ids, layout: &RowLayout<'_>, ordinal: u64) -> Result<Self, Error> {
        let position = super::source_table::range(0, 0, 0, 0);
        let tuple = layout.tuple(ids, &position, true);
        let tuple_ref = reference(&tuple)?;
        let fields = tuple["tupFields"]
            .as_array()
            .expect("constructed tuple")
            .iter()
            .map(reference)
            .collect::<Result<_, _>>()?;
        let mut ty = ids.node("DBSPTypeRef", &position);
        ty["type"] = tuple;
        ty["mutable"] = false.into();
        ty["code"] = "REF".into();
        ty["mayBeNull"] = false.into();
        let name = format!("t_{ordinal}");
        let mut variable = ids.generated("DBSPVariablePath");
        variable["type"] = reference(&ty)?;
        variable["variable"] = name.clone().into();
        Ok(Self {
            name,
            ty,
            tuple: tuple_ref,
            variable,
            fields,
        })
    }

    pub(super) fn read(&mut self, ids: &mut Ids, index: usize) -> Result<Value, Error> {
        let field_type = self
            .fields
            .get(index)
            .ok_or_else(|| Error("field outside input row".into()))?;
        let mut deref = ids.generated("DBSPDerefExpression");
        let var_ref = reference(&self.variable)?;
        deref["expression"] = std::mem::replace(&mut self.variable, var_ref);
        deref["type"] = self.tuple.clone();
        let mut field = ids.generated("DBSPFieldExpression");
        field["expression"] = deref;
        field["type"] = field_type.clone();
        field["fieldNo"] = index.into();
        Ok(field)
    }

    pub(super) fn close(
        self,
        ids: &mut Ids,
        result: Value,
        body: Value,
        position: &Value,
    ) -> Result<Value, Error> {
        let mut param = ids.generated("DBSPParameter");
        param["type"] = reference(&self.ty)?;
        param["name"] = self.name.into();
        let mut ty = ids.generated("DBSPTypeFunction");
        ty["resultType"] = result;
        ty["parameterTypes"] = json!([self.ty]);
        ty["code"] = "FUNCTION".into();
        ty["mayBeNull"] = false.into();
        let mut closure = ids.node("DBSPClosureExpression", position);
        closure["type"] = ty;
        closure["parameters"] = json!([param]);
        closure["body"] = body;
        Ok(closure)
    }
}
