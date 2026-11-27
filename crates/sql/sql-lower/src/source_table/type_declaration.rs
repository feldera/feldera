//! Emit DBSP items for structured type declarations. Only a direct reference in a
//! declared attribute preserves the nominal struct. Table rows use expanded field types.
use super::column_layout::{field, struct_display, struct_hash, structure, ColumnLayout, Repr};
use super::{location, Error, Ids};
use crate::view::reference;
use feldera_sql_rel_model::{
    catalog::Name, operators::DiagnosticSource, statements::Attribute, types::TypeKind, TypeId,
    TypeTable,
};
use serde_json::{json, Value};
use std::fmt::Write;

pub(crate) fn declare_record(
    circuit: &mut Value,
    ids: &mut Ids,
    name: &Name,
    diagnostic: &DiagnosticSource,
    attributes: &[Attribute],
    types: &TypeTable,
    ty: TypeId,
) -> Result<(), Error> {
    let TypeKind::Row { fields, .. } = &types.types[ty.0 as usize].kind else {
        return Err(Error("structured declaration requires a row type".into()));
    };
    let declarations = circuit["declarations"]
        .as_array_mut()
        .expect("system circuit");
    let position = location(&diagnostic.origin);
    let mut signature = String::from("[");
    let mut members = Vec::with_capacity(fields.len());
    for (index, (attribute, member)) in attributes.iter().zip(fields).enumerate() {
        let declared = attribute.named_type.as_ref().and_then(|name| {
            declarations
                .iter()
                .map(|decl| &decl["item"]["type"])
                .find(|ty| ty["name"]["name"] == name.name)
        });
        let (ty, short) = if let Some(declared) = declared {
            let nullable = attribute.nullable == Some(true);
            let short = struct_display(
                declared["name"]["name"].as_str().unwrap(),
                declared["hashName"].as_str().unwrap(),
                nullable,
            );
            let ty = if nullable {
                // withMayBeNull copies the struct and shares its ordered Field objects.
                let mut result = ids.node("DBSPTypeStruct", &declared["position"]);
                for key in ["name", "hashName", "code"] {
                    result[key] = declared[key].clone();
                }
                result["fields"] = declared["fields"]
                    .as_array()
                    .expect("declared fields")
                    .iter()
                    .map(reference)
                    .collect::<Result<Vec<_>, _>>()?
                    .into();
                result["mayBeNull"] = true.into();
                result
            } else {
                reference(declared)?
            };
            (ty, short)
        } else {
            let ty = types
                .column_type(member.ty)
                .ok_or_else(|| Error("unsupported structured attribute type".into()))?;
            let layout = ColumnLayout::lower(&ty)?;
            (layout.emit(ids, Repr::Struct), layout.short)
        };
        write!(signature, "{}:{short},", attribute.name.name).expect("string write");
        members.push(field(ids, &position, &attribute.name, index as u32, ty));
    }
    signature.push(']');
    let ty = structure(
        ids,
        &position,
        name,
        members,
        struct_hash(&name.name, &signature),
        false,
    );
    let mut item = ids.node("DBSPStructItem", &position);
    item["type"] = ty;
    declarations.push(json!({"class":"DBSPDeclaration","id":ids.next_operator(),"item":item}));
    Ok(())
}
