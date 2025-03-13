use feldera_sqllib::{SqlString, Variant};
use std::collections::BTreeMap;
use std::sync::Arc;

pub fn check_condition(
    condition: Option<SqlString>,
    subject_properties: Option<Variant>,
    resource_properties: Option<Variant>,
) -> Result<Option<bool>, Box<dyn std::error::Error>> {
    Ok(do_check_condition(
        condition,
        subject_properties,
        resource_properties,
    ))
}

pub fn do_check_condition(
    condition: Option<SqlString>,
    subject_properties: Option<Variant>,
    resource_properties: Option<Variant>,
) -> Option<bool> {
    let condition = condition?;
    let subject_properties = subject_properties?;
    let resource_properties = resource_properties?;

    // NOTE: compiling the JMESPath expression on each invocation is highly inefficient.
    // A better solution is to maintain a cache of pre-compiled expressions and only invoke
    // compilation in case of a cache miss.
    let expr = jmespath::compile(condition.str())
        .map_err(|e| println!("invalid jmes expression: {e}"))
        .ok()?;
    let all_properties = Variant::Map(Arc::new(BTreeMap::from([
        (Variant::String(SqlString::from_ref("subject")), subject_properties),
        (Variant::String(SqlString::from_ref("resource")), resource_properties),
    ])));

    let result = expr
        .search(all_properties)
        .map_err(|e| println!("error evaluating jmes expression: {e}"))
        .ok()?;
    Some(result.as_ref() == &jmespath::Variable::Bool(true))
}
