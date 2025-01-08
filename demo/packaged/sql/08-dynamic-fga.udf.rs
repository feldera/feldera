use feldera_sqllib::Variant;
use std::collections::BTreeMap;

pub fn check_condition(
    condition: Option<String>,
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
    condition: Option<String>,
    subject_properties: Option<Variant>,
    resource_properties: Option<Variant>,
) -> Option<bool> {
    let condition = condition?;
    let subject_properties = subject_properties?;
    let resource_properties = resource_properties?;

    // NOTE: compiling the JMESPath expression on each invocation is highly inefficient.
    // A better solution is to maintain a cache of pre-compiled expressions and only invoke
    // compilation in case of a cache miss.
    let expr = jmespath::compile(&condition)
        .map_err(|e| println!("invalid jmes expression: {e}"))
        .ok()?;
    let all_properties = Variant::Map(BTreeMap::from([
        (Variant::String("subject".to_string()), subject_properties),
        (Variant::String("resource".to_string()), resource_properties),
    ]));

    let result = expr
        .search(all_properties)
        .map_err(|e| println!("error evaluating jmes expression: {e}"))
        .ok()?;
    Some(result.as_ref() == &jmespath::Variable::Bool(true))
}
