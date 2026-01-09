use feldera_sqllib::*;

use std::sync::Arc;
use std::collections::{BTreeMap, VecDeque};
use xlformula_engine::calculate;
use xlformula_engine::parse_formula;
use xlformula_engine::NoCustomFunction;
use xlformula_engine::types::{Formula, Value, Error, Boolean};
use chrono::DateTime;

fn parse_as_value(input: SqlString) -> Value {
    if let Ok(number) = input.str().parse::<f32>() {
        return Value::Number(number);
    }
    if let Ok(boolean) = input.str().parse::<bool>() {
        return Value::Boolean(if boolean { Boolean::True } else { Boolean::False });
    }
    if let Ok(date) = DateTime::parse_from_rfc3339(input.str()) {
        return Value::Date(date);
    }
    Value::Text(String::from(input.str()))
}

pub fn cell_value(raw_content: Option<SqlString>, mentions_ids: Option<Arc<Vec<Option<i64>>>>, mentions_values: Option<Arc<Vec<Option<SqlString>>>>) -> Result<Option<SqlString>, Box<dyn std::error::Error>> {
    let cell_content = raw_content.unwrap_or_else(|| SqlString::new());
    let formula = parse_formula::parse_string_to_formula(cell_content.str(), None::<NoCustomFunction>);

    let mentions_ids = mentions_ids.map(Arc::unwrap_or_clone).unwrap_or_else(|| vec![]);
    let mentions_values = mentions_values.map(Arc::unwrap_or_clone).unwrap_or_else(|| vec![]);
    assert_eq!(mentions_ids.len(), mentions_values.len());
    let mut context = BTreeMap::new();
    for (id, value) in mentions_ids.into_iter().zip(mentions_values.into_iter()) {
        if let (Some(id), Some(value)) = (id, value) {
            context.insert(id_to_cell_reference(id), parse_as_value(value));
        }
    }
    let data_function = |s: String| context.get(&s).cloned().unwrap_or_else(|| Value::Error(Error::Value));

    let result = calculate::calculate_formula(formula, Some(&data_function));
    let result_str = calculate::result_to_string(result);
    Ok(Some(SqlString::from(result_str)))
}

fn cell_references_to_ids(crf: &str) -> Option<i64> {
    let mut col = 0;
    let mut row = 0;
    for c in crf.chars() {
        if c.is_ascii_alphabetic() {
            col = col * 26 + (c.to_ascii_uppercase() as i64 - 'A' as i64);
        } else if c.is_ascii_digit() {
            row = row * 10 + (c as i64 - '0' as i64);
        } else {
            return None;
        }
    }
    Some(col + row * 26)
}

fn id_to_cell_reference(id: i64) -> String {
    let mut col = id % 26;
    let row = id / 26;
    let mut result = String::new();
    while col >= 0 {
        result.push((col as u8 + 'A' as u8) as char);
        col = col / 26 - 1;
    }
    result.push_str(&row.to_string());
    result
}

pub fn mentions(raw_content: Option<SqlString>) -> Result<Option<Arc<Vec<Option<i64>>>>, Box<dyn std::error::Error>> {
    let cell_content = raw_content.unwrap_or_else(|| SqlString::new());
    let formula = parse_formula::parse_string_to_formula(cell_content.str(), None::<NoCustomFunction>);

    let mut formulas = VecDeque::from(vec![formula]);
    let mut references = vec![];

    while !formulas.is_empty() {
        let formula = formulas.pop_front().unwrap();
        match formula {
            Formula::Reference(reference) => {
                references.push(reference);
            },
            Formula::Iterator(iterator) => {
                formulas.extend(iterator);
            },
            Formula::Operation(expression) => {
                formulas.extend(expression.values);
            },
            _ => {}
        }
    }
    let mut cell_ids: Vec<Option<i64>> = references.iter().map(|r| cell_references_to_ids(r)).collect();
    cell_ids.sort_unstable();

    Ok(Some(Arc::new(cell_ids)))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn init_tracing() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    #[test]
    fn cell_ref_id() {
        assert_eq!(cell_references_to_ids("A0"), Some(0));
        assert_eq!(cell_references_to_ids("A1"), Some(26));
        assert_eq!(cell_references_to_ids("A2"), Some(52));
        assert_eq!(cell_references_to_ids("B0"), Some(1));
        assert_eq!(cell_references_to_ids("C0"), Some(2));
        assert_eq!(cell_references_to_ids("Z0"), Some(25));
        assert_eq!(cell_references_to_ids("Z100"), Some(100*26 + 25));
        assert_eq!(cell_references_to_ids("Z100"), Some(100*26 + 25));
        assert_eq!(cell_references_to_ids("Z10000000"), Some(260000025));

        assert_eq!(id_to_cell_reference(0), "A0".to_string());
        assert_eq!(id_to_cell_reference(26), "A1".to_string());
        assert_eq!(id_to_cell_reference(52), "A2".to_string());
        assert_eq!(id_to_cell_reference(1), "B0".to_string());
        assert_eq!(id_to_cell_reference(2), "C0".to_string());
        assert_eq!(id_to_cell_reference(25), "Z0".to_string());
        assert_eq!(id_to_cell_reference(100*26 + 25), "Z100".to_string());
        assert_eq!(id_to_cell_reference(100*26 + 25), "Z100".to_string());
        assert_eq!(id_to_cell_reference(260000025), "Z10000000".to_string());
        assert_eq!(id_to_cell_reference(1_040_000_000-1), "Z39999999".to_string());
    }

    #[test]
    fn mentions_empty() {
        init_tracing();

        let result = mentions(Some("".to_string())).unwrap().unwrap();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn mentions_one() {
        init_tracing();
        let result = mentions(Some("=A1".to_string())).unwrap().unwrap();
        assert_eq!(result, vec![Some(26)]);
    }

    #[test]
    fn mentions_two() {
        init_tracing();
        let result = mentions(Some("=A1+A2".to_string())).unwrap().unwrap();
        assert_eq!(result, vec![Some(26), Some(52)]);
    }

    #[test]
    fn mentions_set() {
        init_tracing();
        let result = mentions(Some("=SUM(A0, A10)".to_string())).unwrap().unwrap();
        assert_eq!(result, vec![Some(0), Some(26*10)]);
    }


    #[test]
    fn empty() {
        let result = cell_value(Some("".to_string()), None, None).unwrap().unwrap();
        assert_eq!(result, String::new());
    }

    #[test]
    fn non_formula() {
        let result = cell_value(Some("just a text".to_string()), None, None).unwrap().unwrap();
        assert_eq!(result, "just a text".to_string());
    }

    #[test]
    fn math() {
        let result = cell_value(Some("=(1*(2+3))*2".to_string()), None, None).unwrap().unwrap();
        assert_eq!(result, "10");
    }
}
