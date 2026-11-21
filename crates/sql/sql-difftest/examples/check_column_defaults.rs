//! Compare Java before/after column-default graphs, including the next generated variable name.
use feldera_sql_difftest::compare_circuits;
use feldera_sql_expr::{prepare_column_default, Rewrite, Rule};
use feldera_sql_ir::{Circuit, CircuitEditor};
use serde_json::{json, Value};
use std::io::{self, BufRead};

fn as_circuit(expression: &Value) -> Value {
    json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
        {"class":"DBSPConstantOperator","id":1,"inputs":[],"function":expression}
    ]})
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut seen = 0;
    let mut differences = 0;
    for line in io::stdin().lock().lines() {
        let case: Value = serde_json::from_str(&line?)?;
        let input = Circuit::from_json(as_circuit(&case["before"]))?;
        let mut next = 0;
        let actual = CircuitEditor::new(input).rewrite_inner_roots(|editor, id| {
            if case["kind"] == "struct" {
                return Rewrite::new(Rule::EliminateStructs).apply(editor, id);
            }
            let expression = editor.expression(id)?;
            prepare_column_default(editor, expression, &mut next).map(Into::into)
        });
        let difference = match actual {
            Err(e) => Some(e.to_string()),
            Ok(result) => compare_circuits(&as_circuit(&case["after"]), &result.to_json())
                .err()
                .map(|e| e.to_string()),
        };
        if difference.is_some() || case["next_name"].as_u64() != Some(next) {
            eprintln!(
                "{}",
                json!({"id":case["id"], "difference":difference, "next_name":next})
            );
            differences += 1;
        }
        seen += 1;
    }
    println!("{}", json!({"compared":seen,"differences":differences}));
    if seen == 0 || differences > 0 {
        return Err("column default comparison failed".into());
    }
    Ok(())
}
