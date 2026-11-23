use crate::monotonicity::row_projection::clock_operands;
use crate::{TemporalPredicate, WindowComparison, infer_temporal_predicates};
use feldera_sql_ir::{BinaryOp, Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field};
use serde_json::json;

#[test]
fn scalar_clock_bounds_follow_cast_and_function_rules() -> Result<(), Error> {
    let circuit = Circuit::from_json(json!({
        "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
    }))?;
    let (circuit, roots) = circuit.with_inner_roots([json!({
        "class":"DBSPTypeTimestamp","id":1,"code":"TIMESTAMP","mayBeNull":false
    })])?;
    let mut edit = CircuitEditor::new(circuit);
    let site = roots[0];
    let timestamp = edit.type_id(site)?;
    let row = edit.named_variable(timestamp, "row")?;
    let parameter = edit.parameter_at(site, row)?;
    let now = edit.function_path(site, "now")?;
    let clock = edit.make_expr(
        site,
        Expr::Apply,
        timestamp,
        [
            ("function", Field::Reference(now.into())),
            ("arguments", Field::Array(vec![])),
        ],
    )?;
    let bigint = edit.integer_type(timestamp, 64, true, false)?;
    let epoch = edit.cast(site, clock, bigint)?;
    let offset = edit.make_expr(
        site,
        Expr::I64Literal,
        bigint,
        [("value", Field::Data(86400.into()))],
    )?;
    let sum = edit.binary_expression(site, bigint, BinaryOp::Add, epoch, offset)?;
    assert!(
        clock_operands(&mut edit, parameter, sum)?
            .unwrap()
            .contains(&sum)
    );
    for factor in [-1, 0, 3] {
        let scale = edit.make_expr(
            site,
            Expr::I64Literal,
            bigint,
            [("value", Field::Data(factor.to_string().into()))],
        )?;
        let product = edit.binary_expression(site, bigint, BinaryOp::Mul, epoch, scale)?;
        assert_eq!(
            clock_operands(&mut edit, parameter, product)?
                .unwrap()
                .contains(&product),
            factor > 0
        );
    }
    let narrow = edit.integer_type(bigint, 16, true, true)?;
    for (safety, bounded) in [("SqlSafe", false), ("SqlUnsafe", true)] {
        let cast = edit.make_expr(
            site,
            Expr::Cast,
            narrow,
            [
                ("source", Field::Reference(epoch.into())),
                ("safe", Field::Data(safety.into())),
            ],
        )?;
        assert_eq!(
            clock_operands(&mut edit, parameter, cast)?
                .unwrap()
                .contains(&cast),
            bounded
        );
    }
    for (callee, bounded) in [
        ("extract_year_Timestamp", true),
        ("extract_minute_Timestamp", false),
    ] {
        let path = edit.function_path(site, callee)?;
        let call = edit.make_expr(
            site,
            Expr::Apply,
            bigint,
            [
                ("function", Field::Reference(path.into())),
                (
                    "arguments",
                    Field::Array(vec![Field::Reference(clock.into())]),
                ),
            ],
        )?;
        assert_eq!(
            clock_operands(&mut edit, parameter, call)?
                .unwrap()
                .contains(&call),
            bounded
        );
    }
    Ok(())
}

#[test]
fn clock_scaling_preserves_exact_and_float_signs() -> Result<(), Error> {
    let circuit = Circuit::from_json(json!({
        "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
    }))?;
    let (circuit, types) = circuit.with_inner_roots([
        json!({"class":"DBSPTypeDecimal","id":1,"code":"DECIMAL","precision":38,"scale":38,"mayBeNull":false}),
        json!({"class":"DBSPTypeReal","id":2,"code":"REAL","mayBeNull":false}),
        json!({"class":"DBSPTypeInteger","id":3,"code":"UINT128","width":128,"signed":false,"mayBeNull":false}),
    ])?;
    let mut edit = CircuitEditor::new(circuit);
    let decimal = edit.type_id(types[0])?;
    let real = edit.type_id(types[1])?;
    let wide = edit.type_id(types[2])?;
    let variable = edit.named_variable(decimal, "row")?;
    let parameter = edit.parameter_at(types[0], variable)?;
    for (kind, ty, value, positive) in [
        (Expr::DecimalLiteral, decimal, json!("1E-38"), true),
        (Expr::DecimalLiteral, decimal, json!("-1E-38"), false),
        (Expr::DecimalLiteral, decimal, json!("0E-38"), false),
        (Expr::U128Literal, wide, json!(u128::MAX.to_string()), true),
        (
            Expr::RealLiteral,
            real,
            json!(f32::NAN.to_bits() as i32),
            false,
        ),
        (
            Expr::RealLiteral,
            real,
            json!((-0.0_f32).to_bits() as i32),
            false,
        ),
        (Expr::RealLiteral, real, json!(1), true),
    ] {
        let literal = edit.make_expr(types[0], kind, ty, [("value", Field::Data(value))])?;
        // Both operands have bounds. MUL additionally requires a strictly positive factor.
        let product = edit.binary_expression(types[0], ty, BinaryOp::Mul, literal, literal)?;
        let bounded = clock_operands(&mut edit, parameter, product)?.unwrap();
        assert!(bounded.contains(&literal));
        assert_eq!(bounded.contains(&product), positive);
    }
    Ok(())
}

#[test]
fn nullable_clock_maximum_needs_a_nonnull_operand() -> Result<(), Error> {
    let circuit = Circuit::from_json(json!({
        "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
    }))?;
    let (circuit, types) = circuit.with_inner_roots([json!({
        "class":"DBSPTypeTimestamp","id":1,"code":"TIMESTAMP","mayBeNull":false
    })])?;
    let mut edit = CircuitEditor::new(circuit);
    let site = types[0];
    let timestamp = edit.type_id(site)?;
    let nullable = edit.with_nullability(timestamp, true)?;
    let inner_ty = edit.tuple_type(site, false, [nullable.into()])?;
    let input_ty = edit.tuple_type(site, false, [inner_ty.into()])?;
    let reference = edit.reference_type(input_ty)?;
    let row = edit.named_variable(reference, "row")?;
    let parameter = edit.parameter_at(site, row)?;
    let row_value = edit.dereference(row)?;
    let inner = edit.tuple_field(row_value, 0)?;
    let cloned_row = edit.make_expr(
        site,
        Expr::Clone,
        inner_ty,
        [("expression", Field::Reference(inner.into()))],
    )?;
    let key = edit.tuple_field(cloned_row, 0)?;
    let now = edit.function_path(site, "now")?;
    let clock = edit.make_expr(
        site,
        Expr::Apply,
        timestamp,
        [
            ("function", Field::Reference(now.into())),
            ("arguments", Field::Array(vec![])),
        ],
    )?;
    let present = edit.some_at(site, clock)?;
    let cloned = edit.make_expr(
        site,
        Expr::Clone,
        nullable,
        [("expression", Field::Reference(present.into()))],
    )?;
    let ordinary = edit.make_expr(
        site,
        Expr::Cast,
        nullable,
        [
            ("source", Field::Reference(clock.into())),
            ("safe", Field::Data("SqlUnsafe".into())),
        ],
    )?;
    let safe = edit.make_expr(
        site,
        Expr::Cast,
        nullable,
        [
            ("source", Field::Reference(clock.into())),
            ("safe", Field::Data("SqlSafe".into())),
        ],
    )?;
    for (operand, allowed) in [(ordinary, true), (safe, false), (cloned, false)] {
        for (op, bounded) in [
            (BinaryOp::MaxIgnoreNulls, allowed),
            (BinaryOp::Max, true),
            (BinaryOp::Coalesce, false),
        ] {
            let expr = edit.make_expr(
                site,
                Expr::Binary,
                nullable,
                [
                    ("opcode", Field::Data(op.wire_name().into())),
                    ("left", Field::Reference(key.into())),
                    ("right", Field::Reference(operand.into())),
                ],
            )?;
            let facts = clock_operands(&mut edit, parameter, expr)?.unwrap();
            assert_eq!(facts.contains(&expr), bounded, "{op:?}");
        }
    }
    Ok(())
}

#[test]
fn logarithm_clock_bound_requires_a_constant_double_base_above_one() -> Result<(), Error> {
    let circuit = Circuit::from_json(json!({
        "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
    }))?;
    let (circuit, roots) = circuit.with_inner_roots([
        json!({"class":"DBSPTypeDouble","id":1,"code":"DOUBLE","mayBeNull":false}),
        json!({"class":"DBSPTypeTimestamp","id":2,"code":"TIMESTAMP","mayBeNull":false}),
    ])?;
    let mut edit = CircuitEditor::new(circuit);
    let site = roots[0];
    let double = edit.type_id(site)?;
    let timestamp = edit.type_id(roots[1])?;
    let row = edit.named_variable(timestamp, "row")?;
    let parameter = edit.parameter_at(site, row)?;
    let now = edit.function_path(site, "now")?;
    let clock = edit.make_expr(
        site,
        Expr::Apply,
        timestamp,
        [
            ("function", Field::Reference(now.into())),
            ("arguments", Field::Array(vec![])),
        ],
    )?;
    let seconds = edit.integer_type(timestamp, 64, true, false)?;
    let epoch = edit.cast(site, clock, seconds)?;
    let value = edit.cast(site, epoch, double)?;
    let log = edit.function_path(site, "log_d_d")?;
    for base in [2.0_f64, 1.0, 0.5, -2.0, f64::NAN] {
        let literal = edit.make_expr(
            site,
            Expr::DoubleLiteral,
            double,
            [("value", Field::Data((base.to_bits() as i64).into()))],
        )?;
        let call = edit.make_expr(
            site,
            Expr::Apply,
            double,
            [
                ("function", Field::Reference(log.into())),
                (
                    "arguments",
                    Field::Array(vec![
                        Field::Reference(value.into()),
                        Field::Reference(literal.into()),
                    ]),
                ),
            ],
        )?;
        assert_eq!(
            clock_operands(&mut edit, parameter, call)?
                .unwrap()
                .contains(&call),
            base > 1.0
        );
    }
    Ok(())
}

#[test]
fn clock_minus_interval_needs_a_constant_nonnull_offset() -> Result<(), Error> {
    let circuit = Circuit::from_json(json!({
        "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
    }))?;
    let (circuit, roots) = circuit.with_inner_roots([
        json!({"class":"DBSPApplyExpression","id":1,"arguments":[],
            "type":{"class":"DBSPTypeTimestamp","id":2,"code":"TIMESTAMP","mayBeNull":false},
            "function":{"class":"DBSPPathExpression","id":3,"path":{"class":"DBSPPath","id":4,
                "components":[{"class":"DBSPSimplePathSegment","id":5,"identifier":"now","genericArgs":[]}]}}}),
        json!({"class":"DBSPShortIntervalLiteral","id":6,"value":"1000",
            "type":{"class":"DBSPTypeShortInterval","id":7,"code":"INTERVAL_SHORT","units":"SECONDS","mayBeNull":false}}),
        json!({"class":"DBSPShortIntervalLiteral","id":8,
            "type":{"class":"DBSPTypeShortInterval","id":9,"code":"INTERVAL_SHORT","units":"SECONDS","mayBeNull":true}}),
    ])?;
    let mut edit = CircuitEditor::new(circuit);
    let clock = edit.expression(roots[0])?;
    let timestamp = edit.expression_type(clock)?;
    let row = edit.tuple_type(roots[0], false, [timestamp.into()])?;
    let reference = edit.reference_type(row)?;
    let row = edit.named_variable(reference, "row")?;
    let parameter = edit.parameter_at(roots[0], row)?;
    let value = edit.dereference(row)?;
    let key = edit.tuple_field(value, 0)?;
    let boolean = edit.bool_type(roots[0], false)?;
    for (offset, is_bound) in [(roots[1], true), (roots[2], false)] {
        let bound = edit.make_expr(
            roots[0],
            Expr::TimeAddSub,
            timestamp,
            [
                ("left", Field::Reference(clock.into())),
                ("right", Field::Reference(offset)),
                ("opcode", Field::Data("SUB".into())),
            ],
        )?;
        let body = edit.binary_expression(roots[0], boolean, BinaryOp::Gte, key, bound)?;
        let expected = if is_bound {
            TemporalPredicate::Bound {
                key,
                clock: bound,
                comparison: WindowComparison::GreaterEqual,
            }
        } else {
            TemporalPredicate::Residual(body)
        };
        assert_eq!(
            infer_temporal_predicates(&mut edit, parameter, body, &mut 0)?,
            Some(vec![expected])
        );
    }
    // Shadowing the same name resolves an initializer in the enclosing
    // scope and its consumer in the new scope.
    let first = edit.let_statement("t", clock)?;
    let read_clock = edit.named_variable(timestamp, "t")?;
    let inner = edit.named_variable(timestamp, "t")?;
    let read_inner = edit.named_variable(timestamp, "t")?;
    let shadow = edit.let_expression(inner, read_clock, read_inner)?;
    let block = edit.make_expr(
        roots[0],
        Expr::Block,
        timestamp,
        [
            ("contents", Field::Array(vec![Field::Reference(first)])),
            ("lastExpression", Field::Reference(shadow.into())),
        ],
    )?;
    let comparison = edit.binary_expression(roots[0], boolean, BinaryOp::Gt, key, block)?;
    assert_eq!(
        infer_temporal_predicates(&mut edit, parameter, comparison, &mut 0)?,
        Some(vec![TemporalPredicate::Bound {
            key,
            clock: block,
            comparison: WindowComparison::Greater
        }])
    );
    let interval = edit.expression(roots[1])?;
    let interval_type = edit.expression_type(interval)?;
    let local = edit.named_variable(interval_type, "offset")?;
    let offset = edit.named_variable(interval_type, "offset")?;
    let difference = edit.make_expr(
        roots[0],
        Expr::TimeAddSub,
        timestamp,
        [
            ("left", Field::Reference(clock.into())),
            ("right", Field::Reference(offset.into())),
            ("opcode", Field::Data("SUB".into())),
        ],
    )?;
    let expr = edit.let_expression(local, interval, difference)?;
    assert!(
        !clock_operands(&mut edit, parameter, expr)?
            .unwrap()
            .contains(&expr)
    );
    // A reference outside the filter's binding needs lexical resolution.
    let other = edit.named_variable(reference, "other")?;
    let other_parameter = edit.parameter_at(roots[0], other)?;
    assert!(clock_operands(&mut edit, other_parameter, key)?.is_none());
    Ok(())
}
