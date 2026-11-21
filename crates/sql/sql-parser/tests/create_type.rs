use feldera_sql_parser::{emit_statements, parse, Statement, TypeDefinition};

#[test]
fn retains_type_aliases_attribute_nullability_defaults_and_ranges() {
    let source = "CREATE TYPE int32 AS INTEGER;\nCREATE OR REPLACE TYPE s.t AS (id int32 NOT NULL, \"val\" VARCHAR NULL DEFAULT 'x', n INT)";
    let statements = parse(source).unwrap();
    let [Statement::CreateType(alias), Statement::CreateType(row)] = statements.as_slice() else {
        panic!("expected two type definitions");
    };
    assert!(matches!(alias.definition, TypeDefinition::Alias(_)));
    let TypeDefinition::Struct(fields) = &row.definition else {
        panic!("expected struct");
    };
    assert!(row.replace);
    assert_eq!(row.name.to_string(), "s.t");
    assert_eq!(row.span.start.line, 2);
    assert_eq!(
        fields.iter().map(|f| f.nullable).collect::<Vec<_>>(),
        [Some(false), Some(true), None]
    );
    assert_eq!(fields[1].default.as_ref().unwrap().to_string(), "'x'");
    let emitted = emit_statements(&statements).unwrap();
    assert!(emitted[1].contains("\"val\" VARCHAR NULL DEFAULT 'x'"));
    assert_eq!(
        emit_statements(&parse(&emitted.join(";")).unwrap()).unwrap(),
        emitted
    );
}

#[test]
fn rejects_incomplete_and_foreign_type_declarations() {
    for sql in [
        "CREATE TYPE empty_row AS ()",
        "CREATE TYPE x",
        "CREATE TYPE x AS (a INT NOT)",
        "CREATE TYPE x AS (a INT NULL NOT NULL)",
        "CREATE TYPE x AS ENUM ('a')",
        "CREATE TYPE x AS (SELECT INT)",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn row_fields_keep_their_modifiers_across_ast_emission() {
    let sql = "CREATE TABLE data(r ROW(i INT NOT NULL, n ROW(a VARCHAR NULL) ARRAY))";
    let emitted = emit_statements(&parse(sql).unwrap()).unwrap();
    assert!(
        emitted[0].contains("ROW(i INT NOT NULL, n ROW(a VARCHAR NULL) ARRAY)"),
        "{emitted:?}"
    );
    assert_eq!(
        emit_statements(&parse(&emitted[0]).unwrap()).unwrap(),
        emitted
    );
}
