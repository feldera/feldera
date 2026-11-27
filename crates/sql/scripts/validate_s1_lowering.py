#!/usr/bin/env python3
"""Lower the same typed S1 program with Java and Rust and resume both S2 checkpoints."""

import argparse
import json
from pathlib import Path
import subprocess

from benchmark_bound_gen2 import compare, direct
from compile_bound_gen2 import compile_program
from run_determinism import file_digest
from validate_table_lowering import compare_checkpoints
from validate_bound_programs import CASES as BINDING_CASES

CASES = {
    **{name: BINDING_CASES[name] for name in (
        "set_minus", "set_minus_all", "set_mixed", "set_intersect_all", "set_except_chain", "set_intersect_chain",
        "set_union", "set_union_all", "set_chain", "set_filter", "set_identity_names",
        "scalar_assignment_float", "scalar_assignment_text", "scalar_assignment_bool", "scalar_assignment_numeric",
        "scalar_assignment_narrow", "scalar_assignment_date_text", "scalar_assignment_binary_text",
        "record_temporal", "record_nominal", "record_quoted",
        "record_declaration", "record_fields", "record_nested", "record_forward", "record_inline", "record_warnings",
        "uuid_variant_columns", "uuid_literal", "uuid_insert", "variant_assignments",
        "alias_forward_use", "alias_chain", "alias_collections", "alias_warnings", "alias_quoted", "alias_only",
        "temporal_fraction", "temporal_calendar", "temporal_precision_cap", "temporal_insert",
        "type_aliases", "type_alias_collections", "type_standard_spellings", "type_unspecified_decimal",
        "decimal_projection", "decimal_strings", "decimal_widening", "decimal_bigint", "decimal_precision_cap", "decimal_assignment",
        "assignment_numeric", "assignment_warnings", "assignment_warning_order", "assignment_unsigned", "assignment_dates", "assignment_date_bounds",
        "collection_identity", "collection_nested", "collection_project", "collection_shared", "collection_precision",
        "constant_after_insert", "constant_fields", "constant_literal_types", "constant_folded_fields", "constant_field_names",
        "constant_field_list", "local_constant", "materialized_constant", "view_kinds", "view_chain", "constant_view_input",
        "scalar_widths", "temporal_precision", "explicit_view_columns", "named_constant",
        "inline_key", "composite_key", "interleaved_key", "insert_values", "insert_permutation",
        "insert_heterogeneous_permutation", "insert_widths", "insert_null")},
    "two_constants": "CREATE VIEW a AS SELECT 5 AS first;\nCREATE VIEW b AS SELECT 7 AS second;",
    "table_and_constant": "CREATE TABLE t(x INT);\nCREATE VIEW v AS SELECT 42 AS answer;",
    "folded_tangent": "CREATE VIEW vv AS SELECT TAN(1);",
    "zero_tangent": "CREATE VIEW vv AS SELECT TAN(0);",
    "constant": "CREATE VIEW answer AS SELECT 42 AS answer_value;",
    "character_date": "CREATE TABLE records(id INT NOT NULL, day DATE, label VARCHAR(24), tag CHAR(3));\n"
                      "CREATE VIEW result AS SELECT label, day, tag FROM records WHERE id > 2;",
    "string_projection": "CREATE TABLE names(id INT, name VARCHAR, prefix CHAR);\n"
                         "CREATE VIEW labels AS SELECT prefix, name FROM names;",
    "repeated_column": "CREATE TABLE sensors(id INT NOT NULL, y BIGINT);\n"
                       "CREATE VIEW visible AS SELECT id, id AS another FROM sensors;",
    "catalog": "CREATE TABLE sensors(x INT NOT NULL, y BIGINT);",
    "identity": "CREATE TABLE sensors(x INT NOT NULL, y BIGINT);\nCREATE VIEW visible AS SELECT * FROM sensors;",
    "early_view": "CREATE TABLE input_rows(x DOUBLE, y DOUBLE, n DECIMAL(10,2));\n"
                  "CREATE VIEW a AS SELECT x,y,n FROM input_rows;",
    "shared": "CREATE TABLE events(k INT, v BIGINT);\nCREATE VIEW first_view AS SELECT * FROM events;\n"
              "CREATE VIEW second_view AS SELECT * FROM events;",
    "projection": "CREATE TABLE sensors(id INT, x INT NOT NULL, y BIGINT);\n"
                  "CREATE VIEW visible AS SELECT y AS reading, id FROM sensors;",
    "two_projections": "CREATE TABLE sensors(id INT, x INT NOT NULL, y BIGINT);\n"
                       "CREATE VIEW visible AS SELECT id, y FROM sensors;\n"
                       "CREATE VIEW xray AS SELECT id, x FROM sensors;",
    "filtered_projection": "CREATE TABLE readings (sensor INTEGER NOT NULL, temperature INTEGER NOT NULL);\n"
                           "CREATE VIEW hot AS SELECT sensor FROM readings WHERE temperature > 20;",
    "nullable_filter": "CREATE TABLE t(x INT); CREATE VIEW result AS SELECT * FROM t WHERE x > 10;",
    "column_comparison": "CREATE TABLE t(x INT NOT NULL, y INT NOT NULL);\n"
                         "CREATE VIEW result AS SELECT * FROM t WHERE x <= y;",
    "shared_filters": "CREATE TABLE t(x INT NOT NULL, y INT);\n"
                      "CREATE VIEW a AS SELECT y FROM t WHERE x > 2;\n"
                      "CREATE VIEW b AS SELECT x FROM t WHERE x <> 4;",
    "bool_projection": "CREATE TABLE flags(id INT, a BOOLEAN, b BOOLEAN NOT NULL);\n"
                       "CREATE VIEW state AS SELECT b, a FROM flags;",
    "float_projection": "CREATE TABLE samples(a DOUBLE, b DOUBLE, c DECIMAL(10,2));\n"
                        "CREATE VIEW result AS SELECT c, b FROM samples;",
    "reverse_reads": "CREATE TABLE a(x INT);\nCREATE TABLE b(y BIGINT);\n"
                     "CREATE VIEW z AS SELECT * FROM b;\nCREATE VIEW c AS SELECT * FROM a;",
    "two_tables": "CREATE TABLE sensors(x INT);\nCREATE VIEW visible AS SELECT x FROM sensors;\n"
                  "CREATE TABLE events(id BIGINT);\nCREATE VIEW traffic AS SELECT id FROM events;",
}


def validate(output, compiler, comparator, classpath):
    output.mkdir()
    records = []
    for case, sql in CASES.items():
        directory = output / case
        directory.mkdir()
        source = directory / "input.sql"
        source.write_text(sql)
        bound = directory / "input.s1.json"
        with bound.open("xb") as stream:
            subprocess.run([compiler, "--emit-s1", source], stdout=stream, check=True)
        revision = "detached-lowering"
        java = ["java", "-ea", "-cp", classpath]
        golden = directory / "java"
        subprocess.run([*java, "org.dbsp.sqlCompiler.compiler.sql.tools.BoundProgramReplay",
                        bound, golden, revision, "S2"], check=True)
        actual = directory / "rust.s2.json"
        with actual.open("xb") as stream:
            subprocess.run([compiler, "--lower-s1", revision, revision, bound], stdout=stream, check=True)
        compare_checkpoints(directory, golden / "s2.json", actual, comparator)
        if case == "catalog":
            alias = directory / "alias.s2.json"
            alias.symlink_to(actual.name)
            before = file_digest(actual)
            collision = subprocess.run([*java, "org.dbsp.sqlCompiler.CompilerMain", "--gen2",
                                        "--dump-producer-commit", revision, "--dump-runtime-abi", revision,
                                        "--read-s2", actual, "--write-s3", alias],
                                       capture_output=True)
            if (collision.returncode == 0 or file_digest(actual) != before
                    or b"The S3 output must differ from the S2 input" not in collision.stderr + collision.stdout):
                raise ValueError("Overlapping S2 input and S3 output must preserve the checkpoint")
        for label, args in (("direct", [source]), ("java-resumed", ["--read-s2", golden / "s2.json"]),
                            ("rust-resumed", ["--read-s2", actual])):
            artifacts = directory / label
            artifacts.mkdir()
            subprocess.run([*java, "org.dbsp.sqlCompiler.CompilerMain", "--gen2",
                            "--dump-producer-commit", revision, "--dump-runtime-abi", revision,
                            "--errors", artifacts / "diagnostics.txt", "-o", artifacts / "circuit.json",
                            "--js", artifacts / "schema.json", "--dataflow", artifacts / "dataflow.json",
                            *args], check=True)
        expected, resumed = directory / "direct", directory / "rust-resumed"
        subprocess.run([comparator, expected / "circuit.json", resumed / "circuit.json"], check=True)
        for artifact in ("schema.json", "dataflow.json"):
            if json.loads((expected / artifact).read_bytes()) != json.loads((resumed / artifact).read_bytes()):
                raise ValueError(f"{case}: {artifact} differs after Rust lowering")
        if (resumed / "diagnostics.txt").read_bytes() != (directory / "java-resumed/diagnostics.txt").read_bytes():
            raise ValueError(f"{case}: diagnostics differ after Rust lowering")
        reference = directory / "java-compiled"
        candidate = directory / "rust-lowered"
        direct(source, reference, classpath)
        compile_program(source, candidate, compiler, classpath, revision, rust_lowering=True)
        compare(reference, candidate, comparator)
        records.append({"case": case, "source_sha256": file_digest(source), "s1_sha256": file_digest(bound),
                        "rust_s2_sha256": file_digest(actual), "java_s2_sha256": file_digest(golden / "s2.json")})
    (output / "complete.json").write_text(json.dumps({"equal": True, "cases": records}, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", type=Path, required=True)
    parser.add_argument("--comparator", type=Path, required=True)
    parser.add_argument("--classpath", required=True)
    args = parser.parse_args()
    validate(args.output, args.compiler, args.comparator, args.classpath)
