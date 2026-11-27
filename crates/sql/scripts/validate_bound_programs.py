#!/usr/bin/env python3
"""Compare Rust binding and direct Java binding through the entire downstream compiler."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from benchmark_bound_gen2 import compare
from compile_bound_gen2 import compile_program

METERS = "CREATE TABLE meters (sensor INT NOT NULL, reading INT);\nCREATE VIEW totals AS "

CASES = {
    "type_unspecified_decimal": "CREATE TABLE t(a DECIMAL, b DEC, c NUMERIC, d DECIMAL ARRAY, e MAP<NUMERIC,DECIMAL>);"
                                "\nINSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL);"
                                "\nCREATE TABLE s(d NUMERIC, e TIMESTAMP(2)); CREATE VIEW v AS SELECT * FROM s;",
    "type_aliases": "CREATE TABLE t(s TEXT, x STRING, b BYTEA, i INT2, j INT4, k INT8,"
                    " l INT64, m SIGNED, q BOOL, f FLOAT4, g FLOAT32, h FLOAT8, z FLOAT64, n NUMBER);"
                    "\nCREATE VIEW v AS SELECT b, i, q, f, z, n FROM t;",
    "type_alias_collections": "CREATE TABLE t(x INT2 ARRAY, d MAP<TEXT, FLOAT8 ARRAY>);"
                              "\nCREATE VIEW v AS SELECT d, x FROM t;",
    "type_standard_spellings": "CREATE TABLE t(x NUMERIC(38,10), y DECIMAL(8), z DEC(5,1),"
                               " d DOUBLE PRECISION, a TIME(2) WITHOUT TIME ZONE, b TIMESTAMP(0) WITHOUT TIME ZONE);",
    "decimal_projection": "CREATE VIEW v AS SELECT 1.00 AS a, .1 AS b, 000.0040 AS c,"
                          " -.00000012 AS d, -0.00 AS e, 2147483648. AS f;",
    "decimal_strings": "CREATE VIEW v AS SELECT DECIMAL '-001.00' AS a, DECIMAL '.00120' AS b,"
                       " DECIMAL '1E2' AS c, DECIMAL '00.0000000' AS z;",
    "decimal_widening": "CREATE TABLE t(d DECIMAL(13,3)); INSERT INTO t VALUES (1.23), (2.5), (42), (NULL), (-0.001);",
    "decimal_bigint": "CREATE TABLE t(d DOUBLE); INSERT INTO t VALUES (3), (2.0), (2147483648);",
    "decimal_precision_cap": "CREATE TABLE t(d DECIMAL(38,0)); INSERT INTO t VALUES"
                             " (12345678901234567890123456789012345678), (.99), (-.99), (.5), (-.501);",
    "decimal_assignment": "CREATE TABLE t(i INT, n DECIMAL(3,1)); INSERT INTO t VALUES"
                          " (123456789000.1, 2322.345), (-19.2, -100.55), (1.19, -9.999);",
    "assignment_numeric": "CREATE TABLE t(d DATE, i BIGINT, n DECIMAL(7,2), c CHAR(4), f REAL, g DOUBLE);"
                          "\nINSERT INTO t VALUES ('2020-02-29', '+123', -124, 'abcdef', 1, 9),"
                          " ('2000-01-01', '-2', 5, 'x', 0, 2);",
    "assignment_warnings": "CREATE TABLE t(i SMALLINT, d DATE, n DECIMAL(3,2));"
                           "\nINSERT INTO t VALUES ('-32769', '2020-02-30', 55),"
                           " ('hello', 'Bob''s', 42), ('hello', 'Bob''s', 42);"
                           "\nINSERT INTO t VALUES ('hello', 'Bob''s', 42);",
    "assignment_warning_order": "CREATE TABLE t(a INT, b INT); INSERT INTO t(b, a) VALUES ('one', 'two'), ('three', 'two');",
    "assignment_unsigned": "CREATE TABLE t(u INT UNSIGNED, i INT);"
                           "\nINSERT INTO t VALUES ('-0', '  +0002 '), ('4000000000', '03');",
    "assignment_dates": "CREATE TABLE t(d DATE); INSERT INTO t VALUES (DATE '2020-01-31'),"
                        " (DATE '1969-01-01'), (NULL); CREATE VIEW v AS SELECT * FROM t;",
    "assignment_date_bounds": "CREATE VIEW v AS SELECT DATE '0001-01-01' AS first_day,"
                              " DATE '9999-12-31' AS last_day, DATE '2024-02-29' AS leap_day;",
    "collection_identity": "CREATE TABLE t(a INT ARRAY); CREATE VIEW v AS SELECT * FROM t;",
    "collection_nested": "CREATE TABLE t(a INT ARRAY NOT NULL, b VARCHAR(4) ARRAY ARRAY NOT NULL, c INT ARRAY NOT NULL);",
    "collection_project": "CREATE TABLE t(m MAP<VARCHAR, INT> NOT NULL, n MAP<INT, VARCHAR ARRAY>);"
                          "\nCREATE VIEW v AS SELECT n, m FROM t;",
    "collection_shared": "CREATE TABLE t(a DOUBLE ARRAY, b MAP<INT, DOUBLE ARRAY>);"
                         "\nCREATE LOCAL VIEW intermediate AS SELECT b, a FROM t;"
                         "\nCREATE VIEW v AS SELECT a, a AS again FROM intermediate;",
    "collection_precision": "CREATE TABLE t(a TIME(2) ARRAY, b MAP<TIMESTAMP(3), TIME(2)>);",
    "constant_after_insert": "CREATE TABLE t(x INT); INSERT INTO t VALUES (3);"
                             "\nCREATE VIEW v AS SELECT 1 AS x, 1 + 1 AS y;",
    "constant_fields": "CREATE VIEW v AS SELECT 1 AS a, 2 AS b, TRUE AS c, 'Bob''s' AS d;",
    "constant_literal_types": "CREATE VIEW v AS SELECT NULL AS n, X'0f' AS b, 2147483648 AS i;",
    "constant_folded_fields": "CREATE VIEW v AS SELECT 1 + 2 AS x, TAN(1) AS t, -4 AS d;",
    "constant_field_names": 'CREATE VIEW v AS SELECT TRUE AS "True", FALSE AS "lower", 1, 1;',
    "constant_field_list": "CREATE VIEW v(a, b) AS SELECT 1, 'abc';",
    "insert_values": "CREATE TABLE t(a INT); INSERT INTO t VALUES (1), (-2), (NULL), (1);"
                     "\nCREATE VIEW v AS SELECT * FROM t;",
    "insert_permutation": "CREATE TABLE t(a INT, b INT); INSERT INTO t(b, a) VALUES (5, 2), (9, 3);"
                          "\nINSERT INTO t VALUES (3, 4);",
    "insert_widths": "CREATE TABLE t(id BIGINT NOT NULL, flag BOOLEAN, s VARCHAR(20), b VARBINARY(4));"
                     "\nINSERT INTO t VALUES (1, TRUE, 'Bob''s', X'ABCD'), (2147483648, NULL, 'a', X'12');",
    "insert_null": "CREATE TABLE t(a INT); INSERT INTO t VALUES (NULL), (NULL);",
    "explicit_view_columns": "CREATE TABLE readings(x INT, y INT);\n"
                             "CREATE LOCAL VIEW swapped(a, b) AS SELECT y, x FROM readings;\n"
                             "CREATE MATERIALIZED VIEW stored AS SELECT b FROM swapped;",
    "named_constant": "CREATE VIEW v(answer) AS SELECT 42;",
    "inline_key": "CREATE TABLE readings(id BIGINT NOT NULL PRIMARY KEY, v INT);\n"
                  "CREATE VIEW output_rows AS SELECT * FROM readings;",
    "composite_key": "CREATE TABLE readings(a INT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (b, a));",
    "interleaved_key": "CREATE TABLE readings(a INT NOT NULL, PRIMARY KEY (a, b), b BIGINT NOT NULL);",
    "local_constant": "CREATE LOCAL VIEW one AS SELECT 5 AS five;",
    "materialized_constant": "CREATE MATERIALIZED VIEW one AS SELECT 5 AS five;",
    "view_kinds": "CREATE TABLE readings(x INT, y BIGINT);\n"
                  "CREATE LOCAL VIEW selected AS SELECT y FROM readings WHERE x > 2;\n"
                  "CREATE MATERIALIZED VIEW stored AS SELECT x, y FROM readings;",
    "view_chain": "CREATE TABLE readings(x INT NULL, y INT);\n"
                  "CREATE LOCAL VIEW selected AS SELECT * FROM readings;\n"
                  "CREATE MATERIALIZED VIEW stored AS SELECT * FROM selected;\n"
                  "CREATE VIEW reordered AS SELECT y, x FROM selected WHERE y > 5;\n"
                  "CREATE VIEW public_copy AS SELECT * FROM stored;",
    "constant_view_input": "CREATE LOCAL VIEW seed AS SELECT 1 AS n;\n"
                           "CREATE VIEW result AS SELECT n FROM seed;",
    "scalar_widths": "CREATE TABLE readings(t TIME, ts TIMESTAMP(6), f REAL NOT NULL, g REAL, "
                     "i TINYINT UNSIGNED, j SMALLINT UNSIGNED, k BIGINT UNSIGNED, "
                     "a BINARY, b VARBINARY, c BINARY(4), d VARBINARY(10));\n"
                     "CREATE VIEW samples AS SELECT d, c, b, a, k, j, i, g, f, ts, t FROM readings;",
    "temporal_precision": "CREATE TABLE times(t TIME(2), ts TIMESTAMP(3), full_time TIME(12), "
                          "full_timestamp TIMESTAMP(9));\n"
                          "CREATE TABLE more_times(t TIME(2) NOT NULL, ts TIMESTAMP(0));\n"
                          "CREATE VIEW repeated AS SELECT t, ts FROM times;",
    "character_date": "CREATE TABLE records(id INT NOT NULL, day DATE, label VARCHAR(24), tag CHAR(3));\n"
                      "CREATE VIEW result AS SELECT label, day, tag FROM records WHERE id > 2;",
    "string_projection": "CREATE TABLE names(id INT, name VARCHAR, prefix CHAR);\n"
                         "CREATE VIEW labels AS SELECT prefix, name FROM names;",
    "repeated_column": "CREATE TABLE sensors(id INT NOT NULL, y BIGINT);\n"
                       "CREATE VIEW visible AS SELECT id, id AS another FROM sensors;",
    "sum": METERS + "SELECT SUM(reading) AS total FROM meters;",
    "count": METERS + "SELECT COUNT(reading) AS samples FROM meters;",
    "count_star": METERS + "SELECT COUNT(*) AS samples FROM meters;",
    "filtered_count_star": METERS + "SELECT COUNT(*) AS samples FROM meters WHERE reading > 0;",
    "grouped": METERS + "SELECT sensor, SUM(reading) AS total FROM meters GROUP BY sensor;",
    "distinct": METERS + "SELECT COUNT(DISTINCT reading) AS samples FROM meters;",
    "filter": "CREATE TABLE readings (sensor INTEGER NOT NULL, temperature INTEGER NOT NULL);\n"
              "CREATE VIEW hot AS SELECT sensor FROM readings WHERE temperature > 20;",
    "reordered": "CREATE TABLE sensors (x INT NOT NULL, y INT NOT NULL);\n"
                 "CREATE VIEW high AS SELECT y, x FROM sensors WHERE x <= 13;",
    "multiline_constant": "-- one constant view\nCREATE VIEW one AS\nSELECT 3 + 4 AS seven;",
    "constant": "CREATE VIEW one AS SELECT 5 AS five;",
    "multiple_tables": "CREATE TABLE north (n INT NOT NULL);\n"
                       "CREATE VIEW northern AS SELECT n FROM north;\n"
                       "CREATE TABLE south (s BIGINT);\n"
                       "CREATE VIEW southern AS SELECT s FROM south WHERE s > 4;",
    "table_only": "CREATE TABLE empty_input (id INT NOT NULL);",
    "empty_filter": "CREATE TABLE meters (n INT);\n"
                    "CREATE VIEW never AS SELECT n FROM meters WHERE FALSE;",
    "two_views": "CREATE TABLE meters (id INT NOT NULL, level INT);\n"
                 "CREATE VIEW high AS SELECT id FROM meters WHERE level > 99;\n"
                 "CREATE VIEW low AS SELECT level, id FROM meters WHERE level <= 1;",
}


def validate(output, compiler, comparator, classpath):
    output.mkdir()
    catalog = Path(__file__).resolve().parents[1] / "sql-binder/src/system_catalog.json"
    options = json.loads(catalog.read_text())["context"]["options"]
    options["ioOptions"]["inputCircuit"] = False
    inputs = output / "inputs.jsonl"
    programs = output / "bound.jsonl"
    with inputs.open("x") as inputs_file, programs.open("x") as bound_file:
        for name, sql in CASES.items():
            filename = output / (name + ".sql")
            filename.write_text(sql)
            result = subprocess.run([compiler, "--emit-s1", filename], check=True, capture_output=True)
            program = json.loads(result.stdout)
            if program["id"] != str(filename):
                raise ValueError(f"{name}: unexpected bound program identity")
            inputs_file.write(json.dumps({
                "id": str(filename), "options": options, "program": sql,
                "submissions": [{"sql": sql, "many": True, "batch": 0}],
                "input_sha256": hashlib.sha256(sql.encode()).hexdigest(),
            }) + "\n")
            bound_file.write(json.dumps(program) + "\n")
    for side, flags in (("direct", ["capture"]), ("bound", ["replay-s1", programs])):
        subprocess.run(["java", "-ea", "-Xmx1g", "-Dfeldera.determinism.seamArtifacts=true",
                        "-Dfeldera.determinism.producer=binder-validation",
                        "-Dfeldera.determinism.runtime=binder-validation",
                        "-cp", classpath, "org.dbsp.sqlCompiler.compiler.sql.tools.CorpusDeterminism",
                        inputs, output / side, comparator, output / (side + "-artifacts"), *flags],
                       check=True)
    def records(side):
        with (output / side / "cases.jsonl").open() as source:
            return [json.loads(line) for line in source]
    expected, actual = records("direct"), records("bound")
    if len(expected) != len(CASES) or len(actual) != len(expected):
        raise ValueError("Incomplete downstream comparison")
    for left, right in zip(expected, actual):
        differences = [k for k in left.keys() | right.keys() if left.get(k) != right.get(k)]
        if differences:
            raise ValueError(f"{left['id']}: downstream difference in {sorted(differences)}")
    for name in CASES:
        filename = output / (name + ".sql")
        preview = subprocess.run([compiler, "--schema", filename], check=True, capture_output=True)
        direct = next(record for record in expected if record["id"] == str(filename))
        expected_schema = direct["schema"]
        for relation in expected_schema["inputs"] + expected_schema["outputs"]:
            for column in relation["fields"]:
                for key in ("unused", "default", "lateness"):
                    column.pop(key, None)
        if json.loads(preview.stdout) != expected_schema:
            raise ValueError(f"{name}: Rust schema preview differs from direct Java")
        golden = output / (name + "-java")
        golden.mkdir()
        plan = golden / "plan.json"
        subprocess.run(["java", "-ea", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain",
                        "--gen2", "--je", "--errors", golden / "diagnostics.json", "--plan", plan, "--js", golden / "schema.json",
                        "--dataflow", golden / "dataflow.json", "-o", golden / "circuit.json",
                        filename], check=True)
        mixed = output / (name + "-mixed")
        compile_program(filename, mixed, compiler, classpath, "binder-validation")
        compare(golden, mixed, comparator)
        rust_plan = subprocess.run([compiler, "--emit-plan", filename], check=True, capture_output=True)
        if json.loads(rust_plan.stdout) != json.loads(plan.read_bytes()):
            raise ValueError(f"{name}: relational plan differs from direct Java")
    (output / "complete.json").write_text(json.dumps({"cases": list(CASES), "equal": True}) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", required=True, type=Path)
    parser.add_argument("--comparator", required=True, type=Path)
    parser.add_argument("--classpath", required=True)
    args = parser.parse_args()
    validate(args.output, args.compiler, args.comparator, args.classpath)
