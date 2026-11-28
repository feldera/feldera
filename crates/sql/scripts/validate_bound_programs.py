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

SET_TABLES = ("CREATE TABLE a(x INT, y BIGINT NOT NULL); CREATE TABLE b(p INT, q BIGINT NOT NULL);"
              " CREATE TABLE c(n INT, m BIGINT NOT NULL); CREATE VIEW v AS ")
# Exercise both orders of nullability. Division reuses its left operand's
# type when it already has the inferred nullability.
NUMERIC_COLUMN_CASES = {
    f"column_arithmetic_{sql_type.lower().replace(' ', '_')}":
        f"CREATE TABLE t(a {sql_type} NOT NULL, b {sql_type});"
        " CREATE VIEW v AS SELECT a+b AS p, a-b AS m, b*a AS x, a/b AS d,"
        " b/a AS nd, b/b AS bd, a/a AS self_div, a+a AS twice,"
        " ABS(a) AS magnitude, ABS(b) AS nullable_magnitude FROM t;"
    for sql_type in ("TINYINT", "SMALLINT", "INT", "BIGINT", "REAL", "DOUBLE",
                     "TINYINT UNSIGNED", "SMALLINT UNSIGNED", "INT UNSIGNED", "BIGINT UNSIGNED")
}

INTEGER_COMPARISONS = {
    name: f"CREATE TABLE t(a {left}, b {right}); CREATE VIEW v AS SELECT a FROM t AS i WHERE {predicate};"
    for name, left, right, predicate in (
        ("narrow_filter", "TINYINT", "BIGINT", "a > 1"),
        ("wide_filter", "BIGINT", "SMALLINT", "a > 1"),
        ("narrow_filter_equality", "TINYINT NOT NULL", "INT", "a = -2"),
        ("unrepresentable_filter_equality", "SMALLINT", "INT", "a = 45000"),
        ("wide_filter_literal", "INT NOT NULL", "INT", "a < 2147483648"),
        ("filter_column_widening", "INT NOT NULL", "SMALLINT NOT NULL", "a >= b"),
        ("filter_column_nullability", "TINYINT NOT NULL", "BIGINT", "a <> b"),
        ("filter_postfix_alias", "SMALLINT", "INT", "i.a > '0'::INT4"),
        ("filter_nested_postfix", "TINYINT", "INT", "i.a <= ('1'::TINYINT)"),
        ("filter_cast", "BIGINT", "INT", "a = CAST(' 2 ' AS INT2)"),
        ("filter_negative_cast", "SMALLINT", "INT", "a < -1::INT2"),
        ("filter_parentheses", "INT", "INT", "a <> ((-1))"),
    )
}

TRIGONOMETRIC_CASES = {
    "exact_trigonometry": "CREATE VIEW v AS SELECT SIN(-0.5), TAN(1), COS(0), ACOS(-0.75),"
                          " ASIN(2), COT(0), ATAN(NULL), DEGREES(1), RADIANS(90);",
    "approximate_trigonometry": "/* 🦀 */ CREATE VIEW v AS SELECT ((cos(CAST(0.25 AS DOUBLE)))) AS x,"
                                " sin(CAST(-0.0 AS DOUBLE PRECISION)), atan(CAST(NULL AS DOUBLE));",
    "mixed_trigonometry": "CREATE VIEW v AS SELECT COS(CAST(1 AS FLOAT8)), SIN(1),"
                          " COS(NULL), COS(CAST(1 AS DOUBLE));",
    "double_literal_scale": "CREATE VIEW v AS SELECT TAN(CAST(10 AS DOUBLE)),"
                            " COS(CAST(-123 AS DOUBLE)), SIN(CAST(0.0001 AS DOUBLE));",
}

TIMESTAMP_DIFF_CASES = {
    "timestampdiff_date": "CREATE VIEW v AS SELECT TIMESTAMPDIFF(MONTH, DATE '2001-02-02', DATE '2001-03-01') AS d;",
    "timestampdiff_scale": "CREATE VIEW v AS SELECT ((TIMESTAMPDIFF(SQL_TSI_WEEK, TIMESTAMP '2023-02-03',"
                           " TIMESTAMP '2022-02-03 01:02:03.000123'))) AS weeks;",
    "timestampdiff_mixed": "CREATE VIEW v AS SELECT TIMESTAMPDIFF(HOUR, TIME '12:00:00.123', DATE '2001-01-01');",
    "timestampdiff_nanos": "CREATE VIEW v AS SELECT TIMESTAMPDIFF(SQL_TSI_FRAC_SECOND,"
                           " TIME '00:00:00', TIME '00:00:01') AS ns;",
    "timestampdiff_units": "CREATE VIEW v AS SELECT TIMESTAMPDIFF(QUARTER, DATE '2001-01-01', DATE '2001-12-31'),"
                           " TIMESTAMPDIFF(YEAR, DATE '2001-01-01', DATE '2001-12-31');",
}

ROUNDING_CASES = {
    "round_exact_ties": "CREATE VIEW v AS SELECT ROUND(15.50), ROUND(-15.50),"
                        " TRUNCATE(-15.50), TRUNCATE(0.042, 2);",
    "round_negative_places": "CREATE VIEW v AS SELECT ROUND(125,-1), ROUND(-125,-1),"
                             " TRUNCATE(-125,-1), ROUND(0.00125,-38);",
    "round_integer_width": "CREATE VIEW v AS SELECT ROUND(2147483647,-1),"
                           " ROUND(-2147483648,-1), ROUND(9223372036854775807,-1);",
    "round_nulls": "CREATE VIEW v AS SELECT ROUND(NULL,NULL), ROUND(CAST(NULL AS BIGINT),2),"
                   " TRUNCATE(15.50,NULL), ROUND(3,CAST(NULL AS INTEGER));",
    "round_decimal_casts": "CREATE VIEW v AS SELECT ROUND(-1.555::DECIMAL(5,2),2),"
                           " TRUNCATE(CAST(1.555 AS DECIMAL(5,2)),1), ROUND(NULL::DECIMAL(6,3));",
    "round_array_projection": "CREATE VIEW v AS SELECT ARRAY[1,2], ROUND(1.555), TRUNCATE(19,-1);",
}

STRING_SLICE_CASES = {
    "slice_unicode": "CREATE VIEW v AS SELECT LEFT('éhéllo', 2), RIGHT('éhéllo', 3),"
                     " SUBSTRING('éhéllo' FROM -1 FOR 4), SUBSTR('éhéllo', -2, 1);",
    "slice_binary": "CREATE VIEW v AS SELECT SUBSTRING(X'1234AB' FROM 2 FOR 2),"
                    " SUBSTR(X'1234AB', 0, 2), LEFT(X'FF', -1), RIGHT(X'0102', 8);",
    "slice_nullable": "CREATE VIEW v AS SELECT LEFT(CAST(NULL AS VARCHAR(3)), 1),"
                      " SUBSTRING(NULL, 3), SUBSTR('abc', CAST(NULL AS INT)), RIGHT('x', NULL);",
    "slice_negative_index": "CREATE VIEW v AS SELECT SUBSTR('abc', -4, 0), SUBSTR('abc', -3, 2),"
                            " SUBSTRING('abc' FROM 0 FOR 2), SUBSTR('abc', 1, -1);",
    "slice_nullable_length": "CREATE VIEW v AS SELECT SUBSTR('abc', -10, NULL),"
                             " SUBSTR(X'1234', -8, CAST(NULL AS INT)), ARRAY[1] AS a;",
    "slice_projected_literals": "CREATE VIEW v AS SELECT ARRAY[1] AS a, SUBSTRING('hello', 2) AS s,"
                                " RIGHT(X'FFAB', 1) AS b, 3.100 AS n, CAST(NULL AS BIGINT) AS i;",
}

LITERAL_ARRAY_CASES = {
    "array_numeric_coercion": "CREATE VIEW v AS SELECT ARRAY[1.25, NULL, 3000000000] AS a;",
    "array_string_widths": "CREATE VIEW v AS SELECT ARRAY['a', 'hello', NULL] AS a;",
    "array_all_null": "CREATE VIEW v AS SELECT ARRAY[NULL, NULL] AS a;",
    "array_temporal": "CREATE VIEW v AS SELECT ARRAY[TIME '12:34:56.1234', NULL] AS a;",
    "array_folded_sibling": "CREATE VIEW v AS SELECT ((ARRAY[1, 2])) AS a, CARDINALITY(ARRAY[1, NULL]) AS n;",
    "array_length_temporal": "CREATE VIEW v AS SELECT ARRAY_LENGTH(ARRAY[TIME '12:34:56.1234']) AS n;",
    "trig_failed_reduction": "CREATE VIEW v AS SELECT 3 / 0, SIN(1.50), TAN((CAST(-1 AS DOUBLE))), SIN(NULL);",
    "array_failed_reduction": "CREATE VIEW v AS SELECT 3 / 0 AS quotient, ARRAY_SIZE(ARRAY[1, 2]) AS n;",
}


CASES = {
    **STRING_SLICE_CASES,
    **ROUNDING_CASES,
    **LITERAL_ARRAY_CASES,
    **TIMESTAMP_DIFF_CASES,
    "runtime_integer_divide": "CREATE VIEW v AS SELECT 1 / 0, 2 / (1 / 0), (3 / 0) / 2;",
    **NUMERIC_COLUMN_CASES,
    **INTEGER_COMPARISONS,
    **TRIGONOMETRIC_CASES,
    "array_insert_permutation": "CREATE TABLE t(a BIGINT ARRAY,b INT);"
                                " INSERT INTO t(b,a) VALUES(5, ARRAY[(-3),-2,0]);"
                                " INSERT INTO t VALUES(ARRAY[9223372036854775807], 4);"
                                " CREATE VIEW v AS SELECT * FROM t;",
    "array_insert_elements": "CREATE TABLE t(a VARCHAR ARRAY, b DATE ARRAY);"
                             " INSERT INTO t VALUES(ARRAY['a','b'], ARRAY[DATE '1999-01-01',DATE '2000-02-29']);",
    "array_insert_decimal": "CREATE TABLE t(a DECIMAL(7,2) ARRAY,i INT NOT NULL);"
                            " INSERT INTO t VALUES(ARRAY[1.25,2.50],6);",
    "array_insert_multiline": "/* 🙂 */ CREATE TABLE t(a INT ARRAY, b BOOLEAN ARRAY);"
                              " INSERT INTO t VALUES(ARRAY[1,2],\n ARRAY [TRUE, FALSE]);"
                              " CREATE VIEW v AS SELECT b,a FROM t;",
    "qualified_wildcard": "CREATE TABLE t(a INT, b INT);"
                          " CREATE VIEW v AS SELECT q.*, a + b FROM t AS q;",
    "overlapping_wildcards": "CREATE TABLE t(a INT, a0 INT);"
                             " CREATE VIEW v AS SELECT a, q.*, a, q.* FROM t AS q;",
    "duplicate_constant_names": "CREATE VIEW v AS SELECT 1 AS a, 2 AS a, 3 AS a0;",
    "empty_duplicate_names": "CREATE TABLE t(a INT, b INT); CREATE VIEW v AS SELECT a, a FROM t WHERE FALSE;",
    "duplicate_set_names": "CREATE TABLE t(a INT, b INT); CREATE TABLE u(a INT, b INT);"
                           " CREATE VIEW v AS SELECT a,a FROM t UNION ALL SELECT b,b FROM u;",
    "quoted_wildcard_names": 'CREATE TABLE t(a INT, b INT);'
                             ' CREATE VIEW c AS SELECT a AS "a""b", b AS "Mi`XeD" FROM t;'
                             ' CREATE VIEW v AS SELECT c.*, c.* FROM c;',
    "cast_null_scalars": "CREATE VIEW v AS SELECT CAST(NULL AS INT UNSIGNED) AS u,"
                         " CAST(NULL AS VARCHAR(3)) AS s, CAST(NULL AS CHAR) AS c,"
                         " CAST(NULL AS BINARY(2)) AS b, CAST(NULL AS DECIMAL(12,2)) AS d,"
                         " CAST(NULL AS REAL) AS r, CAST(NULL AS DOUBLE PRECISION) AS f,"
                         " CAST(NULL AS VARIANT) AS v;",
    "cast_null_temporal": "CREATE VIEW a AS SELECT CAST(NULL AS TIME(3)) AS t, CAST(NULL AS UUID) AS u;"
                          " CREATE VIEW b AS SELECT CAST(NULL AS TIMESTAMP) AS ts, CAST(NULL AS DATE) AS d,"
                          " CAST(NULL AS TIME) AS t, CAST(NULL AS UUID) AS u;",
    "cast_null_aliases": "CREATE VIEW v AS SELECT CAST((NULL) AS INT2) AS i,"
                         " CAST(NULL AS TEXT) AS s, CAST(NULL AS BYTEA) AS b, UNKNOWN AS flag;",
    "cast_null_insert": "CREATE TABLE t(k BIGINT, s VARCHAR(12), b BOOLEAN);"
                        " INSERT INTO t VALUES (1, 'x', TRUE), (CAST(NULL AS SMALLINT), 'abc', UNKNOWN),"
                        " (2147483648, CAST(NULL AS VARCHAR(8)), NULL); CREATE VIEW v AS SELECT * FROM t;",
    "cast_null_precision": "CREATE TABLE t(b VARBINARY(8), n DECIMAL(6,2), t TIME);"
                           " INSERT INTO t VALUES (X'AB', 1.25, TIME '10:00:00.1'),"
                           " (X'1234', CAST(NULL AS DECIMAL(6,2)), TIME '11:00:00.123');",
    "unknown_insert": "CREATE TABLE t(b BOOLEAN, s VARCHAR);"
                      " INSERT INTO t VALUES (UNKNOWN, 'a'), (TRUE, 'abc'), (NULL, NULL);",
    "cast_null_numeric_widths": "CREATE TABLE t(a INT, b BIGINT, c DECIMAL(9,2)); INSERT INTO t VALUES"
                                " (1, CAST(NULL AS TINYINT UNSIGNED), CAST(NULL AS SMALLINT)),"
                                " (CAST(NULL AS INT UNSIGNED), CAST(NULL AS SMALLINT), 0.25);",
    "unicode_origins": "/* 😀 é */ CREATE TABLE t(a INT, b INT); /* 🦀 */"
                      " CREATE VIEW v AS SELECT a FROM t WHERE b > 1;",
    "unicode_identifiers": "CREATE TABLE Über(Á INT); CREATE VIEW Ä AS SELECT ÜBER.Á FROM über;",
    "unicode_record": '/*é*/ CREATE TYPE "😀" AS ("É" INTEGER); CREATE TABLE t(v "😀");',
    "unicode_alias": 'CREATE VIEW v AS SELECT 2 AS "Ž😀", 1 AS "É";',
    "unicode_literals": "CREATE VIEW v AS SELECT 'éé' AS a, (('🤖')) AS b, '🐕' AS c;",
    "unicode_insert": "CREATE TABLE t(a VARCHAR, b VARCHAR); INSERT INTO t(b,a) VALUES ('🤝', '👋👋');"
                      " INSERT INTO t VALUES ('é', '😀'); CREATE VIEW v AS SELECT * FROM t;",
    "unicode_assignment": "CREATE TABLE t(a INT, b BOOLEAN); INSERT INTO t VALUES ('🙂', '🙂');"
                          " INSERT INTO t VALUES ('bad', '🐆');",
    "unicode_null": "CREATE TABLE t(a INT, b VARCHAR); INSERT INTO t VALUES (NULL, '👋');",
    "unicode_tab": "\t/*😀*/ CREATE TABLE t(x INT, y INT);\tCREATE VIEW v AS SELECT y,x FROM t;",
    "set_union": SET_TABLES + "SELECT * FROM a UNION SELECT * FROM b;",
    "set_union_all": SET_TABLES + "SELECT * FROM a UNION ALL SELECT * FROM b;",
    "set_intersect_all": SET_TABLES + "SELECT * FROM a INTERSECT ALL SELECT * FROM b;",
    "set_minus": SET_TABLES + "SELECT x AS k FROM a EXCEPT SELECT p FROM b;",
    "set_minus_all": SET_TABLES + "SELECT * FROM a EXCEPT ALL SELECT * FROM b;",
    "set_except_chain": SET_TABLES + "SELECT x FROM a EXCEPT SELECT p FROM b EXCEPT SELECT n FROM c;",
    "set_intersect_chain": SET_TABLES + "SELECT * FROM a INTERSECT SELECT * FROM b INTERSECT SELECT * FROM c;",
    "set_chain": SET_TABLES + "SELECT x FROM a UNION ALL SELECT p FROM b UNION ALL SELECT n FROM c;",
    "set_mixed": SET_TABLES + "SELECT x FROM a UNION SELECT p FROM b INTERSECT SELECT n FROM c;",
    "set_filter": SET_TABLES + "SELECT y AS z FROM a WHERE x > 1 UNION ALL SELECT q FROM b;",
    "set_identity_names": "CREATE TABLE a(x INT); CREATE TABLE b(y INT);"
                          " CREATE VIEW v AS SELECT x AS n FROM a UNION DISTINCT SELECT y AS n FROM b;",
    "scalar_assignment_float": "CREATE TABLE t(x REAL, y DOUBLE); INSERT INTO t VALUES"
                               " ('NaN', 'what'), ('1e999', '-2e23'), ('', 'inf');",
    "scalar_assignment_text": "CREATE TABLE t(x CHAR(2), y VARCHAR, z VARCHAR(1));"
                              " INSERT INTO t VALUES (123, -42.5, TRUE), (2, 3.20, FALSE);",
    "scalar_assignment_bool": "CREATE TABLE t(x BOOLEAN); INSERT INTO t VALUES"
                              " (' True '), ('1'), ('YES'), ('False'), (NULL), ('1');",
    "scalar_assignment_numeric": "CREATE TABLE t(i INTEGER, r REAL, a BOOLEAN, b BOOLEAN);"
                                 " INSERT INTO t VALUES (TRUE,FALSE,2,-2.25),(FALSE,TRUE,0,0.00);",
    "scalar_assignment_narrow": "CREATE TABLE t(i TINYINT, u TINYINT UNSIGNED);"
                                " INSERT INTO t VALUES (128,-1), (-129,256);",
    "scalar_assignment_date_text": "CREATE TABLE t(x VARCHAR, y CHAR(5));"
                                   " INSERT INTO t VALUES (DATE '2024-01-01', TIME '23:59:59');",
    "scalar_assignment_binary_text": "CREATE TABLE t(x VARCHAR); INSERT INTO t VALUES (X'CAFE'), (X'');",
    "record_declaration": "CREATE TYPE pair AS (x INT NULL, y VARCHAR NOT NULL, z INT2);",
    "record_fields": 'CREATE TYPE pair AS (x INT, "Y" VARCHAR(12) NOT NULL);'
                     ' CREATE TABLE t(a pair, b pair NOT NULL, c pair ARRAY); CREATE VIEW v AS SELECT c, b, a FROM t;',
    "record_nested": "CREATE TYPE p AS (a INT);"
                     " CREATE TYPE q AS (foo p NOT NULL, bar p, baz p ARRAY, qux MAP<INT, p>); CREATE TABLE t(col q);",
    "record_temporal": "CREATE TYPE ticks AS (d TIME(2), t TIMESTAMP(3), a TIME(9) ARRAY);"
                       " CREATE TABLE input_rows(sample ticks);",
    "record_nominal": "CREATE TYPE p AS (a REAL, b BOOL, c DOUBLE); CREATE TYPE q AS (a p, b p, c p NOT NULL);"
                      " CREATE TABLE t(data q); CREATE VIEW v AS SELECT * FROM t;",
    "record_quoted": 'CREATE TYPE "Typ" AS ("a" BIGINT, "B" BIGINT, "T K" BOOLEAN);'
                     ' CREATE TABLE s(t "Typ"); CREATE VIEW v AS SELECT * FROM s;',
    "record_forward": "CREATE TABLE t(a foo_struct ARRAY); CREATE TYPE foo_struct AS (id BIGINT NOT NULL);",
    "record_warnings": "CREATE TYPE s AS (x DECIMAL, y NUMERIC); CREATE TYPE empty_alias AS VARCHAR;",
    "record_inline": "CREATE TABLE t(a ROW(x INTEGER, y VARCHAR), b ROW(p INTEGER NOT NULL),"
                     " c ROW(m REAL NULL, n VARCHAR ARRAY) ARRAY); CREATE VIEW v AS SELECT * FROM t;",
    "uuid_variant_columns": "CREATE TABLE t(a UUID, b VARIANT, c UUID NOT NULL, d VARIANT NOT NULL,"
                            " e UUID ARRAY, f MAP<INT, VARIANT>); CREATE VIEW v AS SELECT d, c, b, a FROM t;",
    "uuid_literal": "CREATE VIEW v AS SELECT UUID '11223344-AABB-1122-EEFF-FFEEDDCCBBAA' AS x,"
                    " UUID '00000000-0001-0002-0003-000000000004' AS y;",
    "uuid_insert": "CREATE TABLE t(a UUID); INSERT INTO t VALUES"
                   " (UUID '00112233-AaBb-0000-0000-000000000000'), (NULL);"
                   " INSERT INTO t VALUES ('invalid'), ('{00112233aabb00000000000000000000}'), ('invalid');",
    "variant_assignments": "CREATE TABLE t(a VARIANT); INSERT INTO t VALUES (1), (NULL);"
                           " INSERT INTO t VALUES ('test'); INSERT INTO t VALUES (1.25); INSERT INTO t VALUES (true);",
    "alias_forward_use": "CREATE TABLE t(a scores); CREATE TYPE scores AS INTEGER;"
                         " CREATE VIEW v AS SELECT * FROM t;",
    "alias_chain": "CREATE TYPE weights AS DECIMAL(12,3); CREATE TYPE rate AS weights;"
                   " CREATE TABLE t(a rate, b rate NOT NULL); CREATE VIEW v AS SELECT b FROM t;",
    "alias_collections": "CREATE TYPE a AS INT2; CREATE TYPE m AS MAP<STRING, a ARRAY>;"
                         " CREATE TABLE t(c m); CREATE VIEW v AS SELECT * FROM t;",
    "alias_warnings": "CREATE TABLE t(a NUMERIC); CREATE TYPE deca AS DECIMAL; CREATE TYPE nums AS INT ARRAY;"
                      " CREATE TABLE s(b DEC, c nums, d deca);",
    "alias_quoted": 'CREATE TYPE "MyTy" AS BIGINT; CREATE TABLE t(c "MyTy");',
    "alias_only": "CREATE TYPE a AS BOOLEAN; CREATE TYPE tm AS TIMESTAMP(2);",
    "temporal_fraction": "CREATE VIEW v AS SELECT TIME '23:59:59.123456789' AS t,"
                         " TIMESTAMP '1969-12-31 23:59:59.125000' AS ts, TIME '12:34:56.120' AS u;",
    "temporal_calendar": "CREATE VIEW v AS SELECT TIMESTAMP '1-1-1 0:0:0.000001' AS a,"
                         " TIMESTAMP '2020-02-29' AS b, TIME '1:2:3.' AS c, DATE '24-2-1' AS d;",
    "temporal_precision_cap": "CREATE VIEW v AS SELECT TIME '12:00:00.12345678908' AS a,"
                              " TIMESTAMP '2020-01-01 01:02:03.12345678901' AS b;",
    "temporal_insert": "CREATE TABLE t(t TIME, d TIMESTAMP); INSERT INTO t VALUES"
                       " (TIME '01:02:03.1', TIMESTAMP '0001-01-01 01:00:01'),"
                       " (TIME '12:34:56.700', TIMESTAMP '1969-12-31 23:59:59.000001'), (NULL, NULL);",
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
    "insert_heterogeneous_permutation": "CREATE TABLE t(i SMALLINT NOT NULL, d DATE, n DECIMAL(3,2));\n"
                                        "INSERT INTO t(d, n, i) VALUES (DATE '2020-01-02', 1.23, 1), (NULL, 3.45, 2);\n"
                                        "CREATE VIEW v AS SELECT i, d, n FROM t;",
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
