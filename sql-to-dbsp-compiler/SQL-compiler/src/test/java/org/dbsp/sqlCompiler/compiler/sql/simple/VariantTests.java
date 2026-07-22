package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPShortIntervalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLongIntervalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeShortInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeLongInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.Linq;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class VariantTests extends SqlIoTest {
    /** Return the default compiler used for testing. */
    @Override
    public CompilerOptions testOptions() {
        // Do not optimize, esp in Calcite
        CompilerOptions options = super.testOptions();
        options.languageOptions.incrementalize = false;
        options.languageOptions.optimizationLevel = 0;
        return options;
    }

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
        CREATE TYPE s AS (
           i INT,
           s VARCHAR,
           a INT ARRAY
        );
        CREATE TYPE t AS (
           sa S ARRAY
        );""");
        super.prepareInputs(compiler);
    }

    public void testQuery(String query, DBSPExpression... fields) {
        // T contains a date with timestamp '100'.
        query = "CREATE VIEW V AS " + query;
        CompilerCircuitStream ccs = this.getCCS(query);
        DBSPZSetExpression expectedOutput = new DBSPZSetExpression(new DBSPTupleExpression(fields));
        InputOutputChange change = new InputOutputChange(new Change(), new Change(new TableData("V", expectedOutput)));
        ccs.addChange(change);
    }

    /** A heterogeneous JSON object exercising every JSON_EACH_* function. */
    static final String JSON_EACH_OBJECT = "PARSE_JSON('{\"b\": true, \"big\": 5000000000, \"dec\": 2.5, " +
            "\"i\": 1, \"n\": null, \"neg\": -5, \"s\": \"text\", \"snum\": \"7\", \"arr\": [1, 2]}')";

    @Test
    public void testJsonEachBigint() {
        // "dec" is fractional and "snum" is a string: neither is a BIGINT field
        this.qst("""
                SELECT * FROM UNNEST(JSON_EACH_BIGINT(%s)) AS kv(k, v);
                 k   | v
                ----------
                 big | 5000000000
                 i   | 1
                 neg | -5
                (3 rows)""".formatted(JSON_EACH_OBJECT));
    }

    @Test
    public void testJsonEachStringBoolean() {
        // Non-string scalars are not stringified
        this.qst("""
                SELECT * FROM UNNEST(JSON_EACH_STRING(%s)) AS kv(k, v);
                 k    | v
                -----------
                 s    | text
                 snum | 7
                (2 rows)

                SELECT * FROM UNNEST(JSON_EACH_BOOLEAN(%s)) AS kv(k, v);
                 k | v
                --------
                 b | true
                (1 row)""".formatted(JSON_EACH_OBJECT, JSON_EACH_OBJECT));
    }

    @Test
    public void testJsonEachDateTime() {
        // JSON has no date/time types, so strings are parsed using the
        // grammar of the corresponding SQL literal; strings that do not
        // parse are omitted.
        this.qst("""
                SELECT * FROM UNNEST(JSON_EACH_DATE(PARSE_JSON('{"d": "2024-01-01", "s": "text", "t": "17:30:40"}'))) AS kv(k, v);
                 k | v
                --------
                 d | 2024-01-01
                (1 row)

                SELECT * FROM UNNEST(JSON_EACH_TIME(PARSE_JSON('{"d": "2024-01-01", "s": "text", "t": "17:30:40"}'))) AS kv(k, v);
                 k | v
                --------
                 t | 17:30:40
                (1 row)

                SELECT * FROM UNNEST(JSON_EACH_TIMESTAMP(PARSE_JSON('{"d": "2024-01-01", "ts": "2024-12-19 16:39:57"}'))) AS kv(k, v);
                 k  | v
                ---------
                 d  | 2024-01-01 00:00:00
                 ts | 2024-12-19 16:39:57
                (2 rows)

                SELECT * FROM UNNEST(JSON_EACH_DATE(CAST(MAP['d', CAST(DATE '2024-01-01' AS VARIANT), 's', CAST('x' AS VARIANT)] AS VARIANT))) AS kv(k, v);
                 k | v
                --------
                 d | 2024-01-01
                (1 row)

                SELECT * FROM UNNEST(JSON_EACH_TIME(CAST(MAP['d', CAST(DATE '2024-01-01' AS VARIANT), 's', CAST('x' AS VARIANT)] AS VARIANT))) AS kv(k, v);
                 k | v
                --------
                (0 rows)""");
    }

    @Test
    public void testJsonEachNonObject() {
        // Negative tests for JSON_EACH_BIGINT
        this.qst("""
                SELECT * FROM UNNEST(JSON_EACH_BIGINT(PARSE_JSON('[1, 2]'))) AS kv(k, v);
                 k | v
                --------
                (0 rows)

                SELECT * FROM UNNEST(JSON_EACH_BIGINT(PARSE_JSON('5'))) AS kv(k, v);
                 k | v
                --------
                (0 rows)

                SELECT * FROM UNNEST(JSON_EACH_BIGINT(PARSE_JSON('null'))) AS kv(k, v);
                 k | v
                --------
                (0 rows)

                SELECT * FROM UNNEST(JSON_EACH_BIGINT(CAST(NULL AS VARIANT))) AS kv(k, v);
                 k | v
                --------
                (0 rows)""");
    }

    @Test
    public void testJsonFilter() {
        // Filter by runtime type, by value, and by key; TO_JSON serializes
        // the VARIANT results, which the test harness cannot compare directly
        this.qst("""
                SELECT TO_JSON(VARIANT_FILTER(%s, (k, v) -> TYPEOF(v) = 'VARCHAR'));
                 r
                ---
                 {"s":"text","snum":"7"}
                (1 row)

                SELECT TO_JSON(VARIANT_FILTER(PARSE_JSON('{"name": "Ada", "age": 36, "address": {"city": "Boston", "zip": "02115"}, "tags": [1, 2], "note": null}'), (k, x) -> TYPEOF(x) = 'VARCHAR'));
                 r
                ---
                 {"name":"Ada"}
                (1 row)

                SELECT TO_JSON(VARIANT_FILTER(%s, (k, v) -> v <> VARIANTNULL()));
                 r
                ---
                 {"arr":[1,2],"b":true,"big":5000000000,"dec":2.5,"i":1,"neg":-5,"s":"text","snum":"7"}
                (1 row)

                SELECT TO_JSON(VARIANT_FILTER(%s, (k, v) -> CAST(k AS VARCHAR) LIKE 's%%'));
                 r
                ---
                 {"s":"text","snum":"7"}
                (1 row)""".formatted(JSON_EACH_OBJECT, JSON_EACH_OBJECT, JSON_EACH_OBJECT));
    }

    @Test
    public void testJsonFilterNonObject() {
        // A non-map variant is a single item with a NULL label:
        // kept whole or dropped to SQL NULL
        this.qst("""
                SELECT TO_JSON(VARIANT_FILTER(PARSE_JSON('5'), (k, v) -> k IS NULL));
                 r
                ---
                 5
                (1 row)

                SELECT TO_JSON(VARIANT_FILTER(PARSE_JSON('5'), (k, v) -> k IS NOT NULL));
                 r
                ---
                NULL
                (1 row)

                SELECT TO_JSON(VARIANT_FILTER(PARSE_JSON('[1, 2]'), (k, v) -> k IS NULL));
                 r
                ---
                 [1,2]
                (1 row)

                SELECT TO_JSON(VARIANT_FILTER(CAST(NULL AS VARIANT), (k, v) -> TRUE));
                 r
                ---
                NULL
                (1 row)""");
    }

    /** A nested JSON object exercising VARIANT_DEEP_FILTER and JSON_KEYS. */
    static final String NESTED_OBJECT =
            "PARSE_JSON('{\"a\": {\"b\": 1, \"c\": {\"d\": 2}}, \"e\": [{\"f\": 3}, 4], \"g\": 5}')";

    @Test
    public void testDeepFilterQuotedPaths() {
        this.qst("""
                SELECT TO_JSON(VARIANT_DEEP_FILTER(PARSE_JSON('{"example.com": {"a": 1}, "example": {"b": 2}}'), (p, v) -> p = 'example' OR p NOT LIKE 'example.%'));
                 r
                ---
                 {"example":{},"example.com":{"a":1}}
                (1 row)""");
    }

    @Test
    public void testJsonDeepFilter() {
        // Paths are dot-joined; array elements use 1-based bracket components;
        // dropping an inner path removes only that subtree
        this.qst("""
                SELECT TO_JSON(VARIANT_DEEP_FILTER(PARSE_JSON('{"a": {"b": 1, "c": {"d": 2}}}'), (p, x) -> p <> 'a.c'));
                 r
                ---
                 {"a":{"b":1}}
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_FILTER(%s, (p, v) -> p <> 'a.c'));
                 r
                ---
                 {"a":{"b":1},"e":[{"f":3},4],"g":5}
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_FILTER(%s, (p, v) -> p <> 'e[1].f'));
                 r
                ---
                 {"a":{"b":1,"c":{"d":2}},"e":[{},4],"g":5}
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_FILTER(%s, (p, v) -> p <> 'e[1]'));
                 r
                ---
                 {"a":{"b":1,"c":{"d":2}},"e":[4],"g":5}
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_FILTER(PARSE_JSON('[1, {"x": 2}, 3]'), (p, v) -> p <> '[2].x'));
                 r
                ---
                 [1,{},3]
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_FILTER(PARSE_JSON('5'), (p, v) -> p IS NULL));
                 r
                ---
                 5
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_FILTER(CAST(NULL AS VARIANT), (p, v) -> TRUE));
                 r
                ---
                NULL
                (1 row)""".formatted(NESTED_OBJECT, NESTED_OBJECT, NESTED_OBJECT));
    }

    @Test
    public void testVariantMap() {
        this.qst("""
                SELECT TO_JSON(VARIANT_MAP(PARSE_JSON('{"a": 1, "b": 2}'), (k, v) -> CAST(v AS BIGINT) * 2));
                 r
                ---
                 {"a":2,"b":4}
                (1 row)

                SELECT TO_JSON(VARIANT_MAP(PARSE_JSON('{"a": 1, "b": 2}'), (k, v) -> k));
                 r
                ---
                 {"a":"a","b":"b"}
                (1 row)

                SELECT TO_JSON(VARIANT_MAP(PARSE_JSON('{"a": 1, "b": "x"}'), (k, v) -> CAST(v AS BIGINT) * 2));
                 r
                ---
                 {"a":2,"b":null}
                (1 row)

                SELECT TO_JSON(VARIANT_MAP(PARSE_JSON('{"a": 1, "b": "x"}'), (k, v) -> TYPEOF(v)));
                 r
                ---
                 {"a":"BIGINT UNSIGNED","b":"VARCHAR"}
                (1 row)

                SELECT TO_JSON(VARIANT_MAP(PARSE_JSON('{"a": 1, "b": "x"}'), (k, v) -> CAST(v AS BIGINT)));
                 r
                ---
                 {"a":1,"b":null}
                (1 row)

                SELECT TO_JSON(VARIANT_MAP(PARSE_JSON('5'), (k, v) -> CAST(v AS BIGINT) + 1));
                 r
                ---
                 6
                (1 row)

                SELECT TO_JSON(VARIANT_MAP(CAST(NULL AS VARIANT), (k, v) -> v));
                 r
                ---
                NULL
                (1 row)""");
    }

    @Test
    public void testVariantDeepMap() {
        // Structure is preserved exactly; the lambda transforms only leaves,
        // labeled by their dot-joined path; JSON nulls are leaves too
        this.qst("""
                SELECT TO_JSON(VARIANT_DEEP_MAP(PARSE_JSON('{"a": {"b": 1}, "e": [1, 2]}'), (p, v) -> p));
                 r
                ---
                 {"a":{"b":"a.b"},"e":["e[1]","e[2]"]}
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_MAP(PARSE_JSON('{"a": {"b": 1}, "e": [1, 2], "s": "x"}'), (p, v) -> CAST(v AS BIGINT) * 2));
                 r
                ---
                 {"a":{"b":2},"e":[2,4],"s":null}
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_MAP(PARSE_JSON('{"user": {"name": "Ada", "ssn": "123"}, "id": 7}'), (p, x) -> CASE WHEN p LIKE 'user.%' THEN CAST('***' AS VARIANT) ELSE x END));
                 r
                ---
                 {"id":7,"user":{"name":"***","ssn":"***"}}
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_MAP(PARSE_JSON('5'), (p, v) -> CAST(v AS BIGINT) + 1));
                 r
                ---
                 6
                (1 row)

                SELECT TO_JSON(VARIANT_DEEP_MAP(CAST(NULL AS VARIANT), (p, v) -> v));
                 r
                ---
                NULL
                (1 row)""");
    }

    @Test
    public void testVariantMerge() {
        this.qst("""
                SELECT TO_JSON(VARIANT_MERGE(PARSE_JSON('{"a": {"x": 1, "y": 2}, "b": 1}'), PARSE_JSON('{"a": {"x": 9, "z": 3}, "c": 4}')));
                 r
                ---
                 {"a":{"x":9,"y":2,"z":3},"b":1,"c":4}
                (1 row)

                SELECT TO_JSON(VARIANT_MERGE(PARSE_JSON('{"a": 1}'), CAST(MAP['new', CAST(5 AS VARIANT)] AS VARIANT)));
                 r
                ---
                 {"a":1,"new":5}
                (1 row)

                SELECT TO_JSON(VARIANT_MERGE(PARSE_JSON('{"a": [1, 2]}'), PARSE_JSON('{"a": [3]}')));
                 r
                ---
                 {"a":[3]}
                (1 row)


                SELECT TO_JSON(VARIANT_MERGE(PARSE_JSON('{"a": 1}'), PARSE_JSON('{"new": 5}')));
                 r
                ---
                 {"a":1,"new":5}
                (1 row)

                SELECT TO_JSON(VARIANT_MERGE(PARSE_JSON('{"a": 1, "b": 2}'), PARSE_JSON('{"a": null}')));
                 r
                ---
                 {"a":null,"b":2}
                (1 row)

                SELECT TO_JSON(VARIANT_MERGE(PARSE_JSON('5'), PARSE_JSON('6')));
                 r
                ---
                 6
                (1 row)

                SELECT TO_JSON(VARIANT_MERGE(CAST(NULL AS VARIANT), PARSE_JSON('{"a": 1}')));
                 r
                ---
                NULL
                (1 row)""");
    }

    @Test
    public void testLambdaTypeChecking() {
        // A predicate with the wrong number of parameters is rejected
        this.queryFailingInCompilation(
                "SELECT VARIANT_FILTER(PARSE_JSON('{}'), (k) -> TRUE)",
                "VARIANT_FILTER(<VARIANT>, <FUNCTION(VARIANT, VARIANT)-><BOOLEAN>>)");
        this.queryFailingInCompilation(
                "SELECT VARIANT_FILTER(PARSE_JSON('{}'), (k, v, x) -> TRUE)",
                "VARIANT_FILTER(<VARIANT>, <FUNCTION(VARIANT, VARIANT)-><BOOLEAN>>)");
        // A predicate that does not return BOOLEAN is rejected
        this.queryFailingInCompilation(
                "SELECT VARIANT_FILTER(PARSE_JSON('{}'), (k, v) -> 5)",
                "VARIANT_FILTER(<VARIANT>, <FUNCTION(VARIANT, VARIANT)-><BOOLEAN>>)");
        // The first argument must be a VARIANT
        this.queryFailingInCompilation(
                "SELECT VARIANT_FILTER(5, (k, v) -> TRUE)",
                "VARIANT_FILTER(<VARIANT>, <FUNCTION(VARIANT, VARIANT)-><BOOLEAN>>)");
        // A non-lambda second argument is rejected
        this.queryFailingInCompilation(
                "SELECT VARIANT_FILTER(PARSE_JSON('{}'), 5)",
                "VARIANT_FILTER(<VARIANT>, <FUNCTION(VARIANT, VARIANT)-><BOOLEAN>>)");
        // In VARIANT_DEEP_FILTER the path is a VARCHAR, not a VARIANT:
        // TYPEOF requires a VARIANT argument
        this.queryFailingInCompilation(
                "SELECT VARIANT_DEEP_FILTER(PARSE_JSON('{}'), (p, v) -> TYPEOF(p) = 'VARCHAR')",
                "TYPEOF");
        // The same rules apply to the map functions; arity checked here
        this.queryFailingInCompilation(
                "SELECT VARIANT_MAP(PARSE_JSON('{}'), (k) -> k)",
                "VARIANT_MAP(<VARIANT>, <FUNCTION(VARIANT, VARIANT)-><ANY>>)");
    }

    @Test
    public void testJsonObjectKeys() {
        // Top-level keys, sorted, including those holding nulls, arrays
        // and nested objects; non-objects and NULL produce no rows
        this.qst("""
                SELECT * FROM UNNEST(JSON_OBJECT_KEYS(%s)) AS t(k);
                 k
                ------
                 arr
                 b
                 big
                 dec
                 i
                 n
                 neg
                 s
                 snum
                (9 rows)

                SELECT * FROM UNNEST(JSON_OBJECT_KEYS(PARSE_JSON('{"a": 1, "b": {"c": 2}, "d": null}'))) AS t(k);
                 k
                ------
                 a
                 b
                 d
                (3 rows)

                SELECT * FROM UNNEST(JSON_OBJECT_KEYS(PARSE_JSON('[1, 2]'))) AS t(k);
                 k
                ------
                (0 rows)

                SELECT * FROM UNNEST(JSON_OBJECT_KEYS(CAST(NULL AS VARIANT))) AS t(k);
                 k
                ------
                (0 rows)""".formatted(JSON_EACH_OBJECT));
    }

    @Test
    public void testJsonKeys() {
        // Every key at every level as a dot-joined path; arrays are not
        // traversed, so "e.f" is absent.  Keys containing special characters
        // are escaped using double quotes, like in BigQuery, so a key with a
        // dot cannot collide with a nested path.
        this.qst("""
                SELECT * FROM UNNEST(JSON_KEYS(PARSE_JSON('{"a.b": 1, "a": {"b": 2}}'))) AS t(k);
                 k
                ------
                 "a.b"
                 a
                 a.b
                (3 rows)

                SELECT * FROM UNNEST(JSON_KEYS(PARSE_JSON('{"a": {"b": 1, "c": {"d": 2}}, "e": [{"f": 3}], "g": 4}'))) AS t(k);
                 k
                ------
                 a
                 a.b
                 a.c
                 a.c.d
                 e
                 g
                (6 rows)

                SELECT * FROM UNNEST(JSON_KEYS(PARSE_JSON('5'))) AS t(k);
                 k
                ------
                (0 rows)

                SELECT * FROM UNNEST(JSON_KEYS(CAST(NULL AS VARIANT))) AS t(k);
                 k
                ------
                (0 rows)""");
    }

    @Test
    public void testUDT() {
        this.compileRustTestCase("""
                CREATE TYPE x AS (v INTEGER, w INTEGER);
                CREATE TABLE TT(xf X ARRAY);
                CREATE VIEW V AS SELECT CAST(xf AS VARIANT) FROM TT;""");
    }

    @Test
    public void testVariant() {
        // adapted from Calcite variant.iq
        this.testQuery("SELECT CAST(1 AS VARIANT)",
                new DBSPVariantExpression(new DBSPI32Literal(1)));
        this.testQuery("SELECT TYPEOF(CAST(1 AS VARIANT))",
                new DBSPStringLiteral("INTEGER"));
        // The runtime knows that this is a TINYINT
        this.testQuery("SELECT CAST(CAST(1 AS TINYINT) AS VARIANT)",
                new DBSPVariantExpression(new DBSPI8Literal((byte) 1)));
        // Converting something to VARIANT and back works
        this.testQuery("SELECT CAST(CAST(1 AS VARIANT) AS INT)",
                new DBSPI32Literal(1, true));
        // Numeric type conversions are performed automatically
        this.testQuery("SELECT CAST(CAST(1 AS VARIANT) AS TINYINT)",
                new DBSPI8Literal((byte) 1, true));
        // Some VARIANT objects when output receive double quotes
        this.testQuery("select CAST('string' as VARIANT)",
                new DBSPVariantExpression(new DBSPStringLiteral("string")));
        // CHAR(3) values are represented as VARCHAR in variants
        this.testQuery("SELECT CAST(CAST('abc' AS VARIANT) AS VARCHAR)",
                new DBSPStringLiteral("abc", true));
        // VARCHAR and CHAR(N) have the same underlying runtime type
        this.testQuery("SELECT CAST(CAST('abc' AS VARIANT) AS CHAR(3))",
                new DBSPStringLiteral("abc", true));
        // The value representing a VARIANT null value (think of a JSON null)
        this.testQuery("SELECT VARIANTNULL()",
                DBSPVariantNullLiteral.variantNull());
        // VARIANT null is not the same as SQL NULL
        this.testQuery("SELECT VARIANTNULL() IS NULL",
                new DBSPBoolLiteral(false));
        // Two VARIANT nulls are equal, unlike SQL NULL
        this.testQuery("SELECT VARIANTNULL() = VARIANTNULL()",
                new DBSPBoolLiteral(true));
        this.testQuery("SELECT TYPEOF(VARIANTNULL())",
                new DBSPStringLiteral("VARIANT"));
        // Variants delegate equality to the underlying values
        this.testQuery("SELECT CAST(1 AS VARIANT) = CAST(1 AS VARIANT)",
                new DBSPBoolLiteral(true));
        // To be equal two variants must have the same value and the same runtime type
        this.testQuery("SELECT CAST(1 AS VARIANT) = CAST(CAST(1 AS TINYINT) AS VARIANT)",
                new DBSPBoolLiteral(false));
        // An array of variant values can have values with any underlying type
        this.testQuery("SELECT ARRAY[CAST(1 AS VARIANT), CAST('abc' AS VARIANT)]",
                new DBSPArrayExpression(
                        new DBSPVariantExpression(new DBSPI32Literal(1)),
                        new DBSPVariantExpression(new DBSPStringLiteral("abc"))));
        // A map with VARCHAR keys and VARIANT values
        this.testQuery("SELECT MAP['a', CAST(1 AS VARIANT), 'b', CAST('abc' AS VARIANT), 'c', CAST(ARRAY[1,2,3] AS VARIANT)]",
                new DBSPMapExpression(
                        new DBSPTypeMap(
                                DBSPTypeString.varchar(false),
                                DBSPTypeVariant.INSTANCE,
                                false),
                        Linq.list(new DBSPStringLiteral("a"),
                                new DBSPStringLiteral("b"),
                                new DBSPStringLiteral("c")),
                        Linq.list(new DBSPVariantExpression(new DBSPI32Literal(1)),
                                new DBSPVariantExpression(new DBSPStringLiteral("abc")),
                                new DBSPVariantExpression(new DBSPArrayExpression(
                                        new DBSPI32Literal(1),
                                        new DBSPI32Literal(2),
                                        new DBSPI32Literal(3)
                                )))));
        // Variant values allow access by index, but return null if they are not arrays
        this.testQuery("SELECT (CAST(1 AS VARIANT))[1]",
                DBSPTypeVariant.INSTANCE_NULLABLE.none());
        this.testQuery("SELECT CAST(ARRAY[1,2,3] AS VARIANT)[1]",
                new DBSPVariantExpression(new DBSPI32Literal(1), true));
        // Acessing items in a VARIANT array returns VARIANT values,
        // even if the array itself does not contain VARIANT values
        // (Otherwise TYPEOF would not compile)
        this.testQuery("SELECT TYPEOF(CAST(ARRAY[1,2,3] AS VARIANT)[1])",
                new DBSPStringLiteral("INTEGER"));
        this.testQuery("SELECT CAST(DATE '2020-01-01' AS VARIANT)",
                new DBSPVariantExpression(new DBSPDateLiteral("2020-01-01")));
        this.testQuery("SELECT CAST(TIMESTAMP '2020-01-01 10:00:00' AS VARIANT)",
                new DBSPVariantExpression(new DBSPTimestampLiteral(
                        CalciteObject.EMPTY,
                        DBSPTypeTimestamp.INSTANCE,
                        new TimestampString("2020-01-01 10:00:00"))));
        this.testQuery("SELECT CAST(TIME '10:01:01' AS VARIANT)",
                new DBSPVariantExpression(new DBSPTimeLiteral(
                        CalciteObject.EMPTY,
                        new DBSPTypeTime(CalciteObject.EMPTY, false),
                        new TimeString("10:01:01"))));
        this.testQuery("SELECT CAST(INTERVAL '4-1' YEARS TO MONTHS AS VARIANT)",
                new DBSPVariantExpression(DBSPLongIntervalLiteral.fromMonths(
                        DBSPTypeLongInterval.Units.YEARS_TO_MONTHS, 49)));
        this.testQuery("SELECT CAST(INTERVAL '4 10:01' DAYS TO MINUTES AS VARIANT)",
                new DBSPVariantExpression(DBSPShortIntervalLiteral.fromMicroseconds(
                        DBSPTypeShortInterval.Units.SECONDS, 1000_000L * (4 * 86400 + 10 * 3600 + 60), false)));
        this.testQuery("SELECT CAST(CAST(1 AS VARIANT) AS VARIANT)",
                new DBSPVariantExpression(new DBSPI32Literal(1)));
        DBSPTypeBinary binary = new DBSPTypeBinary(CalciteObject.EMPTY, 2, false, false);
        DBSPBinaryLiteral lit = new DBSPBinaryLiteral(CalciteObject.EMPTY, binary, new byte[] { 1, 2 });
        this.testQuery("SELECT CAST(x'0102' AS VARIANT)",
                new DBSPVariantExpression(lit));
        this.testQuery("SELECT CAST(CAST(x'0102' AS VARBINARY) AS VARIANT)",
                new DBSPVariantExpression(lit));
    }

    @Test
    public void testVariantAsMap() {
        // One can access fields by name in a VARIANT, even if the
        // variant does not have named fields
        this.testQuery("SELECT CAST(ARRAY[1,2,3] AS VARIANT)['name']",
            DBSPLiteral.none(DBSPTypeVariant.INSTANCE_NULLABLE));
        // One can access fields by name in a VARIANT, even if the
        // variant does not have named fields
        this.testQuery("SELECT CAST(ARRAY[1,2,3] AS VARIANT).\"name\"",
                DBSPLiteral.none(DBSPTypeVariant.INSTANCE_NULLABLE));
        // One can access fields by index in a VARIANT
        this.testQuery("SELECT CAST(Map[1,'a',2,'b',3,'c'] AS VARIANT)[1]",
                new DBSPVariantExpression(new DBSPStringLiteral("a"), true));
        this.testQuery("SELECT TYPEOF(CAST(Map[1,'a',2,'b',3,'c'] AS VARIANT)[1])",
                new DBSPStringLiteral("VARCHAR"));
        // Note that field name is quoted to match the case of the key
        this.testQuery("SELECT CAST(Map['a',1,'b',2,'c',3] AS VARIANT).\"a\"",
                new DBSPVariantExpression(new DBSPI32Literal(1), true));
        this.testQuery("SELECT CAST(Map['A',1,'b',2,'c',3] AS VARIANT).A",
                DBSPLiteral.none(DBSPTypeVariant.INSTANCE_NULLABLE));
        // The safest way is to index with a string
        this.testQuery("SELECT CAST(Map['a',1,'b',2,'c',3] AS VARIANT)['a']",
                new DBSPVariantExpression(new DBSPI32Literal(1), true));
        // Maps can have variant keys too
        // (but you have to index with a variant).
        this.testQuery("SELECT (Map[CAST('a' AS VARIANT), 1, CAST(1 AS VARIANT), 2])[CAST(1 AS VARIANT)]",
                new DBSPI32Literal(2, true));
        // Navigating a JSON-like object
        this.testQuery("SELECT CAST(MAP['a', CAST(1 AS VARIANT), " +
                "                             'b', CAST('abc' AS VARIANT), " +
                "                             'c', CAST(ARRAY[1,2,3] AS VARIANT)]['c'][1] AS INTEGER)",
                new DBSPI32Literal(1, true));
    }

    @Test
    public void parseJsonTests() {
        this.testQuery("SELECT PARSE_JSON(1)",
                new DBSPVariantExpression(
                        new DBSPU64Literal(BigInteger.ONE, false)));
        this.testQuery("SELECT PARSE_JSON('1')",
                new DBSPVariantExpression(
                        new DBSPU64Literal(BigInteger.ONE, false)));
        this.testQuery("SELECT TYPEOF(PARSE_JSON('1'))",
                new DBSPStringLiteral("BIGINT UNSIGNED"));
        this.testQuery("SELECT PARSE_JSON('\"a\"')",
                new DBSPVariantExpression(
                        new DBSPStringLiteral("a")));
        this.testQuery("SELECT PARSE_JSON('false')",
                new DBSPVariantExpression(
                        new DBSPBoolLiteral(false)));
        this.testQuery("SELECT PARSE_JSON('null')",
                DBSPVariantNullLiteral.variantNull());
        this.testQuery("SELECT TYPEOF(PARSE_JSON('null'))",
                new DBSPStringLiteral("VARIANT"));
        this.testQuery("SELECT PARSE_JSON(null)",
                new DBSPVariantExpression(null, DBSPTypeVariant.INSTANCE_NULLABLE));
        this.testQuery("SELECT PARSE_JSON('[1,2,3]')",
                new DBSPVariantExpression(
                        new DBSPArrayExpression(
                                new DBSPVariantExpression(new DBSPU64Literal(BigInteger.valueOf(1), false)),
                                new DBSPVariantExpression(new DBSPU64Literal(BigInteger.valueOf(2), false)),
                                new DBSPVariantExpression(new DBSPU64Literal(BigInteger.valueOf(3), false)))));
        this.testQuery("SELECT PARSE_JSON('{\"a\": 1, \"b\": 2}')",
                new DBSPVariantExpression(
                        new DBSPMapExpression(
                                new DBSPTypeMap(
                                        DBSPTypeVariant.INSTANCE,
                                        DBSPTypeVariant.INSTANCE, false),
                                Linq.list(
                                        new DBSPVariantExpression(new DBSPStringLiteral("a")),
                                        new DBSPVariantExpression(new DBSPU64Literal(BigInteger.valueOf(1), false)),
                                        new DBSPVariantExpression(new DBSPStringLiteral("b")),
                                        new DBSPVariantExpression(new DBSPU64Literal(BigInteger.valueOf(2), false))))));
        this.testQuery("""
                SELECT PARSE_JSON('{"a": 1.0, "b": [2.2, 3.3, null]}') = CAST(
                   MAP[
                      CAST('a' AS VARIANT), CAST(1.0 AS VARIANT),
                      CAST('b' AS VARIANT), CAST(ARRAY[
                          CAST(2.2 AS VARIANT),
                          CAST(3.3 AS VARIANT),
                          VARIANTNULL()
                                                      ] AS VARIANT)
                      ] AS VARIANT)""",
                new DBSPBoolLiteral(true));
    }

    @Test
    public void testCastVec() {
        this.testQuery("""
                SELECT CAST(PARSE_JSON('["10:10:10"]') AS TIME ARRAY)""",
                new DBSPArrayExpression(true,
                        new DBSPTimeLiteral(
                                CalciteObject.EMPTY, DBSPTypeTime.NULLABLE_INSTANCE, new TimeString("10:10:10"))));
        this.testQuery("""
                SELECT CAST(ARRAY[NULL, 1] AS INT ARRAY)""",
                new DBSPArrayExpression(false,
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true).none(),
                        new DBSPI32Literal(1, true)));
        // result is null, since 1 cannot be converted to a TIME
        this.testQuery("""
                SELECT CAST(PARSE_JSON('["10:10:10", 1]') AS TIME ARRAY)""",
                new DBSPArrayExpression(
                        new DBSPTypeArray(DBSPTypeTime.NULLABLE_INSTANCE, true),
                        true));
        this.testQuery("""
                SELECT CAST(PARSE_JSON('["a", 1.0]') AS VARIANT ARRAY)""",
                new DBSPArrayExpression(true,
                        new DBSPVariantExpression(new DBSPStringLiteral("a", true), true),
                        new DBSPVariantExpression(new DBSPDecimalLiteral(CalciteObject.EMPTY,
                                DBSPTypeDecimal.getDefault(), new BigDecimal(1)), true)));
        this.testQuery("""
                SELECT CAST(ARRAY[NULL, 1] AS VARIANT)""",
                new DBSPVariantExpression(
                        new DBSPArrayExpression(false,
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true).none(),
                                new DBSPI32Literal(1, true))));
    }

    @Test
    public void testCastMap() {
        this.testQuery("""
                SELECT CAST(PARSE_JSON('{"a": 1}') AS MAP<VARIANT, VARIANT>)""",
                new DBSPMapExpression(
                        new DBSPTypeMap(
                                DBSPTypeVariant.INSTANCE,
                                DBSPTypeVariant.INSTANCE_NULLABLE,
                                true),
                                Linq.list(
                                        new DBSPVariantExpression(new DBSPStringLiteral("a")),
                                        new DBSPVariantExpression(new DBSPU64Literal(BigInteger.ONE, false), true))));
        this.testQuery("""
                SELECT CAST(PARSE_JSON('{"a": 1.0}') AS MAP<STRING, VARIANT>)""",
                new DBSPMapExpression(
                        new DBSPTypeMap(
                                DBSPTypeString.varchar(false),
                                DBSPTypeVariant.INSTANCE_NULLABLE,
                                true),
                        Linq.list(
                                new DBSPStringLiteral("a"),
                                new DBSPVariantExpression(new DBSPDecimalLiteral(1), true)
                        )));
        this.testQuery("""
                SELECT CAST(PARSE_JSON('{"a": 1}') AS MAP<STRING, INT>)""",
                new DBSPMapExpression(
                        new DBSPTypeMap(
                                DBSPTypeString.varchar(false),
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true),
                                true),
                        Linq.list(
                                new DBSPStringLiteral("a"),
                                new DBSPI32Literal(1, true)
                        )));
        // Wrong type, result is NULL
        this.testQuery("""
                SELECT CAST(PARSE_JSON('{"a": 1}') AS MAP<STRING, TIMESTAMP>)""",
                new DBSPMapExpression(
                        new DBSPTypeMap(
                                DBSPTypeString.varchar(false),
                                DBSPTypeTimestamp.NULLABLE_INSTANCE,
                                true), null, null));

        this.testQuery("""
                SELECT CAST(MAP['a', 1, 'b', 2] AS VARIANT)""",
                new DBSPVariantExpression(new DBSPMapExpression(
                        new DBSPTypeMap(
                                DBSPTypeString.varchar(false),
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false),
                                true),
                        Linq.list(
                                new DBSPStringLiteral("a"),
                                new DBSPI32Literal(1),
                                new DBSPStringLiteral("b"),
                                new DBSPI32Literal(2)
                        ))));
    }

    @Test
    public void unparseJsonTests() {
        DBSPExpression NULL = DBSPStringLiteral.none(DBSPTypeString.varchar(true));
        this.testQuery("SELECT TO_JSON(PARSE_JSON(1))",
                new DBSPStringLiteral("1", true));
        this.testQuery("SELECT TO_JSON(null)",
                NULL);
        this.testQuery("SELECT TO_JSON(PARSE_JSON('1'))",
                new DBSPStringLiteral("1", true));
        this.testQuery("SELECT TO_JSON(PARSE_JSON('\"a\"'))",
                new DBSPStringLiteral("\"a\"", true));
        this.testQuery("SELECT TO_JSON(PARSE_JSON('false'))",
                        new DBSPStringLiteral("false", true));
        this.testQuery("SELECT TO_JSON(PARSE_JSON('null'))",
                new DBSPStringLiteral("null", true));
        this.testQuery("SELECT TO_JSON(PARSE_JSON(null))",
                DBSPTypeString.varchar(true).none());
        this.testQuery("SELECT TO_JSON(PARSE_JSON('[1,2,3]'))",
                new DBSPStringLiteral("[1,2,3]", true));
        this.testQuery("SELECT TO_JSON(PARSE_JSON('{\"a\":1,\"b\":2}'))",
                new DBSPStringLiteral("{\"a\":1,\"b\":2}", true));

        this.testQuery("SELECT PARSE_JSON('{ \"a\": 1, \"b\": 2 }') = PARSE_JSON('{\"b\":2,\"a\":1}')",
                new DBSPBoolLiteral(true));

        // Dates are deserialized as strings
        this.testQuery("SELECT TO_JSON(CAST(DATE '2020-01-01' AS VARIANT))",
                new DBSPStringLiteral("\"2020-01-01\"", true));
        // timestamps are unparsed as strings (timezone is always +00)
        this.testQuery("SELECT TO_JSON(CAST(TIMESTAMP '2020-01-01 10:00:00' AS VARIANT))",
                new DBSPStringLiteral("\"2020-01-01 10:00:00\"", true));
    }

    @Test
    public void structTests() {
        this.testQuery("SELECT TO_JSON(CAST(t(ARRAY[s(2, 'a', ARRAY[1, NULL, 3]), s(3, 'b', array())]) AS VARIANT))",
                new DBSPStringLiteral("{\"sa\":[{\"a\":[1,null,3],\"i\":2,\"s\":\"a\"},{\"a\":[],\"i\":3,\"s\":\"b\"}]}", true));
        this.testQuery("SELECT CAST(s(2, 'a', ARRAY[1, 2, 3]) AS VARIANT)",
                new DBSPVariantExpression(
                        new DBSPMapExpression(
                                new DBSPTypeMap(DBSPTypeString.varchar(false),
                                        DBSPTypeVariant.INSTANCE, false),
                                Linq.list(
                                        new DBSPStringLiteral("i"),
                                        new DBSPStringLiteral("s"),
                                        new DBSPStringLiteral("a")
                                ),
                                Linq.list(
                                        new DBSPVariantExpression(new DBSPI32Literal(2)),
                                        new DBSPVariantExpression(new DBSPStringLiteral("a")),
                                        new DBSPVariantExpression(new DBSPArrayExpression(
                                                new DBSPI32Literal(1),
                                                new DBSPI32Literal(2),
                                                new DBSPI32Literal(3)))))));
        this.testQuery("SELECT TO_JSON(CAST(s(2, 'a', ARRAY[1, 2, 3]) AS VARIANT))",
                new DBSPStringLiteral("{\"a\":[1,2,3],\"i\":2,\"s\":\"a\"}", true));
        this.testQuery("SELECT CAST(PARSE_JSON('{\"i\": 2, \"s\": \"a\", \"a\": [1, 2, 3]}') AS S)",
                new DBSPTupleExpression(true,
                        new DBSPI32Literal(2, true),
                        new DBSPStringLiteral("a", true),
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true))));
        this.testQuery("SELECT CAST(PARSE_JSON('{\"sa\": [{\"i\": 2, \"s\": \"a\", \"a\": [1, 2, 3]}]}') AS T)",
                new DBSPTupleExpression(true,
                        new DBSPArrayExpression(true,
                                new DBSPTupleExpression(true,
                                        new DBSPI32Literal(2, true),
                                        new DBSPStringLiteral("a", true),
                                        new DBSPArrayExpression(true,
                                                new DBSPI32Literal(1, true),
                                                new DBSPI32Literal(2, true),
                                                new DBSPI32Literal(3, true))))));
    }

    @Test
    public void testCastMapToStruct() {
        DBSPType i32 = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true);
        this.testQuery("SELECT CAST(CAST(MAP['i', 0] AS VARIANT) AS S)",
                new DBSPTupleExpression(true,
                        new DBSPI32Literal(0, true),
                        DBSPTypeString.varchar(true).none(),
                        new DBSPTypeArray(i32, true).none()));
        this.testQuery("SELECT CAST(CAST(MAP['i', 's'] AS VARIANT) AS S)",
                new DBSPTupleExpression(true,
                        i32.none(),
                        DBSPTypeString.varchar(true).none(),
                        new DBSPTypeArray(i32, true).none()));
        this.testQuery("SELECT CAST(CAST(MAP['I', 0] AS VARIANT) AS S)",
                new DBSPTupleExpression(true,
                        i32.none(),
                        DBSPTypeString.varchar(true).none(),
                        new DBSPTypeArray(i32, true).none()));
        this.testQuery("SELECT CAST(CAST(MAP['i', 0, 'X', 2] AS VARIANT) AS S)",
                new DBSPTupleExpression(true,
                        new DBSPI32Literal(0, true),
                        DBSPTypeString.varchar(true).none(),
                        new DBSPTypeArray(i32, true).none()));
    }

    @Test
    public void testSparkInline() {
        // inline(from_json(x:steps, 'Array<struct<name STRING, uuid STRING>>'))
        // as (name, uuid)
        String data = """
                '{ "steps": [ { "name": "blah", "uuid": "uuid0" }, { "name": "boo", "uuid": null } ] }'
                """;
        var ccs = this.getCCS("""
                -- input table with string data encoded as json
                CREATE TABLE DATA(encoded VARCHAR);
                -- type of array element
                CREATE TYPE T_ELEM AS (name VARCHAR, "uuid" VARCHAR);
                -- type that contains an array field called 'steps' with elements of type T_ELEM
                CREATE TYPE T_STEPS AS (steps T_ELEM ARRAY);
                -- function which takes a string and returns an object with type T_STEPS
                CREATE FUNCTION jsonstring_as_t_steps(line VARCHAR) RETURNS T_STEPS;
                -- parse the JSON data into a view DECODE which has elements of type T_STEPS
                CREATE LOCAL VIEW DECODE(rec) AS SELECT jsonstring_as_t_steps(encoded) as steps FROM DATA;
                -- extract and flatten the arrays from the DECODE view
                CREATE VIEW OUT(name, "uuid") AS SELECT x.name, x."uuid" FROM DECODE, UNNEST(DECODE.rec.steps) AS x;
                """).withStringTrim();
        ccs.stepWeightOne("INSERT INTO DATA VALUES (" + data + ")",
                """
                         name | uuid
                        -------------
                         blah | uuid0
                         boo  |NULL""");
    }

    @Test
    public void issue5938() {
        // type -> Variant -> string
        this.qst("""
                SELECT CAST(CAST(1 AS VARIANT) AS VARCHAR);
                 r
                ---
                 1
                (1 row)
                
                SELECT CAST(CAST('a' AS VARIANT) AS VARCHAR);
                 r
                ---
                 a
                (1 row)
                
                SELECT CAST(CAST(1.5 AS VARIANT) AS VARCHAR);
                 r
                ---
                 1.5
                (1 row)
                
                 SELECT CAST(CAST(1.5e0 AS VARIANT) AS VARCHAR);
                 r
                ---
                 1.5
                (1 row)
                
                SELECT CAST(CAST(TRUE AS VARIANT) AS VARCHAR);
                 r
                ---
                 true
                (1 row)
                
                SELECT CAST(CAST(UUID '123e4567-e89b-12d3-a456-426655440000' AS VARIANT) AS VARCHAR);
                 r
                ---
                 123e4567-e89b-12d3-a456-426655440000
                (1 row)
                
                SELECT CAST(CAST(INTERVAL '+1 10:10:10.123' DAYS TO SECONDS AS VARIANT) AS VARCHAR);
                 r
                ---
                 +1 10:10:10.123000
                (1 row)

                SELECT CAST(CAST(x'0abc' AS VARIANT) AS VARCHAR);
                 r
                ---
                 0abc
                (1 row)
                
                SELECT CAST(CAST(TIME '10:00:00.123' AS VARIANT) AS VARCHAR);
                 r
                ---
                 10:00:00.123000000
                (1 row)
                
                SELECT CAST(CAST(TIMESTAMP '2024-02-02 10:00:00.123' AS VARIANT) AS VARCHAR);
                 r
                ---
                 2024-02-02 10:00:00.123000
                (1 row)

                SELECT CAST(CAST(DATE '2024-02-02' AS VARIANT) AS VARCHAR);
                 r
                ---
                 2024-02-02
                (1 row)
                """);
    }

    @Test
    public void issue5938a() {
        // String -> Variant -> type
        this.qst("""
                SELECT CAST(CAST('1' AS VARIANT) AS INT);
                 r
                ---
                 1
                (1 row)
                
                SELECT CAST(CAST('1.5' AS VARIANT) AS DECIMAL(10, 1));
                 r
                ---
                 1.5
                (1 row)
                
                SELECT CAST(CAST('1.5e0' AS VARIANT) AS DOUBLE);
                 r
                ---
                 1.5
                (1 row)
                
                SELECT CAST(CAST('TRUE' AS VARIANT) AS BOOLEAN);
                 r
                ---
                 true
                (1 row)
                
                SELECT CAST(CAST('123e4567-e89b-12d3-a456-426655440000' AS VARIANT) AS UUID);
                 r
                ---
                 123e4567-e89b-12d3-a456-426655440000
                (1 row)
                
                SELECT CAST(CAST('+1 10:10:10.123' AS VARIANT) AS INTERVAL DAYS TO SECONDS);
                 r
                ---
                 1 days 10 hours 10 mins 10.123000 secs
                (1 row)

                SELECT CAST(CAST('10:00:00.123' AS VARIANT) AS TIME);
                 r
                ---
                 10:00:00.123000000
                (1 row)
                
                SELECT CAST(CAST('2024-02-02 10:00:00.123' AS VARIANT) AS TIMESTAMP);
                 r
                ---
                 2024-02-02 10:00:00.123000
                (1 row)

                SELECT CAST(CAST('2024-02-02' AS VARIANT) AS DATE);
                 r
                ---
                 2024-02-02
                (1 row)""");
    }
}
