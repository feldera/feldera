package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPMapLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.util.Linq;
import org.junit.Test;

import java.math.BigDecimal;

public class VariantTests extends BaseSQLTests {
    /** Return the default compiler used for testing. */
    @Override
    public DBSPCompiler testCompiler() {
        // Do not optimize, esp in Calcite
        CompilerOptions options = this.testOptions(false, false);
        DBSPCompiler compiler = new DBSPCompiler(options);
        this.prepareInputs(compiler);
        return compiler;
    }

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.compileStatements("""
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
        DBSPZSetLiteral expectedOutput = new DBSPZSetLiteral(new DBSPTupleExpression(fields));
        InputOutputChange change = new InputOutputChange(new Change(), new Change(expectedOutput));
        ccs.addChange(change);
        this.addRustTestCase(ccs);
    }

    @Test
    public void testVariant() {
        // adapted from Calcite variant.iq
        this.testQuery("SELECT CAST(1 AS VARIANT)",
                new DBSPVariantLiteral(new DBSPI32Literal(1)));
        this.testQuery("SELECT TYPEOF(CAST(1 AS VARIANT))",
                new DBSPStringLiteral("INTEGER"));
        // The runtime knows that this is a TINYINT
        this.testQuery("SELECT CAST(CAST(1 AS TINYINT) AS VARIANT)",
                new DBSPVariantLiteral(new DBSPI8Literal((byte) 1)));
        // Converting something to VARIANT and back works
        this.testQuery("SELECT CAST(CAST(1 AS VARIANT) AS INT)",
                new DBSPI32Literal(1, true));
        // Numeric type conversions are performed automatically
        this.testQuery("SELECT CAST(CAST(1 AS VARIANT) AS TINYINT)",
                new DBSPI8Literal((byte) 1, true));
        // Some VARIANT objects when output receive double quotes
        this.testQuery("select CAST('string' as VARIANT)",
                new DBSPVariantLiteral(new DBSPStringLiteral("string")));
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
                new DBSPVecLiteral(
                        new DBSPVariantLiteral(new DBSPI32Literal(1)),
                        new DBSPVariantLiteral(new DBSPStringLiteral("abc"))));
        // A map with VARCHAR keys and VARIANT values
        this.testQuery("SELECT MAP['a', CAST(1 AS VARIANT), 'b', CAST('abc' AS VARIANT), 'c', CAST(ARRAY[1,2,3] AS VARIANT)]",
                new DBSPMapLiteral(
                        new DBSPTypeMap(
                                DBSPTypeString.varchar(false),
                                new DBSPTypeVariant(CalciteObject.EMPTY,
                                        false),
                                false),
                        Linq.list(new DBSPStringLiteral("a"),
                                new DBSPStringLiteral("b"),
                                new DBSPStringLiteral("c")),
                        Linq.list(new DBSPVariantLiteral(new DBSPI32Literal(1)),
                                new DBSPVariantLiteral(new DBSPStringLiteral("abc")),
                                new DBSPVariantLiteral(new DBSPVecLiteral(
                                        new DBSPI32Literal(1),
                                        new DBSPI32Literal(2),
                                        new DBSPI32Literal(3)
                                )))));
        // Variant values allow access by index, but return null if they are not arrays
        this.testQuery("SELECT (CAST(1 AS VARIANT))[1]",
                DBSPZSetLiteral.none(new DBSPTypeVariant(true)));
        this.testQuery("SELECT CAST(ARRAY[1,2,3] AS VARIANT)[1]",
                new DBSPVariantLiteral(new DBSPI32Literal(1), true));
        // Acessing items in a VARIANT array returns VARIANT values,
        // even if the array itself does not contain VARIANT values
        // (Otherwise TYPEOF would not compile)
        this.testQuery("SELECT TYPEOF(CAST(ARRAY[1,2,3] AS VARIANT)[1])",
                new DBSPStringLiteral("INTEGER"));
        this.testQuery("SELECT CAST(DATE '2020-01-01' AS VARIANT)",
                new DBSPVariantLiteral(new DBSPDateLiteral("2020-01-01")));
        this.testQuery("SELECT CAST(TIMESTAMP '2020-01-01 10:00:00' AS VARIANT)",
                new DBSPVariantLiteral(new DBSPTimestampLiteral(
                        CalciteObject.EMPTY,
                        new DBSPTypeTimestamp(CalciteObject.EMPTY, false),
                        new TimestampString("2020-01-01 10:00:00"))));
        this.testQuery("SELECT CAST(TIME '10:01:01' AS VARIANT)",
                new DBSPVariantLiteral(new DBSPTimeLiteral(
                        CalciteObject.EMPTY,
                        new DBSPTypeTime(CalciteObject.EMPTY, false),
                        new TimeString("10:01:01"))));
        this.testQuery("SELECT CAST(INTERVAL '4-1' YEARS TO MONTHS AS VARIANT)",
                new DBSPVariantLiteral(new DBSPIntervalMonthsLiteral(49)));
        this.testQuery("SELECT CAST(INTERVAL '4 10:01' DAYS TO MINUTES AS VARIANT)",
                new DBSPVariantLiteral(new DBSPIntervalMillisLiteral(1000L * (4 * 86400 + 10 * 3600 + 60), false)));
        this.testQuery("SELECT CAST(CAST(1 AS VARIANT) AS VARIANT)",
                new DBSPVariantLiteral(new DBSPI32Literal(1)));
        this.testQuery("SELECT CAST(x'0102' AS VARIANT)",
                new DBSPVariantLiteral(new DBSPBinaryLiteral(new byte[] { 1, 2 })));
        this.testQuery("SELECT CAST(CAST(x'0102' AS VARBINARY) AS VARIANT)",
                new DBSPVariantLiteral(new DBSPBinaryLiteral(new byte[] { 1, 2 })));
    }

    @Test
    public void testVariantAsMap() {
        // One can access fields by name in a VARIANT, even if the
        // variant does not have named fields
        this.testQuery("SELECT CAST(ARRAY[1,2,3] AS VARIANT)['name']",
            DBSPLiteral.none(new DBSPTypeVariant(true)));
        // One can access fields by name in a VARIANT, even if the
        // variant does not have named fields
        this.testQuery("SELECT CAST(ARRAY[1,2,3] AS VARIANT).\"name\"",
                DBSPLiteral.none(new DBSPTypeVariant(true)));
        // One can access fields by index in a VARIANT
        this.testQuery("SELECT CAST(Map[1,'a',2,'b',3,'c'] AS VARIANT)[1]",
                new DBSPVariantLiteral(new DBSPStringLiteral("a"), true));
        this.testQuery("SELECT TYPEOF(CAST(Map[1,'a',2,'b',3,'c'] AS VARIANT)[1])",
                new DBSPStringLiteral("VARCHAR"));
        // Note that field name is quoted to match the case of the key
        this.testQuery("SELECT CAST(Map['a',1,'b',2,'c',3] AS VARIANT).\"a\"",
                new DBSPVariantLiteral(new DBSPI32Literal(1), true));
        // Unquoted field may not match, depending on the 'unquotedCasing' compiler flag
        this.testQuery("SELECT CAST(Map['A',1,'b',2,'c',3] AS VARIANT).A",
                DBSPLiteral.none(new DBSPTypeVariant(true)));
        // The safest way is to index with a string
        this.testQuery("SELECT CAST(Map['a',1,'b',2,'c',3] AS VARIANT)['a']",
                new DBSPVariantLiteral(new DBSPI32Literal(1), true));
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
                new DBSPVariantLiteral(
                        new DBSPDecimalLiteral(1)));
        this.testQuery("SELECT PARSE_JSON('1')",
                new DBSPVariantLiteral(
                        new DBSPDecimalLiteral(1)));
        this.testQuery("SELECT TYPEOF(PARSE_JSON('1'))",
                new DBSPStringLiteral("DECIMAL"));
        this.testQuery("SELECT PARSE_JSON('\"a\"')",
                new DBSPVariantLiteral(
                        new DBSPStringLiteral("a")));
        this.testQuery("SELECT PARSE_JSON('false')",
                new DBSPVariantLiteral(
                        new DBSPBoolLiteral(false)));
        this.testQuery("SELECT PARSE_JSON('null')",
                DBSPVariantNullLiteral.variantNull());
        this.testQuery("SELECT TYPEOF(PARSE_JSON('null'))",
                new DBSPStringLiteral("VARIANT"));
        this.testQuery("SELECT PARSE_JSON(null)",
                DBSPVariantLiteral.none(new DBSPTypeVariant(CalciteObject.EMPTY, true)));
        this.testQuery("SELECT PARSE_JSON('[1,2,3]')",
                new DBSPVariantLiteral(
                        new DBSPVecLiteral(
                                new DBSPVariantLiteral(new DBSPDecimalLiteral(1)),
                                new DBSPVariantLiteral(new DBSPDecimalLiteral(2)),
                                new DBSPVariantLiteral(new DBSPDecimalLiteral(3)))));
        this.testQuery("SELECT PARSE_JSON('{\"a\": 1, \"b\": 2}')",
                new DBSPVariantLiteral(
                        new DBSPMapLiteral(
                                new DBSPTypeMap(
                                        new DBSPTypeVariant(false),
                                        new DBSPTypeVariant(false), false),
                                Linq.list(
                                        new DBSPVariantLiteral(new DBSPStringLiteral("a")),
                                        new DBSPVariantLiteral(new DBSPDecimalLiteral(1)),
                                        new DBSPVariantLiteral(new DBSPStringLiteral("b")),
                                        new DBSPVariantLiteral(new DBSPDecimalLiteral(2))))));
        this.testQuery("""
                SELECT PARSE_JSON('{"a": 1, "b": [2, 3.3, null]}') = CAST(
                   MAP[
                      CAST('a' AS VARIANT), CAST(1.0 AS VARIANT),
                      CAST('b' AS VARIANT), CAST(ARRAY[
                          CAST(2.0 AS VARIANT),
                          CAST(3.3 AS VARIANT),
                          VARIANTNULL()
                                                      ] AS VARIANT)
                      ] AS VARIANT)""",
                new DBSPBoolLiteral(true));
    }

    @Test
    public void testCastVec() {
        // This is a bug in Calcite, the array should be nullable, and the elements should be nullable too
        this.testQuery("""
                SELECT CAST(ARRAY[NULL, 1] AS INT ARRAY)""",
                new DBSPVecLiteral(false,
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true).none(),
                        new DBSPI32Literal(1, true)));
        // result is null, since 1 cannot be converted to a string
        this.testQuery("""
                SELECT CAST(PARSE_JSON('["a", 1]') AS STRING ARRAY)""",
                new DBSPVecLiteral(
                        new DBSPTypeVec(DBSPTypeString.varchar(false), true),
                        true));
        this.testQuery("""
                SELECT CAST(PARSE_JSON('["a", 1]') AS VARIANT ARRAY)""",
                new DBSPVecLiteral(true,
                        new DBSPVariantLiteral(new DBSPStringLiteral("a", true)),
                        new DBSPVariantLiteral(new DBSPDecimalLiteral(CalciteObject.EMPTY,
                                DBSPTypeDecimal.getDefault(), new BigDecimal(1)))));

        this.testQuery("""
                SELECT CAST(ARRAY[NULL, 1] AS VARIANT)""",
                new DBSPVariantLiteral(
                        new DBSPVecLiteral(false,
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true).none(),
                                new DBSPI32Literal(1, true))));
    }

    @Test
    public void testCastMap() {
        this.testQuery("""
                SELECT CAST(PARSE_JSON('{"a": 1}') AS MAP<VARIANT, VARIANT>)""",
                new DBSPMapLiteral(
                        new DBSPTypeMap(
                                new DBSPTypeVariant(false),
                                new DBSPTypeVariant(false),
                                true),
                                Linq.list(
                                        new DBSPVariantLiteral(new DBSPStringLiteral("a")),
                                        new DBSPVariantLiteral(new DBSPDecimalLiteral(1)))));
        this.testQuery("""
                SELECT CAST(PARSE_JSON('{"a": 1}') AS MAP<STRING, VARIANT>)""",
                new DBSPMapLiteral(
                        new DBSPTypeMap(
                                DBSPTypeString.varchar(false),
                                new DBSPTypeVariant(false),
                                true),
                        Linq.list(
                                new DBSPStringLiteral("a"),
                                new DBSPVariantLiteral(new DBSPDecimalLiteral(1))
                        )));
        this.testQuery("""
                SELECT CAST(PARSE_JSON('{"a": 1}') AS MAP<STRING, INT>)""",
                new DBSPMapLiteral(
                        new DBSPTypeMap(
                                DBSPTypeString.varchar(false),
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false),
                                true),
                        Linq.list(
                                new DBSPStringLiteral("a"),
                                new DBSPI32Literal(1)
                        )));
        // Wrong type, result is NULL
        this.testQuery("""
                SELECT CAST(PARSE_JSON('{"a": 1}') AS MAP<STRING, TIMESTAMP>)""",
                new DBSPMapLiteral(
                        new DBSPTypeMap(
                                DBSPTypeString.varchar(false),
                                new DBSPTypeTimestamp(CalciteObject.EMPTY, false),
                                true), null, null));

        this.testQuery("""
                SELECT CAST(MAP['a', 1, 'b', 2] AS VARIANT)""",
                new DBSPVariantLiteral(new DBSPMapLiteral(
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
                DBSPVariantLiteral.none(DBSPTypeString.varchar(true)));
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
        this.testQuery("SELECT CAST(s(2, 'a', ARRAY[1, 2, 3]) AS VARIANT)",
                new DBSPVariantLiteral(
                        new DBSPMapLiteral(
                                new DBSPTypeMap(DBSPTypeString.varchar(false),
                                        new DBSPTypeVariant(false), false),
                                Linq.list(
                                        new DBSPStringLiteral("i"),
                                        new DBSPStringLiteral("s"),
                                        new DBSPStringLiteral("a")
                                ),
                                Linq.list(
                                        new DBSPVariantLiteral(new DBSPI32Literal(2)),
                                        new DBSPVariantLiteral(new DBSPStringLiteral("a")),
                                        new DBSPVariantLiteral(new DBSPVecLiteral(
                                                new DBSPI32Literal(1),
                                                new DBSPI32Literal(2),
                                                new DBSPI32Literal(3)))))));
        this.testQuery("SELECT TO_JSON(CAST(s(2, 'a', ARRAY[1, 2, 3]) AS VARIANT))",
                new DBSPStringLiteral("{\"a\":[1,2,3],\"i\":2,\"s\":\"a\"}", true));
        this.testQuery("SELECT CAST(PARSE_JSON('{\"i\": 2, \"s\": \"a\", \"a\": [1, 2, 3]}') AS S)",
                new DBSPTupleExpression(true,
                        new DBSPI32Literal(2, true),
                        new DBSPStringLiteral("a", true),
                        new DBSPVecLiteral(true,
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true))));
        this.testQuery("SELECT TO_JSON(CAST(t(ARRAY[s(2, 'a', ARRAY[1, NULL, 3]), s(3, 'b', array())]) AS VARIANT))",
                new DBSPStringLiteral("{\"sa\":[{\"a\":[1,null,3],\"i\":2,\"s\":\"a\"},{\"a\":[],\"i\":3,\"s\":\"b\"}]}", true));
        this.testQuery("SELECT CAST(PARSE_JSON('{\"sa\": [{\"i\": 2, \"s\": \"a\", \"a\": [1, 2, 3]}]}') AS T)",
                new DBSPTupleExpression(true,
                        new DBSPVecLiteral(true,
                                new DBSPTupleExpression(true,
                                        new DBSPI32Literal(2, true),
                                        new DBSPStringLiteral("a", true),
                                        new DBSPVecLiteral(true,
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
                        new DBSPTypeVec(i32, true).none()));
        this.testQuery("SELECT CAST(CAST(MAP['i', 's'] AS VARIANT) AS S)",
                new DBSPTupleExpression(true,
                        i32.none(),
                        DBSPTypeString.varchar(true).none(),
                        new DBSPTypeVec(i32, true).none()));
        this.testQuery("SELECT CAST(CAST(MAP['I', 0] AS VARIANT) AS S)",
                new DBSPTupleExpression(true,
                        i32.none(),
                        DBSPTypeString.varchar(true).none(),
                        new DBSPTypeVec(i32, true).none()));
        this.testQuery("SELECT CAST(CAST(MAP['i', 0, 'X', 2] AS VARIANT) AS S)",
                new DBSPTupleExpression(true,
                        new DBSPI32Literal(0, true),
                        DBSPTypeString.varchar(true).none(),
                        new DBSPTypeVec(i32, true).none()));
    }
}
