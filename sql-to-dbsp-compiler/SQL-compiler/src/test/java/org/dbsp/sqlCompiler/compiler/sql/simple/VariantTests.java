package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPMapLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.util.Linq;
import org.junit.Test;

public class VariantTests extends BaseSQLTests {
    /** Return the default compiler used for testing. */
    @Override
    public DBSPCompiler testCompiler() {
        // Do not optimize, esp in Calcite
        CompilerOptions options = this.testOptions(false, false);
        return new DBSPCompiler(options);
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
        // However, you have to use the right type, or you get NULL
        this.testQuery("SELECT CAST(CAST(1 AS VARIANT) AS TINYINT)",
                new DBSPTypeInteger(CalciteObject.EMPTY, 8, true, true).nullValue());
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
                DBSPVariantLiteral.sqlNull(true));
        this.testQuery("SELECT CAST(ARRAY[1,2,3] AS VARIANT)[1]",
                new DBSPVariantLiteral(new DBSPI32Literal(1), true));
        // Acessing items in a VARIANT array returns VARIANT values,
        // even if the array itself does not contain VARIANT values
        // (Otherwise TYPEOF would not compile)
        this.testQuery("SELECT TYPEOF(CAST(ARRAY[1,2,3] AS VARIANT)[1])",
                new DBSPStringLiteral("INTEGER"));
    }

    @Test
    public void testVariantAsMap() {
        // One can access fields by name in a VARIANT, even if the
        // variant does not have named fields
        this.testQuery("SELECT CAST(ARRAY[1,2,3] AS VARIANT)['name']",
            DBSPVariantLiteral.sqlNull(true));
        // One can access fields by name in a VARIANT, even if the
        // variant does not have named fields
        this.testQuery("SELECT CAST(ARRAY[1,2,3] AS VARIANT).\"name\"",
                DBSPVariantLiteral.sqlNull(true));
        // One can access fields by index in a VARIANT
        this.testQuery("SELECT CAST(Map[1,'a',2,'b',3,'c'] AS VARIANT)[1]",
                new DBSPVariantLiteral(new DBSPStringLiteral("a"), true));
        this.testQuery("SELECT TYPEOF(CAST(Map[1,'a',2,'b',3,'c'] AS VARIANT)[1])",
                new DBSPStringLiteral("VARCHAR"));
        // Note that field name is quoted to match the case of the key
        this.testQuery("SELECT CAST(Map['a',1,'b',2,'c',3] AS VARIANT).\"a\"",
                new DBSPVariantLiteral(new DBSPI32Literal(1), true));
        // Unquoted field may not match, depending on dialect
        this.testQuery("SELECT CAST(Map['a',1,'b',2,'c',3] AS VARIANT).a",
                DBSPVariantLiteral.sqlNull(true));
        // The safest way is to use an index
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
}
