package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.junit.Test;

public class SerdeTest extends SqlIoTest {
    @Test
    public void jsonStructTest() {
        String ddl = """
            CREATE TYPE address AS (
               number INT
            );
            CREATE TABLE PERS(s VARCHAR);
            CREATE FUNCTION jsonstring_as_address(s VARCHAR) RETURNS address;
            CREATE VIEW V AS SELECT Z.x.number FROM
               (SELECT jsonstring_as_address(PERS.s) AS x FROM PERS) Z
            WHERE z.x is NOT NULL;
            """;
        DBSPCompiler compiler = this.testCompiler();
        // This test behaves very differnetly if we don't use this option.
        compiler.options.languageOptions.unquotedCasing = "lower";
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        // This will be converted properly to a struct
        DBSPExpression address0 = new DBSPTupleExpression(
                new DBSPStringLiteral("{ \"NUMBER\": 2 }", true)
        );
        // This will be converted properly to a struct
        DBSPExpression address1 = new DBSPTupleExpression(
                new DBSPStringLiteral("{ \"number\": 2 }", true)
        );
        // This will generate a null json
        DBSPExpression invalid = new DBSPTupleExpression(
                new DBSPStringLiteral("5", true)
        );
        // This will generate a null json
        DBSPExpression invalidJson = new DBSPTupleExpression(
                new DBSPStringLiteral("{{", true)
        );
        DBSPExpression person0 = new DBSPTupleExpression(
                new DBSPI32Literal(2, true)
        );
        DBSPZSetLiteral input = new DBSPZSetLiteral(address0, address1, invalid, invalidJson);
        DBSPZSetLiteral output = new DBSPZSetLiteral(person0, person0
                // new DBSPTupleExpression(new DBSPI32Literal()));
        );
        ccs.addPair(new Change(input), new Change(output));
        this.addRustTestCase(ccs);
    }
}
