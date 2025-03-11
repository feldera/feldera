package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
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
        // This test behaves very differently if we don't use this option.
        compiler.options.languageOptions.unquotedCasing = "lower";
        compiler.submitStatementsForCompilation(ddl);
        CompilerCircuitStream ccs = this.getCCS(compiler);
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
        DBSPZSetExpression input = new DBSPZSetExpression(address0, address1, invalid, invalidJson);
        DBSPZSetExpression output = new DBSPZSetExpression(person0, person0);
        ccs.addPair(new Change(input), new Change(output));
    }

    @Test
    public void jsonStructTest2() {
        // Matches the example in the documentation json.md
        String ddl = """
            CREATE TYPE address2 AS (
               city VARCHAR,
               street VARCHAR,
               number INT
            );
            CREATE TABLE data(addr VARCHAR);
            CREATE FUNCTION jsonstring_as_address2(addr VARCHAR) RETURNS address2;

            CREATE VIEW decoded0 AS
            SELECT CAST(PARSE_JSON(data.addr) AS address2) FROM data;

            CREATE VIEW decoded1
            AS SELECT jsonstring_as_address2(data.addr) FROM data;
            """;
        DBSPCompiler compiler = this.testCompiler();
        // This test behaves very differently if we don't use this option.
        compiler.options.languageOptions.unquotedCasing = "lower";
        compiler.submitStatementsForCompilation(ddl);
        CompilerCircuitStream ccs = this.getCCS(compiler);
        DBSPExpression addressIn0 = new DBSPTupleExpression(
                new DBSPStringLiteral("{ \"city\": \"Boston\", \"street\": \"Main\", \"number\": 10 }", true)
        );
        DBSPTupleExpression addressOut0 = new DBSPTupleExpression(new DBSPTupleExpression(true,
                new DBSPStringLiteral("Boston", true),
                new DBSPStringLiteral("Main", true),
                new DBSPI32Literal(10, true)
        ));
        DBSPZSetExpression input0 = new DBSPZSetExpression(addressIn0);
        DBSPZSetExpression output0 = new DBSPZSetExpression(addressOut0);
        ccs.addPair(new Change(input0), new Change(output0, output0));

        DBSPExpression addressIn1 = new DBSPTupleExpression(
                new DBSPStringLiteral("{ \"city\": \"Boston\", \"street\": \"Main\", \"NUMBER\": 10 }", true)
        );
        DBSPZSetExpression input1 = new DBSPZSetExpression(addressIn1);
        DBSPZSetExpression output1 = new DBSPZSetExpression(addressOut0);
        assert addressOut0.fields != null;
        ccs.addPair(new Change(input1), new Change(new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPTupleExpression(true, 
                        new DBSPStringLiteral("Boston", true),
                        new DBSPStringLiteral("Main", true),
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true).none()))), output1));
    }
}
