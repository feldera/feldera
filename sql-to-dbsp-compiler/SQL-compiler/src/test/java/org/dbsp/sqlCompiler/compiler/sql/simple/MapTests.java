package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChangeStream;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.util.Linq;
import org.junit.Test;

import java.nio.charset.Charset;

public class MapTests extends BaseSQLTests {
    public DBSPCompiler compileQuery(String statements, String query) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.optimizationLevel = 0;
        compiler.compileStatements(statements);
        compiler.compileStatement(query);
        return compiler;
    }

    void testQuery(String statements, String query, InputOutputChangeStream streams) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(statements, query);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler, streams);
        this.addRustTestCase(ccs);
    }

    private void testQuery(String query, DBSPZSetExpression literal) {
        this.testQuery("", query,
                new InputOutputChangeStream().addChange(
                        new InputOutputChange(new Change(), new Change(literal))));
    }

    @Test
    public void mapLiteralTest() {
        String query = "SELECT MAP['hi',2]";
        DBSPType str = DBSPTypeString.varchar(false);
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPMapExpression(
                        new DBSPTypeMap(
                                str,
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false),
                                false),
                        Linq.list(new DBSPStringLiteral(CalciteObject.EMPTY, str, "hi", Charset.defaultCharset()),
                                new DBSPI32Literal(2))))));
    }

    @Test
    public void mapIndexTest() {
        String query = "SELECT MAP['hi',2]['hi'], MAP['hi',2]['x']";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(2, true),
                        new DBSPI32Literal())));
    }

    @Test
    public void mapBlackboxTest() {
        String query = "SELECT blackbox(MAP['hi',2])";
        DBSPType str = DBSPTypeString.varchar(false);
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPMapExpression(
                        new DBSPTypeMap(
                                str,
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false),
                                false),
                        Linq.list(new DBSPStringLiteral(CalciteObject.EMPTY, str, "hi", Charset.defaultCharset()),
                                new DBSPI32Literal(2))))));
    }

    @Test
    public void mapCardinalityTest() {
        String query = "SELECT CARDINALITY(MAP['hi',2])";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(1))));
    }

    @Test
    public void testMapSubquery() {
        String ddl = "CREATE TABLE T(v varchar, x int)";
        String query = "SELECT MAP(SELECT * FROM T)";
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPStringLiteral("hello", true),
                        new DBSPI32Literal(10, true)),
                new DBSPTupleExpression(
                        new DBSPStringLiteral("there", true),
                        new DBSPI32Literal(5, true)));
        DBSPTypeMap mapType = new DBSPTypeMap(
                DBSPTypeString.varchar(true),
                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true ,true), false);
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPMapExpression(
                                mapType,
                                Linq.list(
                                        new DBSPStringLiteral("there", true),
                                        new DBSPI32Literal(5, true),
                                        new DBSPStringLiteral("hello", true),
                                        new DBSPI32Literal(10, true))
                        )));
        this.testQuery(ddl, query,
                new InputOutputChangeStream().addPair(new Change(input), new Change(result)));
    }
}
