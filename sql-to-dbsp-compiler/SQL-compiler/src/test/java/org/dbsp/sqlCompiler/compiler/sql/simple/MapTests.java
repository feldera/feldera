package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPMapLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
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
        this.addRustTestCase(query, ccs);
    }

    private void testQuery(String statements, String query) {
        this.testQuery(statements, query, new InputOutputChangeStream());
    }

    private void testQuery(String query, DBSPZSetLiteral literal) {
        this.testQuery("", query,
                new InputOutputChangeStream().addChange(
                        new InputOutputChange(new Change(), new Change(literal))));
    }

    @Test
    public void mapLiteralTest() {
        String query = "SELECT MAP['hi',2]";
        DBSPType str = DBSPTypeString.varchar(false);
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPMapLiteral(
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
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(
                        new DBSPI32Literal(2, true),
                        new DBSPI32Literal())));
    }
}
