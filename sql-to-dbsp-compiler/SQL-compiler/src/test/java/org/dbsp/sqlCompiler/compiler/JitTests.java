package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.backend.jit.ToJitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPGeoPointLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Logger;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;

/**
 * Runs tests using the JIT compiler backend and runtime.
 */
public class JitTests extends EndToEndTests {
    @Override
    void testQuery(String query, DBSPZSetLiteral.Contents expectedOutput) {
        DBSPZSetLiteral.Contents input = this.createInput();
        super.testQueryBase(query, false, true, true, new InputOutputPair(input, expectedOutput));
    }

    // All the @Ignore-ed tests below should eventually pass.

    @Test @Override @Ignore("Average not yet implemented")
    public void averageTest() {
        Logger.INSTANCE.setLoggingLevel(ToJitVisitor.class, 4);
        String query = "SELECT AVG(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10, true))));
    }

    @Test @Override @Ignore("Average not yet implemented")
    public void constAggregateExpression2() {
        Logger.INSTANCE.setLoggingLevel(ToJitVisitor.class, 4);
        String query = "SELECT 34 / AVG (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPI32Literal(34, true))));
    }

    @Test @Override @Ignore("Uses Decimals, not yet supported by JIT")
    public void divZero() {
        String query = "SELECT 'Infinity' / 0";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(null, DBSPTypeDecimal.DEFAULT_NULLABLE,
                                null))));
    }

    @Test @Override @Ignore("Uses Decimals, not yet supported by JIT")
    public void correlatedAggregate() {
        String query = "SELECT Sum(r.COL1 * r.COL5) FROM T r\n" +
                "WHERE\n" +
                "0.5 * (SELECT Sum(r1.COL5) FROM T r1) =\n" +
                "(SELECT Sum(r2.COL5) FROM T r2 WHERE r2.COL1 = r.COL1)";
        this.testQuery(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32))));
    }

    @Test @Override @Ignore("Uses Decimals, not yet supported by JIT")
    public void decimalParse() {
        String query = "SELECT CAST('0.5' AS DECIMAL)";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(null, DBSPTypeDecimal.DEFAULT,
                                new BigDecimal("0.5")))));
    }

    @Test @Override @Ignore("Uses Decimals, not yet supported by JIT")
    public void decimalParseFail() {
        String query = "SELECT CAST('blah' AS DECIMAL)";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(null, DBSPTypeDecimal.DEFAULT,
                                new BigDecimal(0)))));
    }

    @Test @Override @Ignore("WINDOWS not yet implemented")
    public void overTest() {
        DBSPExpression t = new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPI64Literal(2));
        String query = "SELECT T.COL1, COUNT(*) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test @Override @Ignore("WINDOWS not yet implemented")
    public void overSumTest() {
        DBSPExpression t = new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(13.0));
        String query = "SELECT T.COL1, SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test @Override @Ignore("WINDOWS not yet implemented")
    public void overTwiceTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(13.0),
                new DBSPI64Literal(2));
        String query = "SELECT T.COL1, " +
                "SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING), " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test @Override @Ignore("WINDOWS not yet implemented")
    public void overConstantWindowTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPI64Literal(2));
        String query = "SELECT T.COL1, " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test @Override @Ignore("WINDOWS not yet implemented")
    public void overTwiceDifferentTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(13.0),
                new DBSPI64Literal(2));
        String query = "SELECT T.COL1, " +
                "SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING), " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test @Override @Ignore("ORDER BY not yet implemented")
    public void orderbyTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPVecLiteral(e1, e0)
        ));
    }

    @Test @Override @Ignore("ORDER BY not yet implemented")
    public void orderbyDescendingTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2 DESC";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPVecLiteral(e0, e1)
        ));
    }

    @Test @Override @Ignore("ORDER BY not yet implemented")
    public void orderby2Test() {
        String query = "SELECT * FROM T ORDER BY T.COL2, T.COL1";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPVecLiteral(e1, e0)
        ));
    }

    @Test @Override @Ignore("GEO POINT not yet implemented")
    public void geoPointTest() {
        String query = "SELECT ST_POINT(0, 0)";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPGeoPointLiteral(null,
                                new DBSPDoubleLiteral(0), new DBSPDoubleLiteral(0)).some())));
    }

    @Test @Override @Ignore("GEO POINT not yet implemented")
    public void geoDistanceTest() {
        String query = "SELECT ST_DISTANCE(ST_POINT(0, 0), ST_POINT(0,1))";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPDoubleLiteral(1).some())));
    }
}
