package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitPostfix;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTimeAddSub;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPShortIntervalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeShortInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/** Tests for the saturating operations and casts. */
public class SaturatingOperationsTests extends BaseSQLTests {
    static final DBSPTypeDecimal D42 = new DBSPTypeDecimal(CalciteObject.EMPTY, 4, 2, false);
    static final DBSPTypeDecimal D52 = new DBSPTypeDecimal(CalciteObject.EMPTY, 5, 2, false);

    static DBSPExpression decimal(DBSPTypeDecimal type, String value) {
        return new DBSPDecimalLiteral(type, new BigDecimal(value));
    }

    static DBSPExpression microseconds(long microseconds) {
        return DBSPShortIntervalLiteral.fromMicroseconds(
                DBSPTypeShortInterval.Units.DAYS, microseconds, false);
    }

    static DBSPExpression days(long days) {
        return microseconds(days * 86_400_000_000L);
    }

    static DBSPStatement assertEqual(DBSPExpression operation, DBSPExpression expected) {
        return new DBSPApplyExpression("assert_eq!", DBSPTypeVoid.INSTANCE, operation, expected).toStatement();
    }

    /** An assertion that {@code opcode} applied to {@code left} and {@code right} gives
     * {@code expected}. */
    static DBSPStatement check(DBSPOpcode opcode, DBSPExpression left, DBSPExpression right, DBSPExpression expected) {
        return assertEqual(new DBSPBinaryExpression(CalciteObject.EMPTY, expected.getType(), opcode, left, right), expected);
    }

    /** Like {@link #check}, for date, timestamp and interval arithmetic. */
    static DBSPStatement checkTime(DBSPOpcode opcode, DBSPExpression left, DBSPExpression right, DBSPExpression expected) {
        return assertEqual(new DBSPTimeAddSub(CalciteObject.EMPTY, expected.getType(), opcode, left, right), expected);
    }

    /** An assertion that the saturating cast of {@code value} to the type of {@code expected}
     * gives {@code expected}. */
    static DBSPStatement checkCast(DBSPExpression value, DBSPExpression expected) {
        DBSPType type = expected.getType();
        DBSPExpression cast = new DBSPCastExpression(
                CalciteObject.EMPTY, value, type, DBSPCastExpression.CastType.Saturating);
        return assertEqual(cast, expected);
    }

    @Test
    public void saturatingOperations() throws IOException, InterruptedException {
        DBSPTypeDecimal d31 = new DBSPTypeDecimal(CalciteObject.EMPTY, 3, 1, false);
        DBSPTypeDecimal d51 = new DBSPTypeDecimal(CalciteObject.EMPTY, 5, 1, false);
        DBSPTypeDecimal d120 = new DBSPTypeDecimal(CalciteObject.EMPTY, 12, 0, false);

        List<DBSPStatement> checks = new ArrayList<>();
        checks.add(check(DBSPOpcode.ADD_SATURATING,
                new DBSPI32Literal(Integer.MAX_VALUE), new DBSPI32Literal(1), new DBSPI32Literal(Integer.MAX_VALUE)));
        checks.add(check(DBSPOpcode.SUB_SATURATING, new DBSPU32Literal(15), new DBSPU32Literal(20), new DBSPU32Literal(0)));
        checks.add(check(DBSPOpcode.MUL_SATURATING,
                new DBSPI64Literal(Long.MIN_VALUE), new DBSPI64Literal(2), new DBSPI64Literal(Long.MIN_VALUE)));
        checks.add(check(DBSPOpcode.DIV_SATURATING,
                new DBSPI32Literal(Integer.MIN_VALUE), new DBSPI32Literal(-1), new DBSPI32Literal(Integer.MAX_VALUE)));
        checks.add(check(DBSPOpcode.ADD_SATURATING,
                new DBSPDoubleLiteral(1.5), new DBSPDoubleLiteral(2.25), new DBSPDoubleLiteral(3.75)));

        // The result type of a DECIMAL operation determines where it saturates
        checks.add(check(DBSPOpcode.ADD_SATURATING, decimal(D42, "99.99"), decimal(D42, "0.01"), decimal(D52, "100.00")));
        checks.add(check(DBSPOpcode.ADD_SATURATING, decimal(D42, "99.99"), decimal(D42, "0.01"), decimal(D42, "99.99")));
        checks.add(check(DBSPOpcode.SUB_SATURATING, decimal(D42, "-99.99"), decimal(D42, "0.01"), decimal(D42, "-99.99")));
        checks.add(check(DBSPOpcode.MUL_SATURATING, decimal(D42, "-10.00"), decimal(D42, "10.00"), decimal(D42, "-99.99")));
        checks.add(check(DBSPOpcode.DIV_SATURATING, decimal(D42, "1.00"), decimal(D42, "0.01"), decimal(D42, "99.99")));

        checks.add(check(DBSPOpcode.MUL_INTERVAL_SATURATING, days(1), new DBSPI64Literal(2), days(2)));
        checks.add(check(DBSPOpcode.MUL_INTERVAL_SATURATING,
                days(1), new DBSPI64Literal(Long.MAX_VALUE), microseconds(Long.MAX_VALUE)));
        checks.add(check(DBSPOpcode.DIV_INTERVAL_SATURATING, days(1), new DBSPDoubleLiteral(0.5), days(2)));
        checks.add(check(DBSPOpcode.DIV_INTERVAL_SATURATING,
                microseconds(Long.MIN_VALUE), new DBSPDoubleLiteral(0.5), microseconds(Long.MIN_VALUE)));

        // 0001-01-01 is the first legal date, and 9999-12-31 the last
        checks.add(checkTime(DBSPOpcode.SUB_SATURATING,
                new DBSPDateLiteral("0001-01-05"), days(5), new DBSPDateLiteral("0001-01-01")));
        checks.add(checkTime(DBSPOpcode.ADD_SATURATING,
                new DBSPDateLiteral("9999-12-31"), days(1), new DBSPDateLiteral("9999-12-31")));
        checks.add(checkTime(DBSPOpcode.ADD_SATURATING,
                new DBSPTimestampLiteral("9999-12-31 00:00:00", false), days(1),
                new DBSPTimestampLiteral("9999-12-31 23:59:59.999999", false)));
        checks.add(checkTime(DBSPOpcode.ADD_SATURATING, days(1), days(1), days(2)));

        checks.add(checkCast(new DBSPI32Literal(300), new DBSPI8Literal((byte) 127, false)));
        checks.add(checkCast(new DBSPI32Literal(-300), new DBSPI8Literal((byte) -128, false)));
        checks.add(checkCast(new DBSPI32Literal(-3), new DBSPI8Literal((byte) -3, false)));
        checks.add(checkCast(new DBSPDoubleLiteral(1e20), new DBSPI32Literal(Integer.MAX_VALUE)));
        checks.add(checkCast(decimal(d120, "-100000000000"), new DBSPI32Literal(Integer.MIN_VALUE)));
        checks.add(checkCast(new DBSPI32Literal(1000), decimal(d31, "99.9")));
        checks.add(checkCast(decimal(d51, "-1234.5"), decimal(d31, "-99.9")));

        DBSPExpression body = new DBSPBlockExpression(checks, null);
        DBSPFunction tester = new DBSPFunction(CalciteObject.EMPTY, "test", new ArrayList<>(),
                DBSPTypeVoid.INSTANCE, body, Linq.list("#[test]"));
        DBSPCompiler compiler = this.testCompiler();
        PrintStream stream = new PrintStream(BaseSQLTests.TEST_FILE_PATH);
        RustFileWriter writer = new RustFileWriter(new CircuitPostfix(compiler));
        writer.setOutputBuilder(new IndentStream(stream));
        writer.add(tester);
        writer.write(compiler);
        stream.close();
        BaseSQLTests.compileAndTestRust(false);
    }
}
