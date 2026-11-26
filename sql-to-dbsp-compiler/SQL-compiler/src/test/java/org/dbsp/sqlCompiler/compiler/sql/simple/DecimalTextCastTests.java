package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class DecimalTextCastTests {
    private static DBSPExpression fold(DBSPCompiler compiler, String text) {
        var type = new DBSPTypeDecimal(CalciteObject.EMPTY, 5, 2, false);
        var cast = new DBSPCastExpression(CalciteObject.EMPTY, new DBSPStringLiteral(text),
                type, DBSPCastExpression.CastType.SqlUnsafe);
        return new Simplify(compiler).apply(cast).to(DBSPExpression.class);
    }

    private static void checkWarning(String text, String kind) {
        var compiler = DBSPCompiler.forCircuitReplay(new CompilerOptions());
        Assert.assertTrue(text, fold(compiler, text).is(DBSPCastExpression.class));
        Assert.assertEquals(text, 1, compiler.messages.messages.size());
        var warning = compiler.messages.messages.get(0);
        Assert.assertTrue(text, warning.warning);
        Assert.assertEquals(text, kind, warning.errorType);
        String message = kind.equals("Not a number")
                ? " String '" + text + "' cannot be interpreted as a DECIMAL"
                : "cannot represent '" + text + "' as DECIMAL(5, 2): precision of DECIMAL type too small to represent value";
        Assert.assertEquals(message, warning.message);
    }

    @Test
    public void decimalOverflowKeepsCast() {
        checkWarning("1e1000000000", "Not a number");
        checkWarning("1e-1000000000", "Not a number");
        checkWarning("0e99999999999", "Not a number");
        checkWarning("1e-2147483648", "Not a number");
        checkWarning("1e-646456995", "Not a number");
        checkWarning("8.8080652584198167660e646456990", "Invalid DECIMAL");
        checkWarning("8.8080652584198167661e646456990", "Not a number");
        var compiler = DBSPCompiler.forCircuitReplay(new CompilerOptions());
        var zero = fold(compiler, "1e-646456994").to(DBSPDecimalLiteral.class);
        Assert.assertEquals(new BigDecimal("0.00"), zero.value);
        Assert.assertEquals(0, compiler.messages.warningCount());
    }
}
