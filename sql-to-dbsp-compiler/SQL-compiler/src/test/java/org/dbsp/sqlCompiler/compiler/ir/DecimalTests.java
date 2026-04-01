package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class DecimalTests {
    @Test
    public void testDecimalBounds() {
        for (int scale = 0; scale < 5; scale += 1) {
            DBSPTypeDecimal dec = new DBSPTypeDecimal(CalciteObject.EMPTY, 4, scale, false);
            Assert.assertEquals(
                    new DBSPDecimalLiteral(CalciteObject.EMPTY, dec, BigDecimal.valueOf(9999, scale)),
                    dec.getMaxValue());
            Assert.assertEquals(
                    new DBSPDecimalLiteral(CalciteObject.EMPTY, dec, BigDecimal.valueOf(9999, scale).negate()),
                    dec.getMinValue());
        }
    }
}
