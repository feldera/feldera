package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeoPoint;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUuid;
import org.joou.UInteger;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

public class BoundedTypeTests {
    @SuppressWarnings("DataFlowIssue")
    @Test
    public void testLimits() {
        Assert.assertEquals(
                DBSPNullLiteral.INSTANCE,
                DBSPTypeNull.INSTANCE.getMinValue());
        Assert.assertEquals(
                DBSPNullLiteral.INSTANCE,
                DBSPTypeNull.INSTANCE.getMaxValue());
        Assert.assertEquals(
                "00000000-0000-0000-0000-000000000000",
                DBSPTypeUuid.INSTANCE.getMinValue().toString());
        Assert.assertEquals(
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
                DBSPTypeUuid.INSTANCE.getMaxValue().toString());
        Assert.assertEquals(
                BigInteger.valueOf(Integer.MAX_VALUE),
                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false)
                        .getMaxValue()
                        .to(DBSPIntLiteral.class)
                        .getValue());
        Assert.assertEquals(
                BigInteger.valueOf(Integer.MIN_VALUE),
                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false)
                        .getMinValue()
                        .to(DBSPIntLiteral.class)
                        .getValue());
        Assert.assertEquals(
                BigInteger.valueOf(UInteger.MAX_VALUE),
                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.UINT32, false)
                        .getMaxValue()
                        .to(DBSPIntLiteral.class)
                        .getValue());
        Assert.assertEquals(
                BigInteger.valueOf(0),
                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.UINT32, false)
                        .getMinValue()
                        .to(DBSPIntLiteral.class)
                        .getValue());
        Assert.assertEquals(
                Double.MAX_VALUE,
                DBSPTypeDouble.INSTANCE.getMaxValue().to(DBSPDoubleLiteral.class).value,
                0.0);
        Assert.assertEquals(
                Double.MIN_VALUE,
                DBSPTypeDouble.INSTANCE.getMinValue().to(DBSPDoubleLiteral.class).value,
                0.0);
        Assert.assertEquals(
                false,
                DBSPTypeBool.INSTANCE.getMinValue().to(DBSPBoolLiteral.class).value);
        Assert.assertEquals(
                true,
                DBSPTypeBool.INSTANCE.getMaxValue().to(DBSPBoolLiteral.class).value);
        Assert.assertEquals(
                "0001-01-01 00:00:00",
                DBSPTypeTimestamp.INSTANCE.getMinValue().toString());
        Assert.assertEquals(
                "9999-12-31 23:59:59.999",
                DBSPTypeTimestamp.INSTANCE.getMaxValue().toString());
        Assert.assertEquals(
                "00:00:00",
                DBSPTypeTime.INSTANCE.getMinValue().toString());
        Assert.assertEquals(
                "23:59:59.999999999",
                DBSPTypeTime.INSTANCE.getMaxValue().toString());
        Assert.assertEquals(
                new DBSPGeoPointConstructor(
                        CalciteObject.EMPTY, DBSPTypeDouble.INSTANCE.getMinValue(),
                        DBSPTypeDouble.INSTANCE.getMinValue(), DBSPTypeGeoPoint.INSTANCE),
                DBSPTypeGeoPoint.INSTANCE.getMinValue());
        Assert.assertEquals(
                new DBSPGeoPointConstructor(
                        CalciteObject.EMPTY, DBSPTypeDouble.INSTANCE.getMaxValue(),
                        DBSPTypeDouble.INSTANCE.getMaxValue(), DBSPTypeGeoPoint.INSTANCE),
                DBSPTypeGeoPoint.INSTANCE.getMaxValue());
    }
}
