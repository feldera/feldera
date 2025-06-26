package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.intern.InternInner;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeInterned;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.junit.Assert;
import org.junit.Test;

/** Test for the dataflow analysis that performs interning */
public class InternTest {
    @Test
    public void testIntern() {
        DBSPType i = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true);
        DBSPTypeString nullableStr = DBSPTypeString.varchar(true);
        DBSPTypeString str = DBSPTypeString.varchar(false);
        DBSPTypeTuple tuple = new DBSPTypeTuple(i, nullableStr, i, str);
        DBSPVariablePath var = tuple.ref().var();
        DBSPExpression add = new DBSPBinaryExpression(CalciteObject.EMPTY, i, DBSPOpcode.ADD,
                var.deref().field(0), var.deref().field(1));
        DBSPExpression cmp = new DBSPBinaryExpression(CalciteObject.EMPTY, DBSPTypeBool.create(true),
                DBSPOpcode.EQ, var.deref().field(1), var.deref().field(3));
        DBSPExpression concat = new DBSPBinaryExpression(CalciteObject.EMPTY, str, DBSPOpcode.CONCAT,
                var.deref().field(1), new DBSPStringLiteral(" hello"));
        DBSPApplyExpression len = new DBSPApplyExpression("len", i, concat);
        DBSPExpression cond = new DBSPIfExpression(CalciteObject.EMPTY,
                cmp.wrapBoolIfNeeded(), add, len);
        DBSPExpression result = new DBSPTupleExpression(cond, var.deref().field(3));
        DBSPClosureExpression closure = result.closure(var);

        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPType parameterType = new DBSPTypeTuple(i, DBSPTypeInterned.INSTANCE, i, DBSPTypeInterned.INSTANCE);
        InternInner ii = new InternInner(compiler, true, false, parameterType.ref());
        DBSPExpression converted = ii.apply(closure).to(DBSPExpression.class);
        DBSPType convertedType = converted.getType();
        DBSPType expectedType = new DBSPTypeFunction(
                new DBSPTypeTuple(cond.getType(), DBSPTypeInterned.INSTANCE), parameterType.ref());
        Assert.assertTrue(convertedType.sameType(expectedType));
    }
}
