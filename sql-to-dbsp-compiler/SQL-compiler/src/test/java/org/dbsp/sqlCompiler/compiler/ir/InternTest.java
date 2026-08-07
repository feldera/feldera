package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.outer.intern.InternInner;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeInterned;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.junit.Assert;
import org.junit.Test;

/** Test for the dataflow analysis that performs interning */
public class InternTest {
    final ExpressionBuilder b = new ExpressionBuilder();

    @Test
    public void testIntern() {
        DBSPTypeTuple tuple = b.tup(b.i32n(), b.strn(), b.i32n(), b.str());
        DBSPClosureExpression closure = b.closure(tuple, t -> {
            DBSPExpression add = b.add(b.field(t, 0), b.field(t, 1));
            DBSPExpression cmp = b.binary(DBSPOpcode.EQ, b.field(t, 1), b.field(t, 3));
            DBSPExpression concat = b.binary(b.str(), DBSPOpcode.CONCAT,
                    b.field(t, 1), b.lit(" hello"));
            DBSPExpression len = b.call(b.i32n(), "len", concat);
            DBSPExpression cond = b.ifThenElse(cmp.wrapBoolIfNeeded(), add, len);
            return b.tuple(cond, b.field(t, 3));
        });

        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPType parameterType = b.tup(
                b.i32n(), DBSPTypeInterned.INSTANCE, b.i32n(), DBSPTypeInterned.INSTANCE);
        InternInner ii = new InternInner(compiler, true, false, parameterType.ref());
        DBSPExpression converted = ii.apply(closure).to(DBSPExpression.class);
        DBSPType convertedType = converted.getType();
        DBSPType expectedType = new DBSPTypeFunction(
                b.tup(b.i32n(), DBSPTypeInterned.INSTANCE), parameterType.ref());
        Assert.assertTrue(convertedType.sameType(expectedType));
    }
}
