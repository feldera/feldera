package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StaticCandidatesTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    static ImplementStatics pass() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.optimizer.compareRustStatics");
        return new ImplementStatics(compiler, true);
    }

    @Test
    public void distinguishConstructedValuesFromCallsWithConstantArguments() {
        var parameter = b.i32().var();
        var call = b.call("f", b.lit(1));
        var constructed = new DBSPArrayExpression(b.lit(2));
        var computed = new DBSPArrayExpression(call);
        var captures = b.call("g", b.lambda(b.i32(), inner -> b.add(inner, parameter)));
        var closed = b.call("g", b.lambda(b.i32(), inner -> b.add(inner, b.lit(3))));
        var pass = pass();
        pass.preorder(b.tuple(call, constructed, computed, captures, closed).closure(parameter));
        Assert.assertTrue(pass.shouldBeStatic(call));
        Assert.assertTrue(pass.shouldBeStatic(constructed));
        Assert.assertFalse(pass.shouldBeStatic(computed));
        Assert.assertFalse(pass.shouldBeStatic(captures));
        Assert.assertTrue(pass.shouldBeStatic(closed));
    }

    @Test
    public void staticErrorsCountRepeatedUsesAcrossInitializers() {
        var origin = CalciteObject.create(new SourcePositionRange(new SourcePosition(2, 1), new SourcePosition(2, 2)));
        var value = new DBSPI32Literal(origin, b.i32(), 1);
        var behavior = DBSPHandleErrorExpression.RuntimeBehavior.PanicWithSource;
        var shared = new DBSPHandleErrorExpression(origin, 19, behavior, value, true);
        var silent = new DBSPHandleErrorExpression(origin, 19, behavior, value, false);
        var last = new DBSPHandleErrorExpression(origin, 19, behavior, value, true);
        var binding = b.let(shared, variable -> variable);
        var pass = pass().renumber;
        pass.index = 7;

        var tuple = b.tuple(binding, binding, shared, shared, silent, last);
        var rewritten = pass.renumber(tuple).to(DBSPTupleExpression.class);
        Assert.assertSame(rewritten.fields[0], rewritten.fields[1]);
        Assert.assertSame(rewritten.fields[2], rewritten.fields[3]);
        Assert.assertEquals(7, rewritten.fields[2].to(DBSPHandleErrorExpression.class).index);
        Assert.assertEquals(0, rewritten.fields[4].to(DBSPHandleErrorExpression.class).index);
        Assert.assertEquals(11, rewritten.fields[5].to(DBSPHandleErrorExpression.class).index);
        Assert.assertEquals(12, pass.renumber(shared).to(DBSPHandleErrorExpression.class).index);
        Assert.assertEquals(13, pass.index);
    }

    @Test
    public void firstUntypedArrayKeepsItsSourcePosition() {
        var span = new SourcePositionRange(new SourcePosition(3, 2), new SourcePosition(3, 7));
        var origin = CalciteObject.create(span);
        var ty = new DBSPTypeArray(DBSPTypeAny.INSTANCE, false);
        var first = new DBSPArrayExpression(origin, ty, List.of());
        var second = new DBSPArrayExpression(ty, false);
        var closure = b.tuple(first, second).closure(b.i32().var());
        var error = Assert.assertThrows(CompilationError.class, () -> pass().apply(closure));
        Assert.assertSame(origin, error.calciteObject);
        Assert.assertEquals("Could not infer a type for array elements; please specify it using CAST(array AS X ARRAY)",
                error.getMessage());
    }
}
