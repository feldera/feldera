package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
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
