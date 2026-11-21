package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.util.Maybe;
import org.junit.Assert;
import org.junit.Test;

public class ProjectionCompositionTests {
    private final ExpressionBuilder b = new ExpressionBuilder();
    private final DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());

    @Test public void projectTheResultOfABlockAndKeepItsBindings() {
        var before = b.closure(b.tup(b.i32()), row ->
                b.block(b.call("read_value", b.field(row, 0)), v -> b.tuple(v, b.lit(0))));
        var after = b.closure(before.body.getType(), row -> b.field(row, 0));
        var expected = after.applyAfter(this.compiler, before, Maybe.YES);
        Assert.assertEquals(1, expected.body.to(DBSPBlockExpression.class).contents.size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields")) {
            var actual = RustCircuitPass.composeProjection(this.compiler, before, after);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            Assert.assertSame(before.parameters[0], actual.parameters[0]);
        }
    }

    @Test public void keepAProducerWhenTheProjectionDuplicatesAnExpensiveField() {
        var before = b.closure(b.tup(b.i32()), row -> b.tuple(b.call("read_value", b.field(row, 0))));
        var after = b.closure(before.body.getType(), row -> b.tuple(b.field(row, 0), b.field(row, 0)));
        Assert.assertFalse(after.shouldInlineComposition(this.compiler, before));
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"))
            Assert.assertNull(RustCircuitPass.tryComposeProjection(this.compiler, before, after));
    }
}
