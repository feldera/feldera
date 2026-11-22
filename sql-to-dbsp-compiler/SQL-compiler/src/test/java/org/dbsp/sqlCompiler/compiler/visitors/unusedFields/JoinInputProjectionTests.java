package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;

public class JoinInputProjectionTests {
    @Test
    public void keepsCompositeKeysAndDistinguishesNullFromAnEmptyValue() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteObject.EMPTY;
        var number = new DBSPTypeInteger(site, 64, true, false);
        var text = DBSPTypeString.varchar(false);
        var key = new DBSPTypeTuple(number, text);
        for (boolean nullable : List.of(false, true)) {
            var row = new DBSPTypeTuple(number, number, text).withMayBeNull(nullable);
            var pair = new DBSPTypeRawTuple(key.ref(), row.ref());
            for (var columns : List.of(List.<Integer>of(), List.of(0, 2))) {
                long start = DBSPVariablePath.nextGeneratedName();
                var expected = RemoveUnusedFields.joinInputProjection(site, pair, columns);
                long end = DBSPVariablePath.nextGeneratedName();
                DBSPVariablePath.restoreGeneratedNames(start);
                var actual = RustCircuitPass.projectJoinInput(compiler, site, pair, columns);
                Assert.assertEquals(end, DBSPVariablePath.nextGeneratedName());
                Assert.assertTrue(actual.getResultType().sameType(expected.getResultType()));
                Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            }
        }
    }
}
