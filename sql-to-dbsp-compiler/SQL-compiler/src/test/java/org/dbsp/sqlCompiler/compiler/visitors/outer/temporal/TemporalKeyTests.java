package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TemporalKeyTests {
    private static TemporalFilter key(String rowName, String localName, String aliasName, boolean readRow) {
        var zero = new DBSPI32Literal(0);
        var row = new DBSPVariablePath(rowName, zero.getType());
        var local = new DBSPLetStatement(localName, row);
        var alias = new DBSPVariablePath(aliasName, zero.getType());
        var bind = new DBSPLetExpression(alias, local.getVarReference(), readRow ? row.deepCopy() : alias);
        var body = new DBSPBlockExpression(List.of(local), bind);
        return new TemporalFilter(row.asParameter(), body, zero, DBSPOpcode.GTE);
    }

    @Test
    public void temporalBoundsCompareLocalDeclarationsInScope() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var first = key("r", "r", "x", false);
        for (boolean readRow : List.of(false, true)) {
            var next = key("row", "local", "alias", readRow);
            assertCompatible(compiler, first, next, !readRow);
        }
    }

    private static void assertCompatible(DBSPCompiler compiler, TemporalFilter first, TemporalFilter next,
                                         boolean expected) {
        Assert.assertEquals(expected, first.compatible(next));
        if (Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys")) {
            var result = RustCircuitPass.acceptsTemporalBound(compiler, List.of("GTE", "GTE"),
                    List.of(first.parameter(), first.noNow(), next.parameter(), next.noNow()));
            Assert.assertEquals(Boolean.valueOf(expected), result);
        }
    }

    private static TemporalFilter bound(DBSPExpression key) {
        var zero = new DBSPI32Literal(0);
        return new TemporalFilter(new DBSPVariablePath("row", zero.getType()).asParameter(),
                key, zero, DBSPOpcode.GTE);
    }

    @Test
    public void collectionWeightsQualifiersAndAggregateOperationsAffectKeyEquality() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var one = new DBSPI32Literal(1);
        var two = new DBSPI32Literal(2);
        var type = one.getType();
        var rows = new DBSPZSetExpression(one, two, one);
        var reordered = new DBSPZSetExpression(two, one, one);
        var key = bound(new DBSPQualifyTypeExpression(rows, type));
        assertCompatible(compiler, key, bound(new DBSPQualifyTypeExpression(reordered, type)), true);
        assertCompatible(compiler, key, bound(new DBSPQualifyTypeExpression(reordered, type.withMayBeNull(true))), false);
        reordered.append(two, -2);
        assertCompatible(compiler, key, bound(new DBSPQualifyTypeExpression(reordered, type)), false);

        var min = bound(new DBSPMinMax(CalciteObject.EMPTY, type, null, DBSPMinMax.Aggregation.Min));
        for (var op : List.of(DBSPMinMax.Aggregation.Min, DBSPMinMax.Aggregation.Max)) {
            var other = bound(new DBSPMinMax(CalciteObject.EMPTY, type, null, op));
            assertCompatible(compiler, min, other, op == DBSPMinMax.Aggregation.Min);
        }
    }
}
