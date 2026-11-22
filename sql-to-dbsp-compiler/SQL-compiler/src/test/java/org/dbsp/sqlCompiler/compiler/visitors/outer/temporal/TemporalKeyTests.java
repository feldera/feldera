package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
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
            boolean compatible = first.compatible(next);
            Assert.assertEquals(!readRow, compatible);
            if (Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys")) {
                var result = RustCircuitPass.acceptsTemporalBound(compiler, List.of("GTE", "GTE"),
                        List.of(first.parameter(), first.noNow(), next.parameter(), next.noNow()));
                Assert.assertEquals(Boolean.valueOf(compatible), result);
            }
        }
    }
}
