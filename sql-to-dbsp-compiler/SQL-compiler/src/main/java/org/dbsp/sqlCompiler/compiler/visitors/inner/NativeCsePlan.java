package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Read and insertion sites in one normalized expression tree. The first read identifies the cell. */
public record NativeCsePlan(Map<Long, Read> reads, Map<Long, Map<Long, List<Long>>> insertions) {
    public record Read(long first, boolean borrowed) {}

    private record Cell(DBSPVariablePath var, DBSPExpression expression) implements RustCircuitPass.LazyBinding {}

    /** Bind the plan to live translated initializers in visitor order. */
    static final class Bindings {
        final DBSPCompiler compiler;
        final NativeCsePlan plan;
        final Map<Long, Cell> cells = new HashMap<>();

        Bindings(DBSPCompiler compiler, NativeCsePlan plan) {
            this.compiler = compiler;
            this.plan = plan;
        }

        DBSPExpression read(DBSPExpression expression, DBSPExpression initializer) {
            Read read = this.plan.reads.get(expression.id);
            if (read == null)
                return initializer;
            boolean first = read.first == expression.id;
            var variable = first ? null : Utilities.getExists(this.cells, read.first).var;
            var replacement = RustCircuitPass.readLazyValue(this.compiler, expression.getType(), variable, read.borrowed);
            if (first)
                Utilities.putNew(this.cells, expression.id, new Cell(replacement.variable(), initializer));
            return replacement.value();
        }

        Map<Long, List<RustCircuitPass.LazyBinding>> insertions(DBSPExpression scope) {
            var anchors = this.plan.insertions.get(scope.id);
            if (anchors == null)
                return Map.of();
            var result = new HashMap<Long, List<RustCircuitPass.LazyBinding>>();
            anchors.forEach((anchor, ids) -> result.put(anchor,
                    ids.stream().<RustCircuitPass.LazyBinding>map(id -> Utilities.getExists(this.cells, id)).toList()));
            return result;
        }
    }
}
