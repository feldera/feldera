package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindCommonProjections;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ReplaceCommonProjections;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.Map;

/** Materialize Rust-selected projections with the closures computed by the preceding analysis. */
final class LiveSharedProjections extends CircuitCloneVisitor {
    enum ProjectionMap { Map, MapIndex }
    record Edits(@Nullable ProjectionMap readerIfNarrowed, @Nullable ProjectionMap afterProducer) {}

    private final FindCommonProjections analysis;
    private final Map<Long, Edits> edits;

    private LiveSharedProjections(DBSPCompiler compiler, FindCommonProjections analysis, Map<Long, Edits> edits) {
        super(compiler, false);
        this.analysis = analysis;
        this.edits = edits;
        this.preservesTypes = false;
    }

    static DBSPCircuit apply(DBSPCompiler compiler, DBSPCircuit circuit,
                             FindCommonProjections analysis, Map<Long, Edits> edits) {
        return new LiveSharedProjections(compiler, analysis, edits).apply(circuit);
    }

    @Override
    public void replace(DBSPSimpleOperator operator) {
        if (this.visited.contains(operator))
            return;
        var edit = this.edits.get(operator.id);
        if (edit != null && edit.readerIfNarrowed() != null) {
            var original = operator.inputs.get(0);
            var source = this.mapped(original);
            if (!source.outputType().sameType(original.outputType())) {
                this.visited.add(operator);
                var closure = this.analysis.inputProjection.get(operator);
                this.map(operator, ReplaceCommonProjections.project(edit.readerIfNarrowed() == ProjectionMap.MapIndex, operator.getRelNode(), closure,
                        source, source.outputType().getToplevelFieldCount()));
                return;
            }
        }
        if (edit == null || edit.afterProducer() == null) {
            super.replace(operator);
            return;
        }
        this.visited.add(operator);
        var producer = operator.withInputs(Linq.map(operator.inputs, this::mapped), false).to(DBSPSimpleOperator.class);
        this.addOperator(producer);
        var closure = this.analysis.outputProjection.get(operator);
        this.map(operator, ReplaceCommonProjections.project(edit.afterProducer() == ProjectionMap.MapIndex, CalciteEmptyRel.INSTANCE, closure,
                producer.outputPort(), operator.outputType().getToplevelFieldCount()));
    }
}
