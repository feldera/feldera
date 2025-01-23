package org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields;

import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.util.graph.Port;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Find operators that have multiple outputs that are all projections.
 * Compute for each such operator a projection that is the "largest", and use it
 * as a root of a tree of projections.
 */
public class FindCommonProjections extends CircuitVisitor {
    final CircuitGraphs graphs;
    final Map<DBSPOperator, FieldUseMap> largestProjection;
    final Projection projection;

    protected CircuitGraph getGraph() {
        return this.graphs.getGraph(this.getParent());
    }

    public FindCommonProjections(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler);
        this.graphs = graphs;
        this.largestProjection = new HashMap<>();
        this.projection = new Projection(this.compiler, true);
    }

    @Override
    public void postorder(DBSPOperator operator) {
        List<Port<DBSPOperator>> successors = this.getGraph().getSuccessors(operator);
        boolean skip = false;
        for (Port<DBSPOperator> p: successors) {
            if (!p.node().hasAnnotation(a -> a.is(IsProjection.class))) {
                skip = true;
                break;
            }
        }
        if (skip) {
            super.postorder(operator);
            return;
        }
    }
}
