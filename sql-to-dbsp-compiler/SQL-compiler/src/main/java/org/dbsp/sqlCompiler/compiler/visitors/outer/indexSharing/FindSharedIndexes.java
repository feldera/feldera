package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Maybe;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Finds multiple {@link DBSPMapIndexOperator}s that can be combined into a single wide one */
class FindSharedIndexes extends CircuitWithGraphsVisitor {
    // List of operators that can be combined.
    // Two MapIndex operators can be combined if they
    // - have the same source,
    // - have the same key
    // - have the same value nullability
    // - are followed by a stateful operator
    final List<List<MapIndexAndConsumer>> clusters;
    final Set<DBSPMapIndexOperator> visited;

    boolean sameKey(DBSPClosureExpression first, DBSPClosureExpression second) {
        DBSPVariablePath varLeft = first.getResultType().ref().var();
        DBSPClosureExpression projectLeft = varLeft.deref().field(0).closure(varLeft);

        DBSPVariablePath varRight = second.getResultType().ref().var();
        DBSPClosureExpression projectRight = varRight.deref().field(0).closure(varRight);

        DBSPClosureExpression firstKey = projectLeft.applyAfter(this.compiler, first, Maybe.YES);
        DBSPClosureExpression secondKey = projectRight.applyAfter(this.compiler, second, Maybe.YES);
        return firstKey.equivalent(secondKey);
    }

    public FindSharedIndexes(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs);
        this.clusters = new ArrayList<>();
        this.visited = new HashSet<>();
    }

    /** Find if the node has a single successor which requires an integrator on this input */
    @Nullable
    static MapIndexAndConsumer followedByIntegrator(CircuitGraph graph, DBSPMapIndexOperator operator) {
        List<Port<DBSPOperator>> successors = graph.getSuccessors(operator);
        if (successors.size() > 1)
            return null;
        // Currently only handle joins
        DBSPOperator successor = successors.get(0).node();
        if (successor.is(DBSPJoinBaseOperator.class) &&
                !successor.is(DBSPAsofJoinOperator.class) &&
                !successor.is(DBSPConcreteAsofJoinOperator.class)) {
            var join = successor.to(DBSPJoinBaseOperator.class);
            boolean isLeftInput = join.left().operator == operator;
            return new MapIndexAndConsumer(operator, join, isLeftInput);
        }
        return null;
    }

    // Return non-null if the operator has the requisite shape
    @Nullable
    MapIndexAndConsumer mayBeCombined(CircuitGraph graph, DBSPMapIndexOperator operator) {
        if (this.visited.contains(operator))
            return null;
        MapIndexAndConsumer pair = followedByIntegrator(graph, operator);
        if (pair == null)
            return pair;
        Projection projection = new Projection(this.compiler, true, false);
        projection.apply(operator.getClosureFunction());
        if (!projection.isProjection) return null;
        return pair;
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        CircuitGraph graph = this.getGraph();
        OutputPort input = operator.input();
        List<Port<DBSPOperator>> siblings = graph.getSuccessors(input.node());
        if (siblings.size() < 2) return;

        MapIndexAndConsumer pair = this.mayBeCombined(graph, operator);
        if (pair == null) return;
        boolean nullableValue = operator.getOutputIndexedZSetType().elementType.mayBeNull;

        List<MapIndexAndConsumer> matching = new ArrayList<>();
        matching.add(pair);
        this.visited.add(operator);

        // Scan all siblings to see which ones match
        for (Port<DBSPOperator> port : siblings) {
            DBSPOperator sibling = port.node();
            if (!sibling.is(DBSPMapIndexOperator.class))
                continue;
            if (sibling == operator)
                continue;
            DBSPMapIndexOperator smi = sibling.to(DBSPMapIndexOperator.class);
            MapIndexAndConsumer nextPair = this.mayBeCombined(graph, smi);
            if (nextPair == null)
                continue;

            boolean nullableSmiValue = smi.getOutputIndexedZSetType().elementType.mayBeNull;
            if (nullableSmiValue != nullableValue)
                continue;

            if (!this.sameKey(operator.getClosureFunction(), smi.getClosureFunction()))
                continue;
            this.visited.add(nextPair.index());
            matching.add(nextPair);
        }

        if (matching.size() > 1)
            this.clusters.add(matching);
    }

    record MapIndexAndConsumer(DBSPMapIndexOperator index, DBSPJoinBaseOperator consumer, boolean leftInput) {
        @Override
        public String toString() {
            return "IxJ[" + this.index + ", " + this.consumer + (this.leftInput ? "L" : "R") + "]";
        }
    }
}
