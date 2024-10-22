package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;

import java.util.ArrayList;
import java.util.List;

/** Insert noops between
 * - operators that may introduce consecutive integrators in the circuit
 * - before operators that share a source and have an integrator in front.
 * This will make the scope of Retain{Keys,Values} operators clear later. */
public class SeparateIntegrators extends CircuitCloneVisitor {
    final CircuitGraph graph;

    public SeparateIntegrators(IErrorReporter reporter, CircuitGraph graph) {
        super(reporter, false);
        this.graph = graph;
    }

    public static boolean hasPostIntegrator(DBSPOperator operator) {
        return operator.is(DBSPAggregateOperator.class) ||
                operator.is(DBSPChainAggregateOperator.class) ||
                operator.is(DBSPAggregateLinearPostprocessOperator.class) ||
                operator.is(DBSPPartitionedRollingAggregateWithWaterlineOperator.class) ||
                operator.is(DBSPPartitionedRollingAggregateOperator.class) ||
                operator.is(DBSPIntegrateOperator.class) ||
                operator.is(DBSPLagOperator.class) ||
                operator.is(DBSPIndexedTopKOperator.class) ||
                (operator.is(DBSPSourceMultisetOperator.class) &&
                        operator.to(DBSPSourceMultisetOperator.class).metadata.materialized) ||
                (operator.is(DBSPSourceMapOperator.class) &&
                        operator.to(DBSPSourceMapOperator.class).metadata.materialized);
    }

    public static boolean hasPreIntegrator(CircuitGraph.Port port) {
        DBSPOperator operator = port.operator();
        int input = port.input();
        return operator.is(DBSPJoinBaseOperator.class) ||
                (operator.is(DBSPWindowOperator.class) && input == 0) ||
                operator.is(DBSPPartitionedRollingAggregateOperator.class) ||
                operator.is(DBSPDistinctOperator.class) ||
                operator.is(DBSPAggregateOperator.class) ||
                operator.is(DBSPIntegrateOperator.class) ||
                operator.is(DBSPLagOperator.class) ||
                operator.is(DBSPIndexedTopKOperator.class) ||
                (operator.is(DBSPSinkOperator.class) &&
                        operator.to(DBSPSinkOperator.class).metadata.viewKind ==
                                SqlCreateView.ViewKind.MATERIALIZED);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        throw new InternalCompilerError("StreamAggregate operator should have been removed " + operator);
    }

    @Override
    public void replace(DBSPOperator operator) {
        List<DBSPOperator> sources = new ArrayList<>(operator.inputs.size());
        int index = 0;
        for (DBSPOperator input: operator.inputs) {
            CircuitGraph.Port port = new CircuitGraph.Port(operator, index++);
            boolean addBuffer = false;
            if (hasPreIntegrator(port)) {
                if (hasPostIntegrator(input)) {
                    addBuffer = true;
                } else {
                    for (CircuitGraph.Port dest : this.graph.getDestinations(input)) {
                        if (dest.operator() == operator)
                            continue;
                        if (hasPreIntegrator(dest)) {
                            addBuffer = true;
                            break;
                        }
                    }
                }
            }

            DBSPOperator source = this.mapped(input);
            if (addBuffer) {
                DBSPNoopOperator noop = new DBSPNoopOperator(operator.getNode(), source, null);
                this.addOperator(noop);
                sources.add(noop);
            } else {
                sources.add(source);
            }
        }

        DBSPOperator result = operator.withInputs(sources, this.force);
        result.setDerivedFrom(operator.id);
        this.map(operator, result);
    }
}
