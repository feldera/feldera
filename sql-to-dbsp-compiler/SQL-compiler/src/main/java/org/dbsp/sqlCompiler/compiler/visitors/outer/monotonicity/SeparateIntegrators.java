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
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.util.graph.Port;

import java.util.ArrayList;
import java.util.List;

/** Insert noops between
 * - operators that may introduce consecutive integrators in the circuit
 * - before operators that share a source and have an integrator in front.
 * This will make the scope of Retain{Keys,Values} operators clear later.
 * This is only invoked in incremental compilation mode. */
public class SeparateIntegrators extends CircuitCloneWithGraphsVisitor {
    public SeparateIntegrators(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    public static boolean hasPostIntegrator(DBSPSimpleOperator operator) {
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

    public static boolean hasPreIntegrator(OutputPort port) {
        DBSPOperator operator = port.node();
        int input = port.port();
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
        // In incremental compilation mode this should have been eliminated
        throw new InternalCompilerError("StreamAggregate operator should have been removed " + operator);
    }

    @Override
    public void replace(DBSPSimpleOperator operator) {
        List<OutputPort> sources = new ArrayList<>(operator.inputs.size());
        int index = 0;
        for (OutputPort input: operator.inputs) {
            OutputPort port = new OutputPort(operator, index++);
            boolean addBuffer = false;
            if (hasPreIntegrator(port)) {
                if (input.isSimpleNode() && hasPostIntegrator(input.simpleNode())) {
                    addBuffer = true;
                } else {
                    for (Port<DBSPOperator> dest : this.getGraph().getSuccessors(input.node())) {
                        if (dest.node() == operator)
                            continue;
                        if (hasPreIntegrator(new OutputPort(dest))) {
                            addBuffer = true;
                            break;
                        }
                    }
                }
            }

            OutputPort source = this.mapped(input);
            if (addBuffer) {
                DBSPNoopOperator noop = new DBSPNoopOperator(operator.getRelNode(), source, null);
                this.addOperator(noop);
                sources.add(noop.outputPort());
            } else {
                sources.add(source);
            }
        }

        DBSPSimpleOperator result = operator.withInputs(sources, this.force);
        result.setDerivedFrom(operator.id);
        this.map(operator, result);
    }
}
