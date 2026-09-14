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
import org.dbsp.sqlCompiler.circuit.operator.DBSPPositiveOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRowNumberOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
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
        super(compiler, graphs);
    }

    private static boolean hasPostIntegrator(DBSPSimpleOperator operator) {
        return operator.is(DBSPAggregateOperator.class) ||
                operator.is(DBSPChainAggregateOperator.class) ||
                operator.is(DBSPAggregateLinearPostprocessOperator.class) ||
                operator.is(DBSPPartitionedRollingAggregateWithWaterlineOperator.class) ||
                operator.is(DBSPPartitionedRollingAggregateOperator.class) ||
                operator.is(DBSPIntegrateOperator.class) ||
                operator.is(DBSPLagOperator.class) ||
                operator.is(DBSPIndexedTopKOperator.class) ||
                operator.is(DBSPRankOperator.class) ||
                operator.is(DBSPRowNumberOperator.class) ||
                (operator.is(DBSPSourceMultisetOperator.class) &&
                        operator.to(DBSPSourceMultisetOperator.class).metadata.materialized) ||
                (operator.is(DBSPSourceMapOperator.class) &&
                        operator.to(DBSPSourceMapOperator.class).metadata.materialized);
    }

    /** True when {@code consumer} keeps an integral of the stream it reads on input {@code inputIndex}. */
    private static boolean hasPreIntegrator(DBSPOperator consumer, int inputIndex) {
        return consumer.is(DBSPJoinBaseOperator.class) ||
                consumer.is(DBSPStarJoinBaseOperator.class) ||
                (consumer.is(DBSPWindowOperator.class) && inputIndex == 0) ||
                consumer.is(DBSPPartitionedRollingAggregateOperator.class) ||
                consumer.is(DBSPDistinctOperator.class) ||
                consumer.is(DBSPPositiveOperator.class) ||
                consumer.is(DBSPAggregateOperator.class) ||
                consumer.is(DBSPIntegrateOperator.class) ||
                consumer.is(DBSPLagOperator.class) ||
                consumer.is(DBSPIndexedTopKOperator.class) ||
                (consumer.is(DBSPSinkOperator.class) &&
                        consumer.to(DBSPSinkOperator.class).metadata.viewKind ==
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
        for (int inputIndex = 0; inputIndex < operator.inputs.size(); inputIndex++) {
            OutputPort input = operator.inputs.get(inputIndex);
            boolean needsOwnTrace = false;
            if (hasPreIntegrator(operator, inputIndex)) {
                if (input.isSimpleNode() && hasPostIntegrator(input.simpleNode())) {
                    needsOwnTrace = true;
                } else {
                    for (Port<DBSPOperator> otherConsumer : this.getGraph().getSuccessors(input.node())) {
                        if (otherConsumer.node() == operator)
                            continue;
                        if (hasPreIntegrator(otherConsumer.node(), otherConsumer.port())) {
                            needsOwnTrace = true;
                            break;
                        }
                    }
                }
            }

            OutputPort source = this.mapped(input);
            if (needsOwnTrace) {
                DBSPNoopOperator noop = new DBSPNoopOperator(operator.getRelNode(), source);
                this.addOperator(noop);
                sources.add(noop.outputPort());
            } else {
                sources.add(source);
            }
        }

        DBSPSimpleOperator result = operator.withInputs(sources, this.force)
                .to(DBSPSimpleOperator.class);
        this.map(operator, result);
    }
}
