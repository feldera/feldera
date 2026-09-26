package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.IHasInputIntegrator;
import org.dbsp.sqlCompiler.circuit.operator.IHasPostIntegrator;
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

    /** True when a consumer that integrates the output of {@code operator} reads the
     * integrator that {@code operator} keeps.  Such a consumer needs a noop to integrate
     * on its own, so that a Retain operator applies to one integrator only. */
    private static boolean hasPostIntegrator(DBSPSimpleOperator operator) {
        // A source keeps an integrator of its contents only when the table is materialized
        return (operator.is(IHasPostIntegrator.class) &&
                operator.to(IHasPostIntegrator.class).integratorHoldsOutput()) ||
                (operator.is(DBSPSourceMultisetOperator.class) &&
                        operator.to(DBSPSourceMultisetOperator.class).metadata.materialized) ||
                (operator.is(DBSPSourceMapOperator.class) &&
                        operator.to(DBSPSourceMapOperator.class).metadata.materialized);
    }

    /** True when {@code consumer} keeps an integral of the stream it reads on input {@code inputIndex}. */
    private static boolean hasPreIntegrator(DBSPOperator consumer, int inputIndex) {
        // A sink keeps an integrator of the view contents only when the view is materialized
        if (consumer.is(DBSPSinkOperator.class))
            return consumer.to(DBSPSinkOperator.class).metadata.viewKind ==
                    SqlCreateView.ViewKind.MATERIALIZED;
        return consumer.is(IHasInputIntegrator.class) &&
                consumer.to(IHasInputIntegrator.class).hasInputIntegrator(inputIndex);
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
