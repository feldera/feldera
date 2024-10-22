package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/** Insert noops between consecutive operators that will introduce integrators in the circuit */
public class SeparateIntegrators extends CircuitCloneVisitor {
    public SeparateIntegrators(IErrorReporter reporter) {
        super(reporter, false);
    }

    public static boolean hasPostIntegrator(DBSPOperator operator) {
        if (operator.is(DBSPAggregateOperator.class))
            return true;
        if (operator.is(DBSPChainAggregateOperator.class))
            return true;
        /*
        if (operator.is(DBSPAggregateLinearPostprocessOperator.class))
            return true;
         */
        if (operator.is(DBSPPartitionedRollingAggregateWithWaterlineOperator.class))
            return true;
        if (operator.is(DBSPPartitionedRollingAggregateOperator.class))
            return true;
        if (operator.is(DBSPStreamAggregateOperator.class))
            return true;
        if (operator.is(DBSPIntegrateOperator.class))
            return true;
        return false;
    }

    /** Should be called with an operator that has an integrator in front of some of the inputs.
     * This will insert noops in front of these inputs if the source operator ends in an integrator. */
    void buffer(DBSPOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        List<DBSPOperator> newSources = new ArrayList<>();
        for (DBSPOperator source: sources) {
            if (hasPostIntegrator(source)) {
                DBSPNoopOperator noop = new DBSPNoopOperator(operator.getNode(), source, null);
                this.addOperator(noop);
                newSources.add(noop);
            } else {
                newSources.add(source);
            }
        }

        DBSPOperator result = operator.withInputs(newSources, this.force);
        result.setDerivedFrom(operator.id);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPWindowOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        // Only need to buffer input 0
        if (hasPostIntegrator(sources.get(0))) {
            DBSPNoopOperator noop = new DBSPNoopOperator(operator.getNode(), sources.get(0), null);
            this.addOperator(noop);
            sources.set(0, noop);
        }

        DBSPOperator result = operator.withInputs(sources, this.force);
        result.setDerivedFrom(operator.id);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator node) {
        this.buffer(node);
    }

    @Override
    public void postorder(DBSPDistinctOperator node) {
        this.buffer(node);
    }

    @Override
    public void postorder(DBSPIntegrateOperator node) {
        this.buffer(node);
    }

    @Override
    public void postorder(DBSPJoinOperator node) {
        this.buffer(node);
    }

    @Override
    public void postorder(DBSPAsofJoinOperator node) {
        this.buffer(node);
    }

    @Override
    public void postorder(DBSPLagOperator node) {
        this.buffer(node);
    }

    @Override
    public void postorder(DBSPIndexedTopKOperator node) {
        this.buffer(node);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator node) {
        this.buffer(node);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator node) {
        this.buffer(node);
    }
}
