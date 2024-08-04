package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctIncrementalOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Logger;

import java.util.HashSet;
import java.util.Set;

/** Keeps tabs of the streams that are append-only.
 * A stream is append-only if it is
 * - the output of an append-only table
 * - produced by some stream operators from append-only streams */
public class AppendOnly extends CircuitVisitor {
    final Set<DBSPOperator> appendOnly;

    public AppendOnly(IErrorReporter errorReporter) {
        super(errorReporter);
        this.appendOnly = new HashSet<>();
    }

    public boolean isAppendOnly(DBSPOperator source) {
        return this.appendOnly.contains(source);
    }

    @Override
    public void postorder(DBSPOperator node) {
        // Default behavior
        super.postorder(node);
    }

    void setAppendOnly(DBSPOperator operator) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append(operator.getIdString())
                .append(" ")
                .append(operator.operation)
                .append(" is append-only")
                .newline();
        this.appendOnly.add(operator);
    }

    @Override
    public void postorder(DBSPSourceTableOperator node) {
        super.postorder(node);
        if (node.metadata.isAppendOnly())
            this.setAppendOnly(node);
    }

    @Override
    public void postorder(DBSPConstantOperator node) {
        this.copy(node);
    }

    /** Make operator append-only if all sources are append-only */
    void copy(DBSPOperator operator) {
        super.postorder(operator);
        for (DBSPOperator source: operator.inputs) {
            if (!this.isAppendOnly(source))
                return;
        }
        this.setAppendOnly(operator);
    }

    @Override
    public void postorder(DBSPMapOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPFilterOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPJoinOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPDeindexOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPMapIndexOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPNoopOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPDelayedIntegralOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPIntegrateOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPDelayOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPDistinctOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPSumOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPDistinctIncrementalOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPFlatMapOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPViewOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPSinkOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPHopOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPIndexedTopKOperator node) {
        this.copy(node);
    }
}
