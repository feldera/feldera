package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;
import java.util.Objects;

/** A DBSP trace can have at most one key-retention policy and one 
 * value-retention policy. */
public class CheckRetain extends CircuitWithGraphsVisitor {
    /** Retention slot of a trace that a GC operator writes */
    enum RetentionKind {
        Key,
        Value
    }

    public CheckRetain(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs);
    }

    /** RetentionKind of this operator, or null. */
    @Nullable
    static RetentionKind slot(DBSPOperator operator) {
        if (operator.is(DBSPIntegrateTraceRetainKeysOperator.class))
            return RetentionKind.Key;
        if (operator.is(DBSPIntegrateTraceRetainValuesOperator.class) ||
                operator.is(DBSPIntegrateTraceRetainNValuesOperator.class))
            return RetentionKind.Value;
        return null;
    }

    void checkUnique(DBSPBinaryOperator retain) {
        RetentionKind slot = Objects.requireNonNull(slot(retain));
        OutputPort data = retain.left();
        for (Port<DBSPOperator> destination: this.getGraph().getSuccessors(data.node())) {
            DBSPOperator other = destination.node();
            if (other == retain || slot(other) != slot)
                continue;
            // A GC operator reads the data it retains on input 0 and the bounds on input 1
            if (!other.inputs.get(0).equals(data))
                continue;
            throw new InternalCompilerError("Operator " + data + " has two " + slot +
                    " retention policies: " + retain + " and " + other);
        }
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator retain) {
        this.checkUnique(retain);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainValuesOperator retain) {
        this.checkUnique(retain);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainNValuesOperator retain) {
        this.checkUnique(retain);
    }
}
