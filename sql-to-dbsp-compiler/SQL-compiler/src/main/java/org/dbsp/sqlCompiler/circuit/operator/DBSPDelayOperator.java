package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;

import javax.annotation.Nullable;
import java.util.List;

/**
 * The z^-1 operator from DBSP.
 * If the delay is used for a cycle then the operator points
 * to a DelayOutput operator which closes the loop.
 * In this way a circuit like
 *            +---------+
 *    ------->|         |
 *     ------>|         |------>
 *     |      +---------+  |
 *     |                   |
 *     |      +---------+  |
 *     -------|    Z    |<--
 *            |         |
 *            +---------+
 * is represented instead like:
 *            +---------+
 *    ------->|         |
 *    ------->|         |------>
 *    |       +---------+  |
 *  +---+                  |    +---------+
 *  |ZO |                  ---->|    Z    |
 *  |   |<......................|         |
 *  +---+          back edge    +---------+
 * where ZO is the DelayOutput operator, and the back-edge is not an explicit operator input.
 *
 * <P>Currently Delays cannot appear in a generated circuit, they are only synthesized
 * for some compiler analyses.  That's why they are marked as NonCoreIR.
 */
@NonCoreIR
public class DBSPDelayOperator extends DBSPUnaryOperator {
    /**
     * This can be null for operators that do not create back-edges.
     */
    @Nullable
    public final DBSPDelayOutputOperator output;

    public DBSPDelayOperator(CalciteObject node, DBSPOperator source) {
        super(node, "Delay", null, source.outputType, source.isMultiset, source);
        this.output = null;
    }

    public DBSPDelayOperator(CalciteObject node, DBSPOperator source, DBSPDelayOutputOperator output) {
        super(node, "Delay", null, source.outputType, source.isMultiset, source);
        this.output = output;
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPDelayOperator(this.getNode(), newInputs.get(0));
        return this;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }
}
