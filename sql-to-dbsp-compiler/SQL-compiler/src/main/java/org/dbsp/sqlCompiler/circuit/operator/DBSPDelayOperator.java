package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

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
 * <p></p>If the function is specified, it is the initial value produced by the delay.
 */
public final class DBSPDelayOperator extends DBSPUnaryOperator {
    /** This can be null for operators that do not create back-edges. */
    @Nullable
    public final DBSPDelayOutputOperator output;

    public DBSPDelayOperator(CalciteObject node, @Nullable DBSPExpression initial,
                             DBSPOperator source, @Nullable DBSPDelayOutputOperator output) {
        super(node, initial == null ? "delay" : "delay_with_initial_value",
                initial, source.outputType, source.isMultiset, source);
        this.output = output;
    }

    public DBSPDelayOperator(CalciteObject node, @Nullable DBSPExpression initial, DBSPOperator source) {
        this(node, initial, source, null);
    }

    public DBSPDelayOperator(CalciteObject node, DBSPOperator source, DBSPDelayOutputOperator output) {
        this(node, null, source, output);
    }

    public DBSPDelayOperator(CalciteObject node, DBSPOperator source) {
        this(node, null, source, null);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPDelayOperator(this.getNode(), this.function, newInputs.get(0), this.output);
        return this;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression function, DBSPType unusedOutputType) {
        return new DBSPDelayOperator(this.getNode(), function, this.input(), this.output);
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
