package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;

/** Implements the table function HOP.  This is desugared into a pair of
 * operators: map (computing the hop start window) followed by flat_map
 * (which generates all the windows).  It does not correspond to any DBSP
 * Rust operator. */
public final class DBSPHopOperator extends DBSPUnaryOperator {
    public final int timestampIndex;
    public final DBSPExpression interval;
    public final DBSPExpression start;
    public final DBSPExpression size;

    public DBSPHopOperator(CalciteObject node, int timestampIndex,
                           DBSPExpression interval,
                           DBSPExpression start, DBSPExpression size,
                           DBSPTypeZSet outputType, DBSPOperator input) {
        super(node, "hop", null, outputType, input.isMultiset, input);
        this.timestampIndex = timestampIndex;
        this.interval = interval;
        this.start = start;
        this.size = size;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPHopOperator(
                this.getNode(), this.timestampIndex, this.interval, this.start, this.size,
                outputType.to(DBSPTypeZSet.class), this.input());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPHopOperator(
                    this.getNode(), this.timestampIndex, this.interval, this.start, this.size,
                    this.getOutputZSetType(), newInputs.get(0));
        return this;
    }
}
