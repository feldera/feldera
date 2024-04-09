package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;

import java.util.List;

/** Implements the LAG/LEAD operators for an SQL OVER Window */
public class DBSPLagOperator extends DBSPUnaryOperator {
    public final DBSPComparatorExpression comparator;
    public final DBSPExpression projection;
    public final int offset;

    /**
     * Create a LEAD/LAG window aggregation operator.
     * @param node        Calcite object that is being compiled.
     * @param offset      Lead/lag offset.
     * @param function    Expression that produces the output from two arguments:
     *                    the current row and the delayed row.
     * @param projection  Projection that computes the delayed row from the input row.
     * @param comparator  Comparator used for sorting.
     * @param outputType  Type of output record produced.
     * @param source      Input node for the lag operator.
     */
    public DBSPLagOperator(CalciteObject node, int offset,
                           DBSPExpression function, DBSPExpression projection,
                           DBSPComparatorExpression comparator,
                           DBSPTypeIndexedZSet outputType, DBSPOperator source) {
        super(node, "lag_custom_order", function, outputType, source.isMultiset, source);
        this.comparator = comparator;
        this.projection = projection;
        this.offset = offset;
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 1: "Expected 1 input " + newInputs;
        if (force || this.inputsDiffer(newInputs)) {
            return new DBSPLagOperator(this.getNode(), this.offset,
                    this.getFunction(), this.projection, this.comparator,
                    this.getOutputIndexedZSetType(), newInputs.get(0));
        }
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
