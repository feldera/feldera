package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

/**
 * The DBSPWindow operator corresponds to a DBSP window() call.
 * The left input is a stream of IndexedZSets, while the
 * right input is a stream of scalar pairs.  The keys of
 * elements in the left input are compared with the two scalars
 * in the pair; when they fall between the two limits,
 * they are emitted to the output ZSet. */
public final class DBSPWindowOperator extends DBSPBinaryOperator {
    public final boolean lowerInclusive;
    public final boolean upperInclusive;

    public DBSPWindowOperator(
            CalciteObject node, boolean lowerInclusive, boolean upperInclusive,
            DBSPOperator data, DBSPOperator control) {
        super(node, "window", null, data.getType(), data.isMultiset,
                data, control);
        // Check that the left input and output are indexed ZSets
        this.getOutputIndexedZSetType();
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return this;
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2: "Expected 2 inputs, got " + newInputs.size();
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWindowOperator(
                    this.getNode(), this.lowerInclusive, this.upperInclusive,
                    newInputs.get(0), newInputs.get(1)).copyAnnotations(this);
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
