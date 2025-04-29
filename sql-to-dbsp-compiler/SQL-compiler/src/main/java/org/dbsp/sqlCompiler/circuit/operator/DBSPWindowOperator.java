package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

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
            CalciteRelNode node, boolean lowerInclusive, boolean upperInclusive,
            OutputPort data, OutputPort control) {
        super(node, "window", null, data.outputType(), data.isMultiset(),
                data, control, false);
        // Check that the left input and output are indexed ZSets
        this.getOutputIndexedZSetType();
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return this;
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        Utilities.enforce(newInputs.size() == 2, "Expected 2 inputs, got " + newInputs.size());
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWindowOperator(
                    this.getRelNode(), this.lowerInclusive, this.upperInclusive,
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

    @SuppressWarnings("unused")
    public static DBSPWindowOperator fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPSimpleOperator.CommonInfo info = commonInfoFromJson(node, decoder);
        boolean lowerInclusive = Utilities.getBooleanProperty(node, "lowerInclusive");
        boolean upperInclusive = Utilities.getBooleanProperty(node, "upperInclusive");
        return new DBSPWindowOperator(CalciteEmptyRel.INSTANCE,
                lowerInclusive, upperInclusive, info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPWindowOperator.class);
    }
}
