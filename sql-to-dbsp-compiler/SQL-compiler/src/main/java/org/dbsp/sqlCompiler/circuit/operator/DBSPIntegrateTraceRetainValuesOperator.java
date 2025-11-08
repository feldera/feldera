package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Currently always inserted after the input of a join operator */
public final class DBSPIntegrateTraceRetainValuesOperator
        extends DBSPBinaryOperator implements GCOperator {

    public final boolean accumulate;

    public DBSPIntegrateTraceRetainValuesOperator(
            CalciteRelNode node, DBSPExpression function,
            OutputPort data, OutputPort control, boolean accumulate) {
        super(node, accumulate ? "accumulate_integrate_trace_retain_values" : "integrate_trace_retain_values",
                function, data.outputType(), data.isMultiset(), data, control, false);
        this.accumulate = accumulate;
    }

    public static DBSPIntegrateTraceRetainValuesOperator create(
            CalciteRelNode node, OutputPort data, IMaybeMonotoneType dataProjection, OutputPort control, boolean accumulate) {
        DBSPType controlType = control.outputType();
        Utilities.enforce(controlType.is(DBSPTypeTupleBase.class),
                () -> "Control type is not a tuple: " + controlType);
        DBSPTypeTupleBase controlTuple = controlType.to(DBSPTypeTupleBase.class);
        Utilities.enforce(controlTuple.size() == 2);

        DBSPVariablePath controlArg = controlType.ref().var();
        Utilities.enforce(data.outputType().is(DBSPTypeIndexedZSet.class),
                () -> "Data is not indexed: " + data.outputType());
        DBSPType valueType = data.getOutputIndexedZSetType().elementType;
        DBSPVariablePath dataArg = valueType.ref().var();
        DBSPParameter param = new DBSPParameter(dataArg.variable, dataArg.getType());
        DBSPExpression project = dataProjection
                .to(PartiallyMonotoneTuple.class)
                .getField(1)
                .projectExpression(dataArg.deref());
        DBSPExpression compare0 = controlArg.deref().field(0).not();
        DBSPExpression compare = DBSPControlledKeyFilterOperator.generateTupleCompare(
                project, controlArg.deref().field(1), DBSPOpcode.CONTROLLED_FILTER_GTE);
        compare = ExpressionCompiler.makeBinaryExpression(
                node, compare.getType(), DBSPOpcode.OR, compare0, compare);
        DBSPExpression closure = compare.closure(param, controlArg.asParameter());
        return new DBSPIntegrateTraceRetainValuesOperator(node, closure, data, control, accumulate);
    }

    public static DBSPIntegrateTraceRetainValuesOperator create(
            CalciteRelNode node, OutputPort data, IMaybeMonotoneType dataProjection, OutputPort control) {
        return create(node, data, dataProjection, control, true);
}

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            Utilities.enforce(newInputs.size() == 2, () -> "Expected 2 inputs, got " + newInputs.size());
            return new DBSPIntegrateTraceRetainValuesOperator(
                    this.getRelNode(), Objects.requireNonNull(function),
                    newInputs.get(0), newInputs.get(1), this.accumulate);
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

    // equivalent inherited from parent

    @SuppressWarnings("unused")
    public static DBSPIntegrateTraceRetainValuesOperator fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPSimpleOperator.CommonInfo info = commonInfoFromJson(node, decoder);
        boolean accumulate = Utilities.getBooleanProperty(node, "accumulate");

        return new DBSPIntegrateTraceRetainValuesOperator(CalciteEmptyRel.INSTANCE,
                info.getFunction(), info.getInput(0), info.getInput(1), accumulate)
                .addAnnotations(info.annotations(), DBSPIntegrateTraceRetainValuesOperator.class);
    }
}
