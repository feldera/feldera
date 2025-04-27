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
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
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

public final class DBSPIntegrateTraceRetainKeysOperator
        extends DBSPBinaryOperator implements GCOperator
{
    public DBSPIntegrateTraceRetainKeysOperator(
            CalciteRelNode node, DBSPExpression expression,
            OutputPort data, OutputPort control) {
        super(node, "integrate_trace_retain_keys", expression,
                data.outputType(), data.isMultiset(), data, control, false);
    }

    /** Create a operator to retain keys and returns it.  May return null if the keys contain no fields. */
    @Nullable
    public static DBSPIntegrateTraceRetainKeysOperator create(
            CalciteRelNode node, OutputPort data, IMaybeMonotoneType dataProjection, OutputPort control) {
        DBSPType controlType = control.outputType();
        Utilities.enforce(controlType.is(DBSPTypeTupleBase.class), "Control type is not a tuple: " + controlType);
        DBSPTypeTupleBase controlTuple = controlType.to(DBSPTypeTupleBase.class);
        Utilities.enforce(controlTuple.size() == 2);
        DBSPType leftSliceType = Objects.requireNonNull(dataProjection.getProjectedType());
        Utilities.enforce(leftSliceType.sameType(controlTuple.getFieldType(1)),
                "Projection type does not match control type " + leftSliceType + "/" + controlType);

        DBSPParameter param;
        DBSPExpression compare;
        DBSPVariablePath controlArg = controlType.ref().var();
        DBSPExpression compare0 = controlArg.deref().field(0).not();
        if (data.outputType().is(DBSPTypeIndexedZSet.class)) {
            DBSPType keyType = data.getOutputIndexedZSetType().keyType;
            DBSPVariablePath dataArg = keyType.ref().var();
            param = new DBSPParameter(dataArg.variable, dataArg.getType());
            IMaybeMonotoneType dataField0 = dataProjection
                    .to(PartiallyMonotoneTuple.class)
                    .getField(0);
            if (!dataField0.mayBeMonotone())
                return null;
            DBSPExpression project = dataField0
                    .projectExpression(dataArg.deref());
            compare = DBSPControlledKeyFilterOperator.generateTupleCompare(
                    project, controlArg.deref().field(1).field(0), DBSPOpcode.CONTROLLED_FILTER_GTE);
        } else {
            DBSPType keyType = data.getOutputZSetElementType();
            DBSPVariablePath dataArg = keyType.ref().var();
            param = new DBSPParameter(dataArg.variable, dataArg.getType());
            if (!dataProjection.mayBeMonotone())
                return null;
            DBSPExpression project = dataProjection.projectExpression(dataArg.deref());
            compare = DBSPControlledKeyFilterOperator.generateTupleCompare(
                    project, controlArg.deref().field(1), DBSPOpcode.CONTROLLED_FILTER_GTE);
        }
        compare = ExpressionCompiler.makeBinaryExpression(
                node, compare.getType(), DBSPOpcode.OR, compare0, compare);
        DBSPExpression closure = compare.closure(param, controlArg.asParameter());
        return new DBSPIntegrateTraceRetainKeysOperator(node, closure, data, control);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPIntegrateTraceRetainKeysOperator(
                this.getRelNode(), Objects.requireNonNull(expression),
                this.left(), this.right()).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        Utilities.enforce(newInputs.size() == 2, "Expected 2 inputs, got " + newInputs.size());
        if (force || this.inputsDiffer(newInputs))
            return new DBSPIntegrateTraceRetainKeysOperator(
                    this.getRelNode(), this.getFunction(),
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

    // equivalent inherited from parent

    @SuppressWarnings("unused")
    public static DBSPIntegrateTraceRetainKeysOperator fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPSimpleOperator.CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPIntegrateTraceRetainKeysOperator(CalciteEmptyRel.INSTANCE,
                info.getFunction(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPIntegrateTraceRetainKeysOperator.class);
    }
}
