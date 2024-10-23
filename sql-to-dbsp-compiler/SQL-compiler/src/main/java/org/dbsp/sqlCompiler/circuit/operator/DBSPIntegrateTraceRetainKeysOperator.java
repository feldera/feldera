package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public final class DBSPIntegrateTraceRetainKeysOperator
        extends DBSPBinaryOperator implements GCOperator
{
    public DBSPIntegrateTraceRetainKeysOperator(
            CalciteObject node, DBSPExpression expression,
            DBSPOperator data, DBSPOperator control) {
        super(node, "integrate_trace_retain_keys", expression, data.getType(), data.isMultiset, data, control);
    }

    public static DBSPIntegrateTraceRetainKeysOperator create(
            CalciteObject node, DBSPOperator data, IMaybeMonotoneType dataProjection, DBSPOperator control) {
        DBSPType controlType = control.getType();
        assert controlType.is(DBSPTypeTupleBase.class) : "Control type is not a tuple: " + controlType;
        DBSPTypeTupleBase controlTuple = controlType.to(DBSPTypeTupleBase.class);
        assert controlTuple.size() == 2;
        DBSPType leftSliceType = Objects.requireNonNull(dataProjection.getProjectedType());
        assert leftSliceType.sameType(controlTuple.getFieldType(1)):
                "Projection type does not match control type " + leftSliceType + "/" + controlType;

        DBSPParameter param;
        DBSPExpression compare;
        DBSPVariablePath controlArg = controlType.ref().var();
        DBSPExpression compare0 = controlArg.deref().field(0).not();
        if (data.outputType.is(DBSPTypeIndexedZSet.class)) {
            DBSPType keyType = data.getOutputIndexedZSetType().keyType;
            DBSPVariablePath dataArg = keyType.var();
            param = new DBSPParameter(dataArg.variable, dataArg.getType().ref());
            DBSPExpression project = dataProjection
                    .to(PartiallyMonotoneTuple.class)
                    .getField(0)
                    .projectExpression(dataArg);
            compare = DBSPControlledFilterOperator.generateTupleCompare(
                    project, controlArg.deref().field(1).field(0), DBSPOpcode.CONTROLLED_FILTER_GTE);
        } else {
            DBSPType keyType = data.getOutputZSetElementType();
            DBSPVariablePath dataArg = keyType.var();
            param = new DBSPParameter(dataArg.variable, dataArg.getType().ref());
            DBSPExpression project = dataProjection.projectExpression(dataArg);
            compare = DBSPControlledFilterOperator.generateTupleCompare(
                    project, controlArg.deref().field(1), DBSPOpcode.CONTROLLED_FILTER_GTE);
        }
        compare = ExpressionCompiler.makeBinaryExpression(
                node, compare.getType(), DBSPOpcode.OR, compare0, compare);
        DBSPExpression closure = compare.closure(param, controlArg.asParameter());
        return new DBSPIntegrateTraceRetainKeysOperator(node, closure, data, control);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPIntegrateTraceRetainKeysOperator(
                this.getNode(), Objects.requireNonNull(expression),
                this.left(), this.right()).copyAnnotations(this);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2: "Expected 2 inputs, got " + newInputs.size();
        if (force || this.inputsDiffer(newInputs))
            return new DBSPIntegrateTraceRetainKeysOperator(
                    this.getNode(), this.getFunction(),
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
}
