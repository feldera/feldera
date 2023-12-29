package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.ValueProjection;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class DBSPIntegrateTraceRetainKeysOperator extends DBSPOperator {
    public DBSPIntegrateTraceRetainKeysOperator(
            CalciteObject node, DBSPExpression expression,
            DBSPOperator data, DBSPOperator control) {
        super(node, "integrate_trace_retain_keys", expression, data.getType(), data.isMultiset);
        this.addInput(data);
        this.addInput(control);
    }

    public static DBSPIntegrateTraceRetainKeysOperator create(
            CalciteObject node, DBSPOperator data, ValueProjection dataProjection, DBSPOperator control) {
        DBSPType controlType = control.getType();
        DBSPType leftSliceType = dataProjection.getProjectionResultType();
        assert leftSliceType.sameType(controlType):
                "Projection type does not match control type " + leftSliceType + "/" + controlType;

        DBSPParameter param;
        DBSPExpression compare;
        DBSPVariablePath controlArg = new DBSPVariablePath("c", controlType.ref());
        if (data.outputType.is(DBSPTypeIndexedZSet.class)) {
            DBSPType keyType = data.getOutputIndexedZSetType().keyType;
            DBSPVariablePath dataArg = new DBSPVariablePath("d", keyType);
            param = new DBSPParameter(dataArg.variable, dataArg.getType().ref());
            compare = DBSPControlledFilterOperator.generateTupleCompare(
                    dataArg, controlArg.deref().field(0));
        } else {
            DBSPType keyType = data.getOutputZSetElementType();
            DBSPVariablePath dataArg = new DBSPVariablePath("d", keyType);
            param = new DBSPParameter(dataArg.variable, dataArg.getType().ref());
            DBSPExpression project = dataProjection.project(dataArg);
            compare = DBSPControlledFilterOperator.generateTupleCompare(
                    project, controlArg.deref());
        }
        DBSPExpression closure = compare.closure(param, controlArg.asParameter());
        return new DBSPIntegrateTraceRetainKeysOperator(node, closure, data, control);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPIntegrateTraceRetainKeysOperator(
                this.getNode(), Objects.requireNonNull(expression),
                this.inputs.get(0), this.inputs.get(1));
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2: "Expected 2 inputs, got " + newInputs.size();
        if (force || this.inputsDiffer(newInputs))
            return new DBSPIntegrateTraceRetainKeysOperator(
                    this.getNode(), this.getFunction(),
                    newInputs.get(0), newInputs.get(1));
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
