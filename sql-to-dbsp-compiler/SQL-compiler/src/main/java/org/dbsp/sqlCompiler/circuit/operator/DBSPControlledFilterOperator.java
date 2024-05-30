package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * The ControlledFilterOperator does not correspond directly to any
 * DBSP operator.  The left input is a stream of ZSets or IndexedZSets, while the
 * right input is a stream of scalars.  The function is a boolean function
 * that takes an input element and a scalar; when the function returns 'true'
 * the input element makes it to the output. */
public final class DBSPControlledFilterOperator extends DBSPOperator {
    public DBSPControlledFilterOperator(
            CalciteObject node, DBSPExpression expression,
            DBSPOperator data, DBSPOperator control) {
        super(node, "controlled_filter", expression, data.getType(), data.isMultiset);
        this.addInput(data);
        this.addInput(control);
        // this.checkArgumentFunctionType(expression, 0, data);
    }

    static DBSPExpression compareRecursive(DBSPExpression compare, DBSPExpression left, DBSPExpression right) {
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();
        assert leftType.sameType(rightType): "Types differ: " + leftType + " vs " + rightType;
        if (leftType.is(DBSPTypeBaseType.class)) {
            // Notice the comparison using AGG_GTE, which never returns NULL
            DBSPExpression comparison = new DBSPBinaryExpression(CalciteObject.EMPTY,
                    new DBSPTypeBool(CalciteObject.EMPTY, false), DBSPOpcode.AGG_GTE, left, right);
            return new DBSPBinaryExpression(CalciteObject.EMPTY,
                    new DBSPTypeBool(CalciteObject.EMPTY, false), DBSPOpcode.AND, compare, comparison);
        } else if (leftType.is(DBSPTypeRef.class)) {
            return compareRecursive(compare, left.deref(), right.deref());
        } else {
            DBSPTypeTupleBase tuple = leftType.to(DBSPTypeTupleBase.class);
            for (int i = 0; i < tuple.size(); i++) {
                compare = compareRecursive(compare, left.field(i), right.field(i));
            }
        }
        return compare;
    }

    // Given two expressions that evaluate to tuples with the same type, generate an expression
    // that evaluates to 'true' only if all fields in the left tuple are bigger or equal (recursively)
    // than the corresponding fields in the right tuple.
    static DBSPExpression generateTupleCompare(DBSPExpression left, DBSPExpression right) {
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();
        assert leftType.sameType(rightType):
                "Types differ: " + leftType + " vs " + rightType;
        DBSPLetStatement leftVar = new DBSPLetStatement("left", left.borrow());
        DBSPLetStatement rightVar = new DBSPLetStatement("right", right.borrow());
        List<DBSPStatement> statements = Linq.list(leftVar, rightVar);
        DBSPExpression compare = compareRecursive(
                new DBSPBoolLiteral(true),
                leftVar.getVarReference().deref(), rightVar.getVarReference().deref());
        return new DBSPBlockExpression(statements, compare);
    }

    public static DBSPControlledFilterOperator create(
            CalciteObject node, DBSPOperator data, IMaybeMonotoneType monotoneType, DBSPOperator control) {
        DBSPType controlType = control.getType();
        DBSPType leftSliceType = Objects.requireNonNull(monotoneType.getProjectedType());
        assert leftSliceType.sameType(controlType):
                "Projection type does not match control type " + leftSliceType + "/" + controlType;

        DBSPType rowType = data.getOutputRowType();
        DBSPVariablePath dataArg = new DBSPVariablePath("d", rowType);
        DBSPParameter param;
        if (rowType.is(DBSPTypeRawTuple.class)) {
            DBSPTypeRawTuple raw = rowType.to(DBSPTypeRawTuple.class);
            param = new DBSPParameter(dataArg.variable,
                    new DBSPTypeRawTuple(raw.tupFields[0].ref(), raw.tupFields[1].ref()));
        } else {
            param = new DBSPParameter(dataArg.variable, dataArg.getType().ref());
        }
        DBSPExpression projection = monotoneType.projectExpression(dataArg);

        DBSPVariablePath controlArg = new DBSPVariablePath("c", controlType.ref());
        DBSPExpression compare = DBSPControlledFilterOperator.generateTupleCompare(projection, controlArg.deref());
        DBSPExpression closure = compare.closure(param, controlArg.asParameter());
        return new DBSPControlledFilterOperator(node, closure, data, control);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPControlledFilterOperator(
                this.getNode(), Objects.requireNonNull(expression),
                this.inputs.get(0), this.inputs.get(1));
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2: "Expected 2 inputs, got " + newInputs.size();
        if (force || this.inputsDiffer(newInputs))
            return new DBSPControlledFilterOperator(
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
