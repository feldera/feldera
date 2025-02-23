package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import java.util.List;

import static org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator.commonInfoFromJson;

/**
 * The {@link DBSPControlledKeyFilterOperator} is an operator with 2 outputs, including
 * an error stream.  The left input is a stream of ZSets, while the
 * right input is a stream of scalars.  {@code function} is a boolean function
 * that takes a scalar and an input element; when the function returns 'true'
 * the input element makes it to the output.  The {@code error} function takes
 * a scalar, and an element and returns an error message with the ERROR_SCHEMA structure.
 * It is invoked only when the function returns 'false'. */
public final class DBSPControlledKeyFilterOperator extends DBSPOperatorWithError {
    public DBSPControlledKeyFilterOperator(
            CalciteObject node, DBSPClosureExpression function, DBSPClosureExpression error,
            OutputPort data, OutputPort control) {
        super(node, "controlled_key_filter_typed", data.outputType(),
                TypeCompiler.makeZSet(error.getResultType()), function, error);
        this.addInput(data);
        this.addInput(control);
        data.getOutputZSetType();
        assert function.getResultType().is(DBSPTypeBool.class);
        assert function.parameters.length == 2;
        assert error.parameters.length == 4;
    }

    public static final String LATE_ERROR = "Late value";

    public OutputPort left() {
        return this.inputs.get(0);
    }

    public OutputPort right() {
        return this.inputs.get(1);
    }

    static DBSPExpression compareRecursive(
            DBSPExpression compare, DBSPOpcode opcode, DBSPExpression left, DBSPExpression right) {
        DBSPType leftType = left.getType();
        if (leftType.is(DBSPTypeBaseType.class)) {
            DBSPType rightType = right.getType();
            assert leftType.withMayBeNull(true)
                    .sameType(rightType.withMayBeNull(true)):
                    "Types differ: " + leftType + " vs " + rightType;
            // Notice the comparison using AGG_GTE, which never returns NULL
            DBSPExpression comparison = new DBSPBinaryExpression(CalciteObject.EMPTY,
                    DBSPTypeBool.create(false), opcode, left, right);
            return new DBSPBinaryExpression(CalciteObject.EMPTY,
                    new DBSPTypeBool(CalciteObject.EMPTY, false), DBSPOpcode.AND, compare, comparison);
        } else if (leftType.is(DBSPTypeRef.class)) {
            return compareRecursive(compare, opcode, left.deref(), right.deref());
        } else {
            DBSPTypeTupleBase tuple = leftType.to(DBSPTypeTupleBase.class);
            for (int i = 0; i < tuple.size(); i++) {
                compare = compareRecursive(compare, opcode, left.field(i), right.field(i));
            }
        }
        return compare;
    }

    /** Given two expressions that evaluate to tuples with the same type
     * (ignoring nullability), generate an expression
     * that evaluates to 'true' only if all fields in the left tuple compare to true
     * using the opcode operation (recursively) than the corresponding fields in the right tuple.
     * @param left   Left tuple to compare
     * @param right  Right tuple to compare
     * @param opcode Comparison operation to use. */
    public static DBSPExpression generateTupleCompare(DBSPExpression left, DBSPExpression right, DBSPOpcode opcode) {
        DBSPLetStatement leftVar = new DBSPLetStatement("left", left.borrow());
        DBSPLetStatement rightVar = new DBSPLetStatement("right", right.borrow());
        List<DBSPStatement> statements = Linq.list(leftVar, rightVar);
        DBSPExpression compare = compareRecursive(
                new DBSPBoolLiteral(true), opcode,
                leftVar.getVarReference().deref(), rightVar.getVarReference().deref());
        return new DBSPBlockExpression(statements, compare);
    }

    @Override
    public DBSPOperatorWithError withInputs(List<OutputPort> newInputs, boolean force) {
        assert newInputs.size() == 2: "Expected 2 inputs, got " + newInputs.size();
        if (force || this.inputsDiffer(newInputs))
            return new DBSPControlledKeyFilterOperator(
                    this.getNode(), this.function, this.error,
                    newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
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

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("let (")
                .append(this.getOutputName(0))
                .append(", ")
                .append(this.getOutputName(1))
                .append("): (")
                .append(this.outputStreamType)
                .append(", ")
                .append(this.errorStreamType)
                .append(") = ")
                .append(this.left().getOutputName())
                .append(".")
                .append(this.operation)
                .append("(&")
                .append(this.right().getOutputName())
                .append(", ")
                .append(this.function)
                .append(", ")
                .append(this.error)
                .append(");");
    }

    @Override
    public String toString() {
        return this.getClass()
                .getSimpleName()
                .replace("DBSP", "")
                .replace("Operator", "")
                + " " + this.getIdString();
    }

    @SuppressWarnings("unused")
    public static DBSPControlledKeyFilterOperator fromJson(JsonNode node, JsonDecoder decoder) {
        // Populate cache
        fromJsonInner(node, "errorType", decoder, DBSPType.class);
        DBSPClosureExpression error = fromJsonInner(node, "error", decoder, DBSPClosureExpression.class);
        DBSPSimpleOperator.CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPControlledKeyFilterOperator(CalciteObject.EMPTY,
                info.getClosureFunction(), error, info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPControlledKeyFilterOperator.class);
    }
}
