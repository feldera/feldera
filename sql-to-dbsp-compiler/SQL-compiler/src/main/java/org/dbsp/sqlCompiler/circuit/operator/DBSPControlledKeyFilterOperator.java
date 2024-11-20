package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.util.IIndentStream;

import java.util.List;
import java.util.Objects;

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
        super(node, "controlled_key_filter", data.outputType(), function, error);
        this.addInput(data);
        this.addInput(control);
        data.getOutputZSetType();
        assert function.getResultType().is(DBSPTypeBool.class);
        assert function.parameters.length == 2;
        assert error.getResultType().sameType(ERROR_SCHEMA) :
            "Error function returns " + error.getResultType() + " but should return " + ERROR_SCHEMA;
        assert error.parameters.length == 3;
    }

    static final String LATE_ERROR = "Late value";

    public OutputPort left() {
        return this.inputs.get(0);
    }

    public OutputPort right() {
        return this.inputs.get(1);
    }

    public static DBSPControlledKeyFilterOperator create(
            CalciteObject node, String tableOrViewName,
            OutputPort data, IMaybeMonotoneType monotoneType,
            OutputPort control, DBSPOpcode opcode) {
        DBSPType controlType = control.outputType();

        DBSPType leftSliceType = Objects.requireNonNull(monotoneType.getProjectedType());
        assert leftSliceType.sameType(controlType):
                "Projection type does not match control type " + leftSliceType + "/" + controlType;

        DBSPType rowType = data.getOutputRowType();
        DBSPVariablePath dataArg = rowType.var();
        DBSPParameter param;
        if (rowType.is(DBSPTypeRawTuple.class)) {
            DBSPTypeRawTuple raw = rowType.to(DBSPTypeRawTuple.class);
            param = new DBSPParameter(dataArg.variable,
                    new DBSPTypeRawTuple(raw.tupFields[0].ref(), raw.tupFields[1].ref()));
        } else {
            param = new DBSPParameter(dataArg.variable, dataArg.getType().ref());
        }
        DBSPExpression projection = monotoneType.projectExpression(dataArg);

        DBSPVariablePath controlArg = controlType.ref().var();
        DBSPExpression compare = DBSPControlledFilterOperator.generateTupleCompare(
                projection, controlArg.deref(), opcode);
        DBSPClosureExpression closure = compare.closure(param, controlArg.asParameter());

        // The last parameter is not used
        DBSPVariablePath valArg = new DBSPTypeRawTuple().var();
        DBSPClosureExpression error = new DBSPTupleExpression(
                new DBSPStringLiteral(tableOrViewName),
                new DBSPStringLiteral(LATE_ERROR),
                dataArg.cast(new DBSPTypeVariant(true))).closure(
                        param, controlArg.asParameter(), valArg.asParameter());
        return new DBSPControlledKeyFilterOperator(node, closure, error, data, control);
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
}
