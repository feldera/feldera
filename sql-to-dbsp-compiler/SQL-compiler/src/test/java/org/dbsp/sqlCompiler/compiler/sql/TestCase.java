package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.simple.Change;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputChange;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeFP;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.TableValue;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Represents a test case that will be executed. */
class TestCase {
    /** Name of the test case. */
    public final String name;
    /** Name of the Java test that is being run. */
    public final String javaTestName;
    public final BaseSQLTests.CompilerCircuitStream ccs;
    /** Non-null if the test is supposed to panic.  In that case this
     * contains the expected panic message. */
    @Nullable
    public final String message;

    TestCase(String name, String javaTestName, BaseSQLTests.CompilerCircuitStream ccs, @Nullable String message) {
        this.name = name;
        this.javaTestName = javaTestName;
        this.ccs = ccs;
        this.message = message;
    }

    /**
     * Generates a Rust function which tests a DBSP circuit.
     *
     * @return The code for a function that runs the circuit with the specified
     * input and tests the produced output. */
    DBSPFunction createTesterCode(int testNumber,
                                  @SuppressWarnings("SameParameterValue")
                                  String codeDirectory) throws IOException {
        List<DBSPStatement> list = new ArrayList<>();
        if (!this.name.isEmpty())
            list.add(new DBSPComment(this.name));
        boolean useHandles = this.ccs.compiler.options.ioOptions.emitHandles;
        DBSPExpression[] circuitArguments = new DBSPExpression[1];
        circuitArguments[0] = new DBSPApplyExpression("CircuitConfig::with_workers", DBSPTypeAny.getDefault(), new DBSPUSizeLiteral(2));
        DBSPLetStatement cas = new DBSPLetStatement("circuitAndStreams",
                new DBSPApplyExpression(this.ccs.circuit.name, DBSPTypeAny.getDefault(), circuitArguments).resultUnwrap(),
                true);
        list.add(cas);
        DBSPLetStatement streams = new DBSPLetStatement("streams", cas.getVarReference().field(1));
        list.add(streams);

        int pair = 0;
        for (InputOutputChange changes : this.ccs.stream.changes) {
            Change inputs = changes.getInputs().simplify();
            Change outputs = changes.getOutputs().simplify();

            TableValue[] tableValues = new TableValue[inputs.getSetCount()];
            for (int i = 0; i < inputs.getSetCount(); i++)
                tableValues[i] = new TableValue("t" + i, inputs.getSet(i));
            String functionName = "input" + pair;
            DBSPFunction inputFunction = TableValue.createInputFunction(
                    functionName, tableValues, codeDirectory, "csv");
            list.add(new DBSPFunctionItem(inputFunction));
            DBSPLetStatement in = new DBSPLetStatement(functionName, inputFunction.call());
            list.add(in);

            if (!useHandles)
                throw new UnimplementedException();

            for (int i = 0; i < inputs.getSetCount(); i++) {
                String function;
                if (inputs.getSetType(i).is(DBSPTypeZSet.class))
                    function = "append_to_collection_handle";
                else
                    function = "append_to_upsert_handle";
                list.add(new DBSPApplyExpression(function, DBSPTypeAny.getDefault(),
                        in.getVarReference().field(i).borrow(), streams.getVarReference().field(i).borrow())
                        .toStatement());
            }
            DBSPLetStatement step =
                    new DBSPLetStatement("_", new DBSPApplyMethodExpression(
                    "step", DBSPTypeAny.getDefault(), cas.getVarReference().field(0)).resultUnwrap());
            list.add(step);

            for (int i = 0; i < changes.outputs.getSetCount(); i++) {
                String message = System.lineSeparator() +
                        "mvn test -Dtest=" + this.javaTestName +
                        System.lineSeparator() + this.name;

                DBSPType rowType = outputs.getSetElementType(i);
                boolean foundFp = false;
                DBSPExpression[] converted = null;
                DBSPVariablePath var = null;
                if (rowType.is(DBSPTypeTuple.class)) {
                    // Convert FP values to strings to ensure deterministic comparisons
                    DBSPTypeTuple tuple = rowType.to(DBSPTypeTuple.class);
                    converted = new DBSPExpression[tuple.size()];
                    var = new DBSPVariablePath("t", tuple.ref());
                    for (int index = 0; index < tuple.size(); index++) {
                        DBSPType fieldType = tuple.getFieldType(index);
                        if (fieldType.is(DBSPTypeFP.class)) {
                            converted[index] = var.deref().field(index).cast(DBSPTypeString.varchar(fieldType.mayBeNull));
                            foundFp = true;
                        } else {
                            converted[index] = var.deref().field(index).applyCloneIfNeeded();
                        }
                    }
                } else {
                    // TODO: handle Vec<> values with FP values inside.
                    // Currently we don't have any tests with this case.
                }

                DBSPExpression expected = outputs.getSet(i);
                DBSPExpression actual = new DBSPApplyExpression("read_output_handle", DBSPTypeAny.getDefault(),
                        streams.getVarReference().field(changes.inputs.getSetCount() + i).borrow());
                if (foundFp) {
                    DBSPExpression convertedValue = new DBSPTupleExpression(converted);
                    DBSPExpression converter = convertedValue.closure(var.asParameter());
                    DBSPVariablePath converterVar = new DBSPVariablePath("converter", DBSPTypeAny.getDefault());
                    list.add(new DBSPLetStatement(converterVar.variable, converter));
                    expected = new DBSPApplyExpression("zset_map", convertedValue.getType(), expected.borrow(), converterVar);
                    actual = new DBSPApplyExpression("zset_map", convertedValue.getType(), actual.borrow(), converterVar);
                }

                DBSPStatement compare =
                        new DBSPApplyExpression("assert!", new DBSPTypeVoid(),
                                new DBSPApplyExpression("must_equal",
                                        new DBSPTypeBool(CalciteObject.EMPTY, false),
                                        actual.borrow(),
                                        expected.borrow()),
                                new DBSPStrLiteral(message, false, true)).toStatement();
                list.add(compare);
            }
            pair++;
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        List<String> annotations = new ArrayList<>();
        annotations.add("#[test]");
        if (this.message != null)
            annotations.add("#[should_panic(expected = " + Utilities.doubleQuote(this.message) + ")]");
        return new DBSPFunction("test" + testNumber, new ArrayList<>(),
                new DBSPTypeVoid(), body, annotations);
    }
}
