package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputPair;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.statement.*;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.Linq;
import org.dbsp.util.TableValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a test case that will be executed.
 */
class TestCase {
    /**
     * Name of the test case.
     */
    public final String name;
    /**
     * Name of the Java test that is being run.
     */
    public final String javaTestName;
    /**
     * Compiler used to compile the test case.
     * Used for code generation.
     */
    public final DBSPCompiler compiler;
    /**
     * Circuit that is being tested.
     */
    public final DBSPCircuit circuit;
    /**
     * Supplied input and expected corresponding outputs for the circuit.
     */
    public final InputOutputPair[] data;

    TestCase(String name, String javaTestName, DBSPCompiler compiler,
             DBSPCircuit circuit, InputOutputPair... data) {
        this.name = name;
        this.javaTestName = javaTestName;
        this.circuit = circuit;
        this.data = data;
        this.compiler = compiler;
    }

    /**
     * Generates a Rust function which tests a DBSP circuit.
     *
     * @return The code for a function that runs the circuit with the specified
     * input and tests the produced output.
     */
    DBSPFunction createTesterCode(int testNumber,
                                  @SuppressWarnings("SameParameterValue") String codeDirectory) throws IOException {
        List<DBSPStatement> list = new ArrayList<>();
        if (!this.name.isEmpty())
            list.add(new DBSPComment(this.name));
        DBSPLetStatement circuit = new DBSPLetStatement("circuit",
                new DBSPApplyExpression(this.circuit.name, DBSPTypeAny.getDefault()), true);
        list.add(circuit);
        Simplify simplify = new Simplify(new StderrErrorReporter());
        int pair = 0;
        for (InputOutputPair pairs : this.data) {
            DBSPZSetLiteral[] inputs = pairs.getInputs();
            inputs = Linq.map(inputs, t -> simplify.apply(t).to(DBSPZSetLiteral.class), DBSPZSetLiteral.class);
            DBSPZSetLiteral[] outputs = pairs.getOutputs();
            outputs = Linq.map(outputs, t -> simplify.apply(t).to(DBSPZSetLiteral.class), DBSPZSetLiteral.class);

            TableValue[] tableValues = new TableValue[inputs.length];
            for (int i = 0; i < inputs.length; i++)
                tableValues[i] = new TableValue("t" + i, inputs[i]);
            String functionName = "input" + pair;
            DBSPFunction inputFunction = TableValue.createInputFunction(
                    functionName, tableValues, codeDirectory, "csv");
            list.add(new DBSPFunctionItem(inputFunction));
            DBSPLetStatement in = new DBSPLetStatement(functionName, inputFunction.call());
            list.add(in);
            DBSPExpression[] arguments = new DBSPExpression[inputs.length];
            for (int i = 0; i < inputs.length; i++)
                arguments[i] = in.getVarReference().field(i);

            DBSPLetStatement out = new DBSPLetStatement("output",
                    circuit.getVarReference().call(arguments));
            list.add(out);
            for (int i = 0; i < pairs.outputs.length; i++) {
                String message = System.lineSeparator() +
                        "mvn test -Dtest=" + this.javaTestName +
                        System.lineSeparator() + this.name;
                DBSPStatement compare = new DBSPExpressionStatement(
                        new DBSPApplyExpression("assert!", new DBSPTypeVoid(),
                                new DBSPApplyExpression("must_equal", new DBSPTypeBool(CalciteObject.EMPTY, false),
                                        out.getVarReference().field(i).borrow(),
                                        outputs[i].borrow()),
                                new DBSPStrLiteral(message, false, true)));
                list.add(compare);
            }
            pair++;
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction("test" + testNumber, new ArrayList<>(),
                new DBSPTypeVoid(), body, Linq.list("#[test]"));
    }
}
