package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToJitVisitor;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToRustJitLiteral;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITProgram;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators.JITSinkOperator;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators.JITSourceOperator;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerPasses;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.statement.*;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeStr;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;
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
     * Supplied input and expected outputs for the circuit.
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
    DBSPFunction createTesterCode(int testNumber, String codeDirectory) throws IOException {
        List<DBSPStatement> list = new ArrayList<>();
        if (!this.name.isEmpty())
            list.add(new DBSPComment(this.name));
        DBSPLetStatement circuit = new DBSPLetStatement("circuit",
                new DBSPApplyExpression(this.circuit.name, DBSPTypeAny.getDefault()), true);
        list.add(circuit);
        Simplify simplify = new Simplify(new StderrErrorReporter());
        for (InputOutputPair pairs : this.data) {
            DBSPZSetLiteral[] inputs = pairs.getInputs();
            inputs = Linq.map(inputs, t -> simplify.apply(t).to(DBSPZSetLiteral.class), DBSPZSetLiteral.class);
            DBSPZSetLiteral[] outputs = pairs.getOutputs();
            outputs = Linq.map(outputs, t -> simplify.apply(t).to(DBSPZSetLiteral.class), DBSPZSetLiteral.class);

            TableValue[] tableValues = new TableValue[inputs.length];
            for (int i = 0; i < inputs.length; i++)
                tableValues[i] = new TableValue("t" + i, inputs[i]);
            DBSPFunction inputFunction = TableValue.createInputFunction(tableValues, codeDirectory, "csv");
            list.add(new DBSPFunctionItem(inputFunction));
            DBSPLetStatement in = new DBSPLetStatement("input", inputFunction.call());
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
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction("test" + testNumber, new ArrayList<>(),
                new DBSPTypeVoid(), body, Linq.list("#[test]"));
    }

    /**
     * Generates a Rust function which tests a DBSP circuit using the JIT compiler.
     *
     * @return The code for a function that runs the circuit with the specified input
     * and tests the produced output.
     */
    DBSPFunction createJITTesterCode(int testNumber) {
        List<DBSPStatement> list = new ArrayList<>();
        if (!this.name.isEmpty())
            list.add(new DBSPComment(this.name));
        if (!this.javaTestName.isEmpty())
            list.add(new DBSPComment(this.javaTestName));
        JITProgram program = ToJitVisitor.circuitToJIT(this.compiler, this.circuit);
        DBSPComment comment = new DBSPComment(program.toAssembly());
        list.add(comment);

        String json = program.asJson().toPrettyString();
        DBSPStrLiteral value = new DBSPStrLiteral(json, false, true);
        DBSPConstItem item = new DBSPConstItem("CIRCUIT", new DBSPTypeStr(CalciteObject.EMPTY,false).ref(), value);
        list.add(item);
        DBSPExpression read = new DBSPApplyMethodExpression("rematerialize", DBSPTypeAny.getDefault(),
                new DBSPApplyExpression("serde_json::from_str::<SqlGraph>",
                        DBSPTypeAny.getDefault(), item.getVariable()).unwrap());
        DBSPLetStatement graph = new DBSPLetStatement("graph", read);
        list.add(graph);

        DBSPLetStatement graphNodes = new DBSPLetStatement("graph_nodes",
                new DBSPApplyMethodExpression("nodes", DBSPTypeAny.getDefault(),
                        graph.getVarReference()));
        list.add(graphNodes);

        DBSPLetStatement demands = new DBSPLetStatement("demands",
                new DBSPConstructorExpression(
                        new DBSPPath("Demands", "new").toExpression(),
                        DBSPTypeAny.getDefault()), true);
        list.add(demands);

        List<JITSourceOperator> tables = program.getSources();
        for (JITSourceOperator source : tables) {
            String table = source.table;
            long index = source.id;
            DBSPExpression nodeId = new DBSPConstructorExpression(
                    new DBSPPath("NodeId", "new").toExpression(),
                    DBSPTypeAny.getDefault(),
                    new DBSPU32Literal((int) index));
            DBSPLetStatement id = new DBSPLetStatement(table + "_id", nodeId);
            list.add(id);

            DBSPExpression indexExpr = new DBSPBinaryExpression(CalciteObject.EMPTY, DBSPTypeAny.getDefault(),
                    DBSPOpcode.RUST_INDEX, graphNodes.getVarReference(), id.getVarReference().borrow());
            DBSPExpression layout = new DBSPApplyMethodExpression(
                    "layout", DBSPTypeAny.getDefault(),
                    new DBSPApplyMethodExpression("unwrap_source", DBSPTypeAny.getDefault(),
                            indexExpr.applyClone()));
            DBSPLetStatement stat = new DBSPLetStatement(table + "_layout", layout);
            list.add(stat);
        }

        DBSPExpression debug = new DBSPConstructorExpression(
                new DBSPPath("CodegenConfig", "debug").toExpression(),
                DBSPTypeAny.getDefault());
        DBSPExpression allocateCircuit = new DBSPConstructorExpression(
                new DBSPPath("DbspCircuit", "new").toExpression(),
                DBSPTypeAny.getDefault(),
                graph.getVarReference(),
                new DBSPBoolLiteral(true),
                new DBSPUSizeLiteral(1),
                debug,
                demands.getVarReference());
        DBSPLetStatement circuit = new DBSPLetStatement("circuit", allocateCircuit, true);
        list.add(circuit);

        if (this.data.length > 1) {
            throw new UnsupportedException("Only support 1 input/output pair for tests", CalciteObject.EMPTY);
        }

        InnerPasses converter = new InnerPasses();
        converter.add(new Simplify(this.compiler));
        converter.add(new ToRustJitLiteral(this.compiler));
        if (data.length > 0) {
            InputOutputPair pair = this.data[0];
            int index = 0;
            for (JITSourceOperator source : tables) {
                String table = source.table;
                DBSPZSetLiteral input = new DBSPZSetLiteral(new DBSPTypeWeight(), pair.inputs[index]);
                DBSPExpression contents = converter.apply(input).to(DBSPExpression.class);
                DBSPExpressionStatement append = new DBSPExpressionStatement(
                        new DBSPApplyMethodExpression(
                                "append_input",
                                DBSPTypeAny.getDefault(),
                                circuit.getVarReference(),
                                new DBSPVariablePath(table + "_id", DBSPTypeAny.getDefault()),
                                contents.borrow()));
                list.add(append);
                index++;
            }
        }

        DBSPExpressionStatement step = new DBSPExpressionStatement(
                new DBSPApplyMethodExpression("step", DBSPTypeAny.getDefault(),
                        circuit.getVarReference()).unwrap());
        list.add(step);

        // read outputs
        if (data.length > 0) {
            InputOutputPair pair = this.data[0];
            int index = 0;
            for (JITSinkOperator sink : program.getSinks()) {
                String view = sink.viewName;
                DBSPZSetLiteral output = new DBSPZSetLiteral(new DBSPTypeWeight(), pair.outputs[index]);
                DBSPExpression sinkId = new DBSPConstructorExpression(
                        new DBSPPath("NodeId", "new").toExpression(),
                        DBSPTypeAny.getDefault(),
                        new DBSPU32Literal((int) sink.id));
                DBSPLetStatement getOutput = new DBSPLetStatement(
                        view, new DBSPApplyMethodExpression("consolidate_output",
                        DBSPTypeAny.getDefault(), circuit.getVarReference(), sinkId));
                list.add(getOutput);
                index++;
                /*
                DBSPExpressionStatement print = new DBSPExpressionStatement(
                        new DBSPApplyExpression("println!", null,
                                new DBSPStrLiteral("{:?}"),
                                getOutput.getVarReference()));
                list.add(print);
                 */
                DBSPExpression contents = converter.apply(output).to(DBSPExpression.class);
                DBSPStatement compare = new DBSPExpressionStatement(
                        new DBSPApplyExpression("assert!", new DBSPTypeVoid(),
                                new DBSPApplyExpression("must_equal_sc", new DBSPTypeBool(CalciteObject.EMPTY, false),
                                        getOutput.getVarReference().borrow(), contents.borrow())));
                list.add(compare);
            }
        }

        DBSPStatement kill = new DBSPExpressionStatement(
                new DBSPApplyMethodExpression(
                        "kill", DBSPTypeAny.getDefault(), circuit.getVarReference()).unwrap());
        list.add(kill);

        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction("test" + testNumber, new ArrayList<>(),
                new DBSPTypeVoid(), body, Linq.list("#[test]"));
    }
}
