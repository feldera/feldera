package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStructExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPConstItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeStr;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;
import org.dbsp.util.Linq;

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

    TestCase(String name, DBSPCompiler compiler, DBSPCircuit circuit, InputOutputPair... data) {
        this.name = name;
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
    DBSPFunction createTesterCode(int testNumber) {
        List<DBSPStatement> list = new ArrayList<>();
        if (!this.name.isEmpty())
            list.add(new DBSPComment(this.name));
        DBSPLetStatement circuit = new DBSPLetStatement("circuit",
                new DBSPApplyExpression(this.circuit.name, DBSPTypeAny.INSTANCE), true);
        list.add(circuit);
        for (InputOutputPair pairs : this.data) {
            DBSPZSetLiteral[] inputs = pairs.getInputs();
            DBSPZSetLiteral[] outputs = pairs.getOutputs();
            DBSPLetStatement out = new DBSPLetStatement("output",
                    circuit.getVarReference().call(inputs));
            list.add(out);
            for (int i = 0; i < pairs.outputs.length; i++) {
                DBSPStatement compare = new DBSPExpressionStatement(
                        new DBSPApplyExpression("assert!", DBSPTypeVoid.INSTANCE,
                                new DBSPApplyExpression("must_equal", DBSPTypeBool.INSTANCE,
                                        out.getVarReference().field(i).borrow(),
                                        outputs[i].borrow()),
                                new DBSPStrLiteral(this.name, false, true)));
                list.add(compare);
            }
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction("test" + testNumber, new ArrayList<>(),
                DBSPTypeVoid.INSTANCE, body, Linq.list("#[test]"));
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
        JITProgram program = ToJitVisitor.circuitToJIT(this.compiler, this.circuit);
        DBSPComment comment = new DBSPComment(program.toAssembly());
        list.add(comment);

        String json = program.asJson().toPrettyString();
        DBSPStrLiteral value = new DBSPStrLiteral(json, false, true);
        DBSPConstItem item = new DBSPConstItem("CIRCUIT", DBSPTypeStr.INSTANCE.ref(), value);
        list.add(item);
        DBSPExpression read = new DBSPApplyMethodExpression("rematerialize", DBSPTypeAny.INSTANCE,
                new DBSPApplyExpression("serde_json::from_str::<SqlGraph>",
                        DBSPTypeAny.INSTANCE, item.getVariable()).unwrap());
        DBSPLetStatement graph = new DBSPLetStatement("graph", read);
        list.add(graph);

        DBSPLetStatement graphNodes = new DBSPLetStatement("graph_nodes",
                new DBSPApplyMethodExpression("nodes", DBSPTypeAny.INSTANCE,
                        graph.getVarReference()));
        list.add(graphNodes);

        DBSPLetStatement demands = new DBSPLetStatement("demands",
                new DBSPStructExpression(
                        DBSPTypeAny.INSTANCE.path(
                                new DBSPPath("Demands", "new")),
                        DBSPTypeAny.INSTANCE), true);
        list.add(demands);

        List<JITSourceOperator> tables = program.getSources();
        for (JITSourceOperator source : tables) {
            String table = source.table;
            long index = source.id;
            DBSPExpression nodeId = new DBSPStructExpression(
                    DBSPTypeAny.INSTANCE.path(
                            new DBSPPath("NodeId", "new")),
                    DBSPTypeAny.INSTANCE,
                    new DBSPU32Literal((int) index));
            DBSPLetStatement id = new DBSPLetStatement(table + "_id", nodeId);
            list.add(id);

            DBSPIndexExpression indexExpr = new DBSPIndexExpression(CalciteObject.EMPTY,
                    graphNodes.getVarReference(), id.getVarReference().borrow(), false);
            DBSPExpression layout = new DBSPApplyMethodExpression(
                    "layout", DBSPTypeAny.INSTANCE,
                    new DBSPApplyMethodExpression("unwrap_source", DBSPTypeAny.INSTANCE,
                            indexExpr.applyClone()));
            DBSPLetStatement stat = new DBSPLetStatement(table + "_layout", layout);
            list.add(stat);
        }

        DBSPExpression debug = new DBSPStructExpression(
                DBSPTypeAny.INSTANCE.path(new DBSPPath("CodegenConfig", "debug")),
                DBSPTypeAny.INSTANCE);
        DBSPExpression allocateCircuit = new DBSPStructExpression(
                DBSPTypeAny.INSTANCE.path(new DBSPPath("DbspCircuit", "new")),
                DBSPTypeAny.INSTANCE,
                graph.getVarReference(),
                DBSPBoolLiteral.TRUE,
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
                DBSPZSetLiteral input = new DBSPZSetLiteral(DBSPTypeWeight.INSTANCE, pair.inputs[index]);
                DBSPExpression contents = converter.apply(input).to(DBSPExpression.class);
                DBSPExpressionStatement append = new DBSPExpressionStatement(
                        new DBSPApplyMethodExpression(
                                "append_input",
                                DBSPTypeAny.INSTANCE,
                                circuit.getVarReference(),
                                new DBSPVariablePath(table + "_id", DBSPTypeAny.INSTANCE),
                                contents.borrow()));
                list.add(append);
                index++;
            }
        }

        DBSPExpressionStatement step = new DBSPExpressionStatement(
                new DBSPApplyMethodExpression("step", DBSPTypeAny.INSTANCE,
                        circuit.getVarReference()).unwrap());
        list.add(step);

        // read outputs
        if (data.length > 0) {
            InputOutputPair pair = this.data[0];
            int index = 0;
            for (JITSinkOperator sink : program.getSinks()) {
                String view = sink.viewName;
                DBSPZSetLiteral output = new DBSPZSetLiteral(DBSPTypeWeight.INSTANCE, pair.outputs[index]);
                DBSPExpression sinkId = new DBSPStructExpression(
                        DBSPTypeAny.INSTANCE.path(
                                new DBSPPath("NodeId", "new")),
                        DBSPTypeAny.INSTANCE,
                        new DBSPU32Literal((int) sink.id));
                DBSPLetStatement getOutput = new DBSPLetStatement(
                        view, new DBSPApplyMethodExpression("consolidate_output",
                        DBSPTypeAny.INSTANCE, circuit.getVarReference(), sinkId));
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
                        new DBSPApplyExpression("assert!", DBSPTypeVoid.INSTANCE,
                                new DBSPApplyExpression("must_equal_sc", DBSPTypeBool.INSTANCE,
                                        getOutput.getVarReference().borrow(), contents.borrow())));
                list.add(compare);
            }
        }

        DBSPStatement kill = new DBSPExpressionStatement(
                new DBSPApplyMethodExpression(
                        "kill", DBSPTypeAny.INSTANCE, circuit.getVarReference()).unwrap());
        list.add(kill);

        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction("test" + testNumber, new ArrayList<>(),
                DBSPTypeVoid.INSTANCE, body, Linq.list("#[test]"));
    }
}
