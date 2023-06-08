/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToJitVisitor;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToRustJitLiteral;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITProgram;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators.JITSinkOperator;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators.JITSourceOperator;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustSqlRuntimeLibrary;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCodeVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.NoIntegralVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeIncrementalVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.PassesVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveOperatorsVisitor;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPConstItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.util.Linq;
import org.dbsp.util.UnsupportedException;
import org.dbsp.util.Utilities;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for SQL-based tests.
 */
public class BaseSQLTests {
    public static final String rustDirectory = "../temp/src";
    public static final String testFilePath = rustDirectory + "/lib.rs";

    public static class InputOutputPair {
        public final DBSPZSetLiteral.Contents[] inputs;
        public final DBSPZSetLiteral.Contents[] outputs;

        public InputOutputPair(DBSPZSetLiteral.Contents[] inputs, DBSPZSetLiteral.Contents[] outputs) {
            this.inputs = inputs;
            this.outputs = outputs;
        }

        public InputOutputPair(DBSPZSetLiteral.Contents input, DBSPZSetLiteral.Contents output) {
            this.inputs = new DBSPZSetLiteral.Contents[1];
            this.inputs[0] = input;
            this.outputs = new DBSPZSetLiteral.Contents[1];
            this.outputs[0] = output;
        }

        static DBSPZSetLiteral[] toZSets(DBSPZSetLiteral.Contents[] data) {
            return Linq.map(data, s -> new DBSPZSetLiteral(DBSPTypeWeight.INSTANCE, s), DBSPZSetLiteral.class);
        }

        public DBSPZSetLiteral[] getInputs() {
            return toZSets(this.inputs);
        }

        public DBSPZSetLiteral[] getOutputs() {
            return toZSets(this.outputs);
        }
    }

    protected static DBSPCircuit getCircuit(DBSPCompiler compiler) {
        String name = "circuit" + testsToRun.size();
        return compiler.getFinalCircuit(name);
    }

    private static class TestCase {
        public final DBSPCompiler compiler;
        public final DBSPCircuit circuit;
        public final InputOutputPair[] data;

        TestCase(DBSPCompiler compiler, DBSPCircuit circuit, InputOutputPair... data) {
            this.circuit = circuit;
            this.data = data;
            this.compiler = compiler;
        }

        /**
         * Generates a Rust function which tests a DBSP circuit.
         * @return The code for a function that runs the circuit with the specified
         *         input and tests the produced output.
         */
        DBSPFunction createTesterCode(int testNumber) {
            List<DBSPStatement> list = new ArrayList<>();
            DBSPLetStatement circ = new DBSPLetStatement("circuit",
                    new DBSPApplyExpression(this.circuit.name, DBSPTypeAny.INSTANCE), true);
            list.add(circ);
            for (InputOutputPair pairs: this.data) {
                DBSPZSetLiteral[] inputs = pairs.getInputs();
                DBSPZSetLiteral[] outputs = pairs.getOutputs();
                DBSPLetStatement out = new DBSPLetStatement("output",
                        circ.getVarReference().call(inputs));
                list.add(out);
                for (int i = 0; i < pairs.outputs.length; i++) {
                    DBSPStatement compare = new DBSPExpressionStatement(
                            new DBSPApplyExpression("assert!", null,
                                    new DBSPApplyExpression("must_equal", DBSPTypeBool.INSTANCE,
                                            new DBSPFieldExpression(null, out.getVarReference(), i).borrow(),
                                            outputs[i].borrow())));
                    list.add(compare);
                }
            }
            DBSPExpression body = new DBSPBlockExpression(list, null);
            return new DBSPFunction("test" + testNumber, new ArrayList<>(),
                    null, body, Linq.list("#[test]"));
        }

        /**
         * Generates a Rust function which tests a DBSP circuit using the JIT compiler.
         * @return The code for a function that runs the circuit with the specified input
         *         and tests the produced output.
         */
        DBSPFunction createJITTesterCode(int testNumber) {
            List<DBSPStatement> list = new ArrayList<>();
            // Logger.INSTANCE.setDebugLevel(ToJitVisitor.class, 4);
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
            for (JITSourceOperator source: tables) {
                String table = source.table;
                long index = source.id;
                DBSPExpression nodeId = new DBSPStructExpression(
                        DBSPTypeAny.INSTANCE.path(
                                new DBSPPath("NodeId", "new")),
                        DBSPTypeAny.INSTANCE,
                        new DBSPU32Literal((int)index));
                DBSPLetStatement id = new DBSPLetStatement(table + "_id", nodeId);
                list.add(id);

                DBSPIndexExpression indexExpr = new DBSPIndexExpression(null,
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
                throw new UnsupportedException("Only support 1 input/output pair for tests");
            }

            ToRustJitLiteral converter = new ToRustJitLiteral(this.compiler);
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
                            new DBSPU32Literal((int)sink.id));
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
                            new DBSPApplyExpression("assert!", null,
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
                    null, body, Linq.list("#[test]"));
        }
    }

    // Collect here all the tests to run and execute them using a single Rust compilation
    static final List<TestCase> testsToRun = new ArrayList<>();

    @BeforeClass
    public static void prepareTests() throws IOException {
        generateLib();
        testsToRun.clear();
    }

    @AfterClass
    public static void runAllTests() throws IOException, InterruptedException {
        if (testsToRun.isEmpty())
            return;
        PrintStream outputStream = new PrintStream(Files.newOutputStream(Paths.get(testFilePath)));
        // Use the compiler from the first test case.
        DBSPCompiler firstCompiler = testsToRun.get(0).compiler;
        RustFileWriter writer = new RustFileWriter(firstCompiler, outputStream);
        int testNumber = 0;
        for (TestCase test: testsToRun) {
            if (!test.compiler.options.same(firstCompiler.options))
                throw new RuntimeException("Tests are not compiled with the same options: "
                        + test.compiler.options + " and " + firstCompiler.options);
            if (test.compiler.options.ioOptions.jit) {
                DBSPFunction tester = test.createJITTesterCode(testNumber);
                writer.add(tester);
            } else {
                // Standard test
                writer.add(test.circuit);
                DBSPFunction tester = test.createTesterCode(testNumber);
                writer.add(tester);
            }
            testNumber++;
        }
        writer.writeAndClose();
        Utilities.compileAndTestRust(rustDirectory, false);
        testsToRun.clear();
    }

    public static void generateLib() throws IOException {
        RustSqlRuntimeLibrary.INSTANCE.writeSqlLibrary( "../lib/genlib/src/lib.rs");
    }

    CircuitVisitor getOptimizer(DBSPCompiler compiler) {
        DeadCodeVisitor dead = new DeadCodeVisitor(compiler);
        return new PassesVisitor(
                compiler,
                new OptimizeIncrementalVisitor(compiler),
                dead,
                new RemoveOperatorsVisitor(compiler, dead.toKeep),
                new NoIntegralVisitor(compiler)
        );
    }

    void testQueryBase(String query, boolean incremental, boolean optimize, boolean jit, InputOutputPair... streams) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCompiler compiler = this.compileQuery(query, incremental, optimize, jit);
            compiler.optimize();
            DBSPCircuit circuit = getCircuit(compiler);
            this.addRustTestCase(compiler, circuit, streams);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void addRustTestCase(DBSPCompiler compiler, DBSPCircuit circuit, InputOutputPair... streams) {
        TestCase test = new TestCase(compiler, circuit, streams);
        testsToRun.add(test);
    }

    static CompilerOptions testOptions(boolean incremental, boolean optimize, boolean jit) {
        CompilerOptions options = new CompilerOptions();
        options.optimizerOptions.throwOnError = true;
        options.optimizerOptions.generateInputForEveryTable = true;
        options.ioOptions.jit = jit;
        options.optimizerOptions.incrementalize = incremental;
        options.optimizerOptions.optimizationLevel = optimize ? 2 : 1;
        return options;
    }

    protected static DBSPCompiler testCompiler(boolean incremental, boolean optimize, boolean jit) {
        return new DBSPCompiler(testOptions(incremental, optimize, jit));
    }

    protected static DBSPCompiler testCompiler() {
        return testCompiler(false, true, false);
    }

    public DBSPCompiler compileQuery(String query, boolean incremental, boolean optimize, boolean jit) {
        DBSPCompiler compiler = testCompiler(incremental, optimize, jit);
        // This is necessary if we want queries that do not depend on the input
        // to generate circuits that still have inputs.
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ", COL5 INT" +
                ", COL6 DOUBLE" +
                ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        return compiler;
    }

    public static final DBSPTupleExpression e0 = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPDoubleLiteral(12.0),
            DBSPBoolLiteral.TRUE,
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32),
            DBSPLiteral.none(DBSPTypeDouble.NULLABLE_INSTANCE)
    );
    public static final DBSPTupleExpression e1 = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPDoubleLiteral(1.0),
            DBSPBoolLiteral.FALSE,
            new DBSPStringLiteral("Hi"),
            new DBSPI32Literal(1, true),
            new DBSPDoubleLiteral(0.0, true)
    );

    public static final DBSPTupleExpression e0NoDouble = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            DBSPBoolLiteral.TRUE,
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true))
    );
    public static final DBSPTupleExpression e1NoDouble = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            DBSPBoolLiteral.FALSE,
            new DBSPStringLiteral("Hi"),
            new DBSPI32Literal(1, true)
    );
    static final DBSPZSetLiteral.Contents z0 = new DBSPZSetLiteral.Contents(e0);
    static final DBSPZSetLiteral.Contents z1 = new DBSPZSetLiteral.Contents(e1);
    static final DBSPZSetLiteral.Contents empty = DBSPZSetLiteral.Contents.emptyWithElementType(z0.getElementType());

    /**
     * Returns the table containing:
     * -------------------------------------------
     * | 10 | 12.0 | true  | Hi | NULL    | NULL |
     * | 10 |  1.0 | false | Hi | Some[1] |  0.0 |
     * -------------------------------------------
     */
    DBSPZSetLiteral.Contents createInput() {
        return new DBSPZSetLiteral.Contents(e0, e1);
    }
}
