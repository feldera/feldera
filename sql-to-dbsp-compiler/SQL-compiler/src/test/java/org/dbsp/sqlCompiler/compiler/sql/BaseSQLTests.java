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

package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.sql.simple.Change;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputChangeStream;
import org.dbsp.util.ProgramAndTester;
import org.dbsp.util.Utilities;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for SQL-based tests.
 */
public class BaseSQLTests {
    @Rule
    public TestName currentTestName = new TestName();

    String currentTestInformation = "";

    @Rule
    public TestWatcher testWatcher = new TestWatcher() {
        @Override
        protected void starting(final Description description) {
            String methodName = description.getMethodName();
            String className = description.getClassName();
            className = className.substring(className.lastIndexOf('.') + 1);
            BaseSQLTests.this.currentTestInformation  = className + "#" + methodName;
        }
    };

    public static final String projectDirectory = "..";
    public static final String rustDirectory = projectDirectory + "/temp/src";
    public static final String testFilePath = rustDirectory + "/lib.rs";

    public static int testsExecuted = 0;

    /**
     * Collect here all the tests to run and execute them using a single Rust compilation.
     */
    static final List<TestCase> testsToRun = new ArrayList<>();

    @BeforeClass
    public static void prepareTests() {
        testsToRun.clear();
    }

    public static File createInputFile(File file, String separator, String... contents) throws IOException {
        file.deleteOnExit();
        PrintWriter script = new PrintWriter(file, StandardCharsets.UTF_8);
        script.println(String.join(separator, contents));
        script.close();
        return file;
    }

    public static File createInputScript(String... contents) throws IOException {
        File result = File.createTempFile("script", ".sql", new File(rustDirectory));
        return createInputFile(result, ";" + System.lineSeparator(), contents);
    }

    DBSPCompiler noThrowCompiler() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        return compiler;
    }

    public void testNegativeQuery(String query, String messageFragment) {
        DBSPCompiler compiler = this.noThrowCompiler();
        compiler.compileStatement("CREATE VIEW VV AS " + query);
        Assert.assertTrue(compiler.messages.exitCode != 0);
        String message = compiler.messages.toString();
        Assert.assertTrue(message.contains(messageFragment));
    }

    /**
     * Runs all the tests from the testsToRun list.
     */
    @AfterClass
    public static void runAllTests() throws IOException, InterruptedException {
        if (testsToRun.isEmpty())
            return;
        PrintStream outputStream = new PrintStream(Files.newOutputStream(Paths.get(testFilePath)));
        // Use the compiler from the first test case.
        DBSPCompiler firstCompiler = testsToRun.get(0).compiler;
        RustFileWriter writer = new RustFileWriter(outputStream);
        int testNumber = 0;
        String[] extraArgs = new String[0];
        for (TestCase test: testsToRun) {
            if (!test.compiler.options.same(firstCompiler.options))
                throw new RuntimeException("Tests are not compiled with the same options: "
                        + test.compiler.options + " and " + firstCompiler.options);
            ProgramAndTester pt;
            // Standard test
            pt = new ProgramAndTester(test.circuit, test.createTesterCode(testNumber, rustDirectory));
            testsExecuted++;
            writer.add(pt);
            testNumber++;
        }
        writer.writeAndClose(firstCompiler);
        Utilities.compileAndTestRust(rustDirectory, true, extraArgs);
        testsToRun.clear();
    }

    protected void addRustTestCase(
            String name, DBSPCompiler compiler, DBSPCircuit circuit, InputOutputChangeStream streams) {
        compiler.messages.show(System.err);
        compiler.messages.clear();
        TestCase test = new TestCase(name, this.currentTestInformation, compiler, circuit, null, streams);
        testsToRun.add(test);
    }

    protected void addRustTestCase(
            String name, DBSPCompiler compiler, DBSPCircuit circuit) {
        this.addRustTestCase(name, compiler, circuit, new InputOutputChangeStream());
    }

    /** Add a test case without inputs */
    public void compileRustTestCase(String statements) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(statements);
        Assert.assertFalse(compiler.hasErrors());
        this.addRustTestCase(this.currentTestInformation, compiler, getCircuit(compiler), new InputOutputChangeStream());
    }

    protected void addFailingRustTestCase(
            String name, String message, DBSPCompiler compiler, DBSPCircuit circuit, InputOutputChangeStream streams) {
        compiler.messages.show(System.err);
        compiler.messages.clear();
        TestCase test = new TestCase(name, this.currentTestInformation, compiler, circuit, message, streams);
        testsToRun.add(test);
    }

    /**
     * Create CompilerOptions according to the specified properties.
     * @param incremental  Generate an incremental program if true.
     * @param optimize     Optimize program if true.
     */
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.throwOnError = true;
        options.languageOptions.generateInputForEveryTable = true;
        options.ioOptions.quiet = true;
        options.ioOptions.emitHandles = true;
        options.languageOptions.incrementalize = incremental;
        options.languageOptions.optimizationLevel = optimize ? 2 : 1;
        return options;
    }

    /** Return the default compiler used for testing. */
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(false, true);
        return new DBSPCompiler(options);
    }

    public static DBSPCircuit getCircuit(DBSPCompiler compiler) {
        compiler.optimize();
        String name = "circuit" + testsToRun.size();
        return compiler.getFinalCircuit(name);
    }

    protected InputOutputChangeStream emptyInputOutputChangeStream() {
        return new InputOutputChangeStream().addChange(
                new InputOutputChange(new Change(), new Change()));
    }

    protected void runtimeFail(String query, String message, InputOutputChangeStream data) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(query);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addFailingRustTestCase(query, message, compiler, circuit, data);
    }

    /** Run a test that fails at runtime without needing any inputs */
    protected void runtimeConstantFail(String query, String message) {
        this.runtimeFail(query, message, this.emptyInputOutputChangeStream());
    }
}
