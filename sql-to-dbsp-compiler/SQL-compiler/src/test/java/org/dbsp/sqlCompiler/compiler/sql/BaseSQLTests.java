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
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputPair;
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
    public static int jitTestsExecuted = 0;

    /**
     * Collect here all the tests to run and execute them using a single Rust compilation.
     */
    static final List<TestCase> testsToRun = new ArrayList<>();

    @BeforeClass
    public static void prepareTests() {
        testsToRun.clear();
    }

    static File createInputScript(String... contents) throws IOException {
        File result = File.createTempFile("script", ".sql", new File(rustDirectory));
        result.deleteOnExit();
        PrintWriter script = new PrintWriter(result, "UTF-8");
        script.println(String.join(";" + System.lineSeparator(), contents));
        script.close();
        return result;
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
        RustFileWriter writer = new RustFileWriter(firstCompiler, outputStream);
        int testNumber = 0;
        String[] extraArgs = new String[0];
        for (TestCase test: testsToRun) {
            if (!test.compiler.options.same(firstCompiler.options))
                throw new RuntimeException("Tests are not compiled with the same options: "
                        + test.compiler.options + " and " + firstCompiler.options);
            ProgramAndTester pt;
            if (test.compiler.options.ioOptions.jit) {
                pt = new ProgramAndTester(null, test.createJITTesterCode(testNumber));
                if (extraArgs.length == 0) {
                    extraArgs = new String[2];
                    extraArgs[0] = "--features";
                    extraArgs[1] = "jit";
                }
                jitTestsExecuted++;
            } else {
                // Standard test
                pt = new ProgramAndTester(test.circuit, test.createTesterCode(testNumber, rustDirectory));
                testsExecuted++;
            }
            writer.add(pt);
            testNumber++;
        }
        writer.writeAndClose();
        Utilities.compileAndTestRust(rustDirectory, true, extraArgs);
        testsToRun.clear();
    }

    protected void addRustTestCase(String name, DBSPCompiler compiler, DBSPCircuit circuit, InputOutputPair... streams) {
        compiler.messages.show(System.err);
        compiler.messages.clear();
        if (this.currentTestInformation.startsWith("Jit") &&
            !compiler.options.ioOptions.jit) {
            // Got confused on setting options a few times, this will help catch such mistakes
            throw new RuntimeException("JIT test " + this.currentTestInformation +
                    " without JIT compiler option?" + compiler.options);
        }
        TestCase test = new TestCase(name, this.currentTestInformation, compiler, circuit, streams);
        testsToRun.add(test);
    }

    /**
     * Create CompilerOptions according to the specified properties.
     * @param incremental  Generate an incremental program if true.
     * @param optimize     Optimize program if true.
     * @param jit          Generate code for the JIT if true.
     */
    public CompilerOptions testOptions(boolean incremental, boolean optimize, boolean jit) {
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.throwOnError = true;
        options.languageOptions.generateInputForEveryTable = true;
        options.ioOptions.jit = jit;
        options.ioOptions.quiet = true;
        options.languageOptions.incrementalize = incremental;
        options.languageOptions.optimizationLevel = optimize ? 2 : 1;
        return options;
    }

    /**
     * Return the default compiler used for testing.
     */
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(false, true, false);
        return new DBSPCompiler(options);
    }

    public static DBSPCircuit getCircuit(DBSPCompiler compiler) {
        compiler.optimize();
        String name = "circuit" + testsToRun.size();
        return compiler.getFinalCircuit(name);
    }
}
