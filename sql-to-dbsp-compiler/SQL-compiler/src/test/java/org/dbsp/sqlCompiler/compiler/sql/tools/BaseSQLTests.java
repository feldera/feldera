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

package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LowerCircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MonotoneAnalyzer;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Base class for SQL-based tests. */
public class BaseSQLTests {
    /** Override this method to prepare the tables on
     * which the tests are built. */
    public void prepareInputs(DBSPCompiler compiler) {}

    public CompilerCircuitStream getCCS(String sql) {
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);
        compiler.submitStatementsForCompilation(sql);
        return new CompilerCircuitStream(compiler, this);
    }

    public CompilerCircuitStream getCCS(DBSPCompiler compiler) {
        return new CompilerCircuitStream(compiler, this);
    }

    public CompilerCircuit getCC(String sql) {
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);
        compiler.submitStatementsForCompilation(sql);
        return new CompilerCircuit(compiler);
    }

    public CompilerCircuitStream getCCS(String sql, List<String> inputs, List<String> outputs) {
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);
        compiler.submitStatementsForCompilation(sql);
        return new CompilerCircuitStream(compiler, inputs, outputs, this);
    }

    @SuppressWarnings("unused")
    protected void showPlan() {
        Logger.INSTANCE.setLoggingLevel(SqlToRelCompiler.class, 2);
    }

    @SuppressWarnings("unused")
    protected void instrumentApply() {
        Logger.INSTANCE.setLoggingLevel(LowerCircuitVisitor.class, 2);
    }

    @SuppressWarnings("unused")
    protected void showMonotone() {
        Logger.INSTANCE.setLoggingLevel(MonotoneAnalyzer.class, 2);
    }

    @SuppressWarnings("unused")
    protected void showFinal() {
        Logger.INSTANCE.setLoggingLevel(DBSPCompiler.class, 1);
    }

    @SuppressWarnings("unused")
    protected void showFinalVerbose(int verbosity) {
        Logger.INSTANCE.setLoggingLevel(DBSPCompiler.class, verbosity);
    }

    /** Run a query that is expected to fail in compilation.
     * @param query             Query to run.
     * @param messageFragment   This fragment should appear in the error message. */
    public void queryFailingInCompilation(String query, String messageFragment) {
        this.statementsFailingInCompilation("CREATE VIEW VV AS " + query, messageFragment);
    }

    void shouldMatch(String messages, String regex, boolean isRegex) {
        if (isRegex) {
            Pattern pattern = Pattern.compile(".*" + regex + ".*", Pattern.DOTALL);
            Matcher matcher = pattern.matcher(messages);
            if (!matcher.matches())
                Assert.fail("Error message\n" + Utilities.singleQuote(messages) +
                        "\ndoes not contain the expected fragment\n" +
                        Utilities.singleQuote(regex));
        } else {
            if (!messages.contains(regex))
                Assert.fail("Error message\n" + Utilities.singleQuote(messages) +
                        "\ndoes not contain the expected fragment\n" +
                        Utilities.singleQuote(regex));
        }
    }

    /** Run one or more statements that are expected to fail in compilation.
     * @param statements Statements to compile.
     * @param regex      Regular expression that should match the error message. */
    public void statementsFailingInCompilation(String statements, String regex, boolean isRegex) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        this.prepareInputs(compiler);
        compiler.submitStatementsForCompilation(statements);
        compiler.getFinalCircuit(true);
        if (compiler.messages.exitCode == 0) {
            throw new RuntimeException("Program was expected to fail: " + statements);
        } else {
            String message = compiler.messages.toString();
            this.shouldMatch(message, regex, isRegex);
        }
    }

    public void statementsFailingInCompilation(String statements, String substring) {
        this.statementsFailingInCompilation(statements, substring, false);
    }

    /** Compile a set of statements that are expected to give a warning at compile time.
     * @param statements  Statement to run.
     * @param regex       This regular expression should match the warning message. */
    public void shouldWarn(String statements, String regex) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        this.prepareInputs(compiler);
        compiler.submitStatementsForCompilation(statements);
        getCircuit(compiler);
        Assert.assertTrue(compiler.hasWarnings);
        String warnings = compiler.messages.messages.stream()
                .filter(error -> error.warning).toList().toString();
        this.shouldMatch(warnings, regex, true);
    }

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

    public static final String PROJECT_DIRECTORY = "..";
    public static final String RUST_DIRECTORY = PROJECT_DIRECTORY + "/temp/src";
    public static final String RUST_CRATES_DIRECTORY = PROJECT_DIRECTORY + "/multi";
    public static final String TEST_FILE_PATH = RUST_DIRECTORY + "/lib.rs";

    public static int testsExecuted = 0;
    public static int testsChecked = 0;

    /** Collect here all the tests to run and execute them using a single Rust compilation. */
    static final List<TestCase> testsToRun = new ArrayList<>();

    @BeforeClass
    public static void prepareTests() {
        testsToRun.clear();
    }

    public static File createInputFile(File file, String contents) throws IOException {
        file.deleteOnExit();
        PrintWriter script = new PrintWriter(file, StandardCharsets.UTF_8);
        script.println(contents);
        script.close();
        return file;
    }

    public static File createInputScript(String contents) throws IOException {
        File result = File.createTempFile("script", ".sql", new File(RUST_DIRECTORY));
        return createInputFile(result, contents);
    }

    public static void createEmptyStubs() {
        try {
            PrintStream outputStream = new PrintStream(Files.newOutputStream(Paths.get(RUST_DIRECTORY, DBSPCompiler.STUBS_FILE_NAME)));
            outputStream.println();
            outputStream.close();
        } catch (IOException ignored) {}
    }

    /** Runs all the tests from the testsToRun list. */
    @AfterClass
    public static void runAllTests() throws IOException, InterruptedException {
        if (testsToRun.isEmpty())
            return;
        int testNumber = 0;

        List<TestCase> toRun = Linq.where(testsToRun, TestCase::hasData);
        List<TestCase> toCheck = Linq.where(testsToRun, testCase -> !testCase.hasData());

        if (!toRun.isEmpty()) {
            PrintStream outputStream = new PrintStream(Files.newOutputStream(Paths.get(TEST_FILE_PATH)));
            RustFileWriter writer = new RustFileWriter(outputStream);
            createEmptyStubs();
            // Use the compiler from the first test case.
            DBSPCompiler firstCompiler = null;
            for (TestCase test : toRun) {
                if (firstCompiler == null)
                    firstCompiler = test.ccs.compiler;
                if (!test.ccs.compiler.options.same(firstCompiler.options))
                    throw new RuntimeException("Test " + Utilities.singleQuote(toRun.get(0).javaTestName) +
                            " and " + Utilities.singleQuote(test.javaTestName) +
                            " are not compiled with the same options: "
                            + test.ccs.compiler.options.diff(firstCompiler.options));
                test.ccs.circuit.setName("circuit" + testNumber);
                ProgramAndTester pt = new ProgramAndTester(test.ccs.circuit, test.createTesterCode(testNumber, RUST_DIRECTORY));
                BaseSQLTests.testsExecuted++;
                testNumber++;
                // Filter here tests
                // if (pt.program() != null && !pt.program().toString().contains("join")) continue;
                writer.add(pt);
            }
            assert firstCompiler != null;
            writer.writeAndClose(firstCompiler);
            Utilities.compileAndTestRust(RUST_DIRECTORY, true);
        }

        if (!toCheck.isEmpty()) {
            createEmptyStubs();
            PrintStream outputStream = new PrintStream(Files.newOutputStream(Paths.get(TEST_FILE_PATH)));
            RustFileWriter writer = new RustFileWriter(outputStream);
            DBSPCompiler firstCompiler = null;
            for (TestCase test : toCheck) {
                if (firstCompiler == null)
                    firstCompiler = test.ccs.compiler;
                if (!test.ccs.compiler.options.same(firstCompiler.options))
                    throw new RuntimeException("Test " + Utilities.singleQuote(toCheck.get(0).javaTestName) +
                            " and " + Utilities.singleQuote(test.javaTestName) +
                            " are not compiled with the same options: "
                            + test.ccs.compiler.options.diff(firstCompiler.options));
                test.ccs.circuit.setName("circuit" + testNumber);
                ProgramAndTester pt = new ProgramAndTester(test.ccs.circuit, test.createTesterCode(testNumber, RUST_DIRECTORY));
                BaseSQLTests.testsChecked++;
                testNumber++;
                // Filter here tests
                // if (pt.program() != null && !pt.program().toString().contains(".flatmap")) continue;
                writer.add(pt);
            }
            assert firstCompiler != null;
            writer.writeAndClose(firstCompiler);
            Utilities.compileAndCheckRust(RUST_DIRECTORY, true);
        }
        testsToRun.clear();
    }

    void addRustTestCase(CompilerCircuitStream ccs) {
        ccs.compiler.messages.show(System.err);
        if (ccs.compiler.messages.exitCode != 0)
            throw new RuntimeException("Compilation failed");
        ccs.compiler.messages.clear();
        TestCase test = new TestCase(this.currentTestInformation, this.currentTestInformation, ccs, null);
        testsToRun.add(test);
    }

    /** Add a test case without inputs */
    public void compileRustTestCase(String statements) {
        this.getCCS(statements);
    }

    protected void addFailingRustTestCase(String message, CompilerCircuitStream ccs) {
        ccs.showErrors();
        TestCase test = new TestCase(this.currentTestInformation, this.currentTestInformation, ccs, message);
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
        options.ioOptions.verbosity = 1;
        options.languageOptions.incrementalize = incremental;
        options.languageOptions.unrestrictedIOTypes = true;
        options.languageOptions.optimizationLevel = optimize ? 2 : 1;
        return options;
    }

    /** Return the default compiler used for testing. */
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(false, true);
        return new DBSPCompiler(options);
    }

    public static DBSPCircuit getCircuit(DBSPCompiler compiler) {
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        if (circuit == null) {
            compiler.showErrors(System.err);
            throw new RuntimeException("No circuit produced");
        }
        return circuit;
    }

    protected InputOutputChangeStream streamWithEmptyChanges() {
        return new InputOutputChangeStream().addChange(
                new InputOutputChange(new Change(), new Change()));
    }

    protected void runtimeFail(String query, String message, InputOutputChangeStream data) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementForCompilation(query);
        new CompilerCircuitStream(compiler, data, this, message);
    }

    /** Run a test that fails at runtime without needing any inputs */
    protected void runtimeConstantFail(String query, String message) {
        this.runtimeFail(query, message, this.streamWithEmptyChanges());
    }

    protected CompilerCircuitStream getCCS(DBSPCompiler compiler, InputOutputChangeStream streams) {
        return new CompilerCircuitStream(compiler, streams, this);
    }

    protected CompilerCircuitStream getCCSFailing(DBSPCompiler compiler, String message) {
        return new CompilerCircuitStream(compiler, this, message);
    }
}
