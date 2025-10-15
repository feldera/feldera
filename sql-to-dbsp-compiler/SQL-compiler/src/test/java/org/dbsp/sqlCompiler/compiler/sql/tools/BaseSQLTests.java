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
import org.dbsp.sqlCompiler.compiler.backend.rust.RustWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.StubsWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.MultiCratesWriter;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LowerCircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MonotoneAnalyzer;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Base class for SQL-based tests. */
public class BaseSQLTests {
    // Debugging: set to true to only compile SQL
    public static final boolean skipRust = false;
    // Debugging: set to only accept some tests
    public static final Predicate<CompilerCircuitStream> acceptTest = x -> true;

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
        Assert.assertTrue("Expected some warnings; got none", compiler.hasWarnings);
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
        SqlIoTest.cachedChangeList.clear();
    }

    public static Path getTestFilePath() {
        return Paths.get(TEST_FILE_PATH);
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
        Utilities.createEmptyFile(Paths.get(RUST_DIRECTORY, DBSPCompiler.STUBS_FILE_NAME));
    }

    /** Runs all the tests from the testsToRun list. */
    @AfterClass
    public static void runAllTests() throws IOException, InterruptedException {
        if (testsToRun.isEmpty())
            return;

        List<TestCase> toRun = Linq.where(testsToRun, TestCase::hasData);
        List<TestCase> toCheck = Linq.where(testsToRun, testCase -> !testCase.hasData());
        DBSPCompiler compiler = testsToRun.get(0).ccs.compiler;

        int testNumber = runTests(toRun, 0, compiler, false);
        runTests(toCheck, testNumber, compiler, true);
        testsToRun.clear();
    }

    static int runTests(List<TestCase> toRun, int testNumber, DBSPCompiler compiler, boolean check)
            throws IOException, InterruptedException {
        if (toRun.isEmpty())
            return testNumber;

        boolean multiCrates = compiler.options.ioOptions.multiCrates();
        HashMap<Long, DBSPFunction> inputFunctions = new HashMap<>();
        PrintStream outputStream = null;
        RustWriter writer;
        String directory;
        Path stubsDir;
        String testCrate = "";

        if (!multiCrates) {
            outputStream = new PrintStream(Files.newOutputStream(getTestFilePath()));
            writer = new RustFileWriter().withTest(true);
            writer.setOutputBuilder(new IndentStream(outputStream));
            directory = RUST_DIRECTORY;
            stubsDir = Paths.get(directory);
            createEmptyStubs();
        } else {
            directory = RUST_CRATES_DIRECTORY;
            MultiCratesWriter multiWriter = new MultiCratesWriter(directory, "x", true);
            testCrate = MultiCratesWriter.getTestName();
            String globals = multiWriter.getGlobalsName();
            writer = multiWriter;
            stubsDir = Paths.get(directory).resolve(globals).resolve("src");
        }
        StubsWriter stubsWriter = new StubsWriter(stubsDir.resolve(DBSPCompiler.STUBS_FILE_NAME));

        // Map from Change id to function that generates the change
        for (TestCase test : toRun) {
            if (!test.ccs.compiler.options.same(compiler.options))
                throw new RuntimeException("Test " + Utilities.singleQuote(toRun.get(0).javaTestName) +
                        " and " + Utilities.singleQuote(test.javaTestName) +
                        " are not compiled with the same options: "
                        + test.ccs.compiler.options.diff(compiler.options));
            test.ccs.circuit.setName("circuit" + testNumber);
            List<DBSPFunction> testers = test.createTesterCode(testNumber, RUST_DIRECTORY, inputFunctions);
            writer.add(test.ccs.circuit);
            if (multiCrates) {
                stubsWriter.add(test.ccs.circuit);
            }
            for (var tester: testers)
                if (acceptTest.test(test.ccs))
                    writer.addTest(tester);
            if (check)
                BaseSQLTests.testsChecked++;
            else
                BaseSQLTests.testsExecuted++;
            testNumber++;
        }
        writer.write(compiler);
        if (outputStream !=null)
            outputStream.close();
        stubsWriter.write(compiler);
        if (!skipRust) {
            if (check) {
                Utilities.compileAndCheckRust(directory, true, testCrate);
            } else {
                String[] extraArgs;
                if (testCrate.isEmpty())
                    extraArgs = new String[] {};
                else {
                    extraArgs = new String[2];
                    extraArgs[0] = "--package";
                    extraArgs[1] = testCrate;
                }
                Utilities.compileAndTestRust(directory, true, extraArgs);
            }
        }

        return testNumber;
    }

    void addRustTestCase(CompilerCircuitStream ccs) {
        ccs.compiler.messages.show(System.err);
        if (ccs.compiler.messages.exitCode != 0)
            throw new RuntimeException("Compilation failed");
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

    public CompilerOptions testOptions() {
        CompilerOptions options = new CompilerOptions();
        if (System.getenv("CI") != null)
            // Set to compile to multiple crates
            options.ioOptions.crates = "x";
        options.languageOptions.throwOnError = true;
        options.languageOptions.generateInputForEveryTable = true;
        options.ioOptions.quiet = true;
        options.ioOptions.emitHandles = true;
        if (!options.ioOptions.multiCrates())
            // Comments can interfere with multi-crates, generating hash collisions
            options.ioOptions.verbosity = 2;
        options.languageOptions.incrementalize = false;
        options.languageOptions.unrestrictedIOTypes = true;
        options.languageOptions.optimizationLevel = 2;
        options.ioOptions.nowStream = true;
        return options;
    }

    /** Return the default compiler used for testing. */
    public final DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions();
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
