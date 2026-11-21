package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.DiagnosticExporter;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

/** Invoked when all tests are finished. */
public class TestListener extends RunListener {
    long startTime = 0;

    @Override
    public void testRunStarted(Description description) throws Exception {
        this.startTime = System.currentTimeMillis();
        DiagnosticExporter.start();
    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        DiagnosticExporter.finish(result);
        long end = System.currentTimeMillis();
        System.out.println("Executed " + BaseSQLTests.testsExecuted +
                ", checked " + BaseSQLTests.testsChecked + " Rust tests in " +
                (end - this.startTime)/1000 + "s");
    }

    @Override
    public void testStarted(Description description) {
        DiagnosticExporter.testStarted(description);
    }

    @Override
    public void testFinished(Description description) {
        DiagnosticExporter.testFinished();
    }
}
