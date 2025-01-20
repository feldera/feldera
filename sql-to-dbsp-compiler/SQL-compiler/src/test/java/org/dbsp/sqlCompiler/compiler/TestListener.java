package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

/** Invoked when all tests are finished. */
public class TestListener extends RunListener {
    long startTime = 0;

    @Override
    public void testRunStarted(Description description) {
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void testRunFinished(Result result) {
        long end = System.currentTimeMillis();
        System.out.println("Executed " + BaseSQLTests.testsExecuted +
                ", checked " + BaseSQLTests.testsChecked + " Rust tests in " +
                (end - this.startTime)/1000 + "s");
    }
}
