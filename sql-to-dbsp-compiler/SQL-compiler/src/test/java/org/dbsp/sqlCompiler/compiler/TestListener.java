package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

/** Invoked when all tests are finished. */
public class TestListener extends RunListener {
    @Override
    public void testRunFinished(Result result) {
        System.out.println("Executed " + BaseSQLTests.testsExecuted + " Rust tests");
    }
}
