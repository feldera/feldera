package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/** Compile the SQL programs in the feldera-qa repository, and rust-compile the results. */
public class QATests {
    static void compileAndCheck(File file) throws SQLException, IOException, InterruptedException {
        CompilerMessages messages = CompilerMain.execute(
                "-i", "--alltables", "--ignoreOrder",
                "-o", BaseSQLTests.TEST_FILE_PATH, file.getAbsolutePath());
        if (messages.errorCount() > 0 || messages.warningCount() > 0) {
            messages.print();
            if (messages.errorCount() > 0)
                throw new RuntimeException("Error during compilation");
        }
        BaseSQLTests.compileAndCheckRust(true);
    }

    @Test
    public void qaTests() throws IOException, SQLException, InterruptedException {
        for (File c : BaseSQLTests.getQATests()) {
            // This program cannot be compiled because it contains a udf
            if (c.toString().matches(".*swiss.*-q1.*")) continue;
            System.out.println("Compiling " + c);
            try {
                compileAndCheck(c);
            } catch (UnsupportedException ex) {
                // This is probably a file containing an ad-hoc query
                System.out.println(c.getName() + " skipped due to unsupported features.");
            }
        }
    }
}
