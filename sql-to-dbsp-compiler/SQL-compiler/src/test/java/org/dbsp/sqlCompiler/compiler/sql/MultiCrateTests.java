package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.SQLException;

public class MultiCrateTests extends BaseSQLTests {
    void compileMultiCrate(String file) throws SQLException, IOException, InterruptedException {
        CompilerMessages messages = CompilerMain.execute(
                "-i", "--alltables", "-q", "--ignoreOrder", "--crates",
                "-o", BaseSQLTests.RUST_CRATES_DIRECTORY, file);
        messages.print();
        Assert.assertEquals(0, messages.errorCount());
        Utilities.compileAndCheckRust(BaseSQLTests.RUST_CRATES_DIRECTORY, true);
    }

    @Test
    public void testMultiCrate() throws IOException, SQLException, InterruptedException {
        String sql = """
                 CREATE TABLE T (C0 INT NOT NULL, C1 DOUBLE NOT NULL, C2 INT, C3 INT LATENESS 2, C4 INT, C5 INT);
                 CREATE VIEW V0 AS SELECT 'x', STDDEV(C1) FROM T;
                 CREATE VIEW V1 AS SELECT * FROM T JOIN T AS R ON T.C0 = R.C3;""";
        File file = createInputScript(sql);
        this.compileMultiCrate(file.getAbsolutePath());
    }

    @Test
    public void testMultiCrateRecursive() throws IOException, SQLException, InterruptedException {
        String sql = """
                DECLARE RECURSIVE VIEW V(v INT);
                CREATE VIEW V AS SELECT v FROM V UNION SELECT 1;""";
        File file = createInputScript(sql);
        this.compileMultiCrate(file.getAbsolutePath());
    }

    @Test
    public void testCmp() throws IOException, SQLException, InterruptedException {
        String sql = """
            CREATE TABLE T (
            COL1 INT NOT NULL
            , COL2 DOUBLE PRECISION NOT NULL
            , COL3 BOOLEAN NOT NULL
            , COL4 VARCHAR NOT NULL
            , COL5 INT
            , COL6 DECIMAL(6, 2));
            CREATE VIEW V AS SELECT T.COL1, LAG(T.COL1) OVER (ORDER BY T.COL1) FROM T;""";
        File file = createInputScript(sql);
        this.compileMultiCrate(file.getAbsolutePath());
    }

    @Test @Ignore
    public void testMultiCrateLarge() throws IOException, SQLException, InterruptedException {
        File file = new File("../extra/current_pipeline.sql");
        this.compileMultiCrate(file.getAbsolutePath());
    }

    @Test
    public void testMultiUdf() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value INTEGER) RETURNS BOOLEAN NOT NULL;
                CREATE VIEW V0 AS SELECT contains_number(CAST('YES: 10 NO:5 MAYBE: 2' AS VARCHAR), 5);""");

        File udf = Paths.get(BaseSQLTests.RUST_CRATES_DIRECTORY, "globals", "src", "udf.rs").toFile();
        udf.deleteOnExit();
        PrintWriter udfFile = new PrintWriter(udf, StandardCharsets.UTF_8);
        udfFile.println("""
                use feldera_sqllib::*;
                pub fn contains_number(str: SqlString, value: Option<i32>) -> Result<bool, Box<dyn std::error::Error>> {
                   match value {
                       None => Err("null value".into()),
                       Some(value) => Ok(str.str().contains(&format!("{}", value).to_string())),
                   }
                }""");
        udfFile.close();
        this.compileMultiCrate(file.getAbsolutePath());
    }
}
