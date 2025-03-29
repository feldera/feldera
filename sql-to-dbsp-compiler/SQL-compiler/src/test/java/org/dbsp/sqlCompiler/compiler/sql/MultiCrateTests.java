package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.MultiCrates;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

public class MultiCrateTests extends BaseSQLTests {
    void compileMultiCrate(String file) throws SQLException, IOException, InterruptedException {
        CompilerMessages messages = CompilerMain.execute(
                "-i", "--alltables", "-q", "--ignoreOrder", "--crates", "x",
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
    public void collisionTest() throws IOException, SQLException, InterruptedException {
        String sql = """
                CREATE TABLE int0_tbl(
                id INT NOT NULL,
                c1 TINYINT,
                c2 TINYINT NOT NULL,
                c3 INT2,
                c4 INT2 NOT NULL,
                c5 INT,
                c6 INT NOT NULL,
                c7 BIGINT,
                c8 BIGINT NOT NULL);
                
                CREATE MATERIALIZED VIEW int_array_agg AS SELECT
                ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
                FROM int0_tbl;
                
                CREATE MATERIALIZED VIEW int_array_agg_where AS SELECT
                ARRAY_AGG(c1) FILTER(WHERE (c5+C6)> 3) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE (c5+C6)> 3) AS f_c2, ARRAY_AGG(c3) FILTER(WHERE (c5+C6)> 3) AS f_c3, ARRAY_AGG(c4) FILTER(WHERE (c5+C6)> 3) AS f_c4, ARRAY_AGG(c5) FILTER(WHERE (c5+C6)> 3) AS f_c5, ARRAY_AGG(c6) FILTER(WHERE (c5+C6)> 3) AS f_c6,  ARRAY_AGG(c7) FILTER(WHERE (c5+C6)> 3) AS f_c7,  ARRAY_AGG(c8) FILTER(WHERE (c5+C6)> 3) AS f_c8
                FROM int0_tbl;""";
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
    public void testJoin() throws IOException, SQLException, InterruptedException {
        String sql = """
                CREATE TABLE t1 (
                    val BIGINT
                );
                
                CREATE VIEW v1 AS SELECT COUNT(*) FROM t1
                    LEFT JOIN t1 AS t2 ON t1.val=t2.val
                    LEFT JOIN t1 AS t3 ON t2.val=t3.val
                    LEFT JOIN t1 AS t4 ON t3.val=t4.val
                    LEFT JOIN t1 AS t5 ON t4.val=t5.val
                    LEFT JOIN t1 AS t6 ON t5.val=t6.val
                    LEFT JOIN t1 AS t7 ON t6.val=t7.val
                    LEFT JOIN t1 AS t8 ON t7.val=t8.val
                    LEFT JOIN t1 AS t9 ON t8.val=t9.val
                    LEFT JOIN t1 AS t10 ON t9.val=t10.val
                    LEFT JOIN t1 AS t11 ON t10.val=t11.val
                    LEFT JOIN t1 AS t12 ON t11.val=t12.val
                    LEFT JOIN t1 AS t13 ON t12.val=t13.val
                    LEFT JOIN t1 AS t14 ON t13.val=t14.val
                    LEFT JOIN t1 AS t15 ON t14.val=t15.val
                    LEFT JOIN t1 AS t16 ON t15.val=t16.val
                    LEFT JOIN t1 AS t17 ON t16.val=t17.val
                    LEFT JOIN t1 AS t18 ON t17.val=t18.val
                    LEFT JOIN t1 AS t19 ON t18.val=t19.val
                    LEFT JOIN t1 AS t20 ON t19.val=t20.val
                    LEFT JOIN t1 AS t21 ON t20.val=t21.val;""";
        File file = createInputScript(sql);
        this.compileMultiCrate(file.getAbsolutePath());
    }

    @Test
    public void testPackagedDemos() throws SQLException, IOException, InterruptedException {
        final String projectsDirectory = "../../demo/packaged/sql";
        final File dir = new File(projectsDirectory);
        FilenameFilter filter = (_d, name) -> !name.contains("setup") && name.endsWith(".sql");
        String[] sqlFiles = dir.list(filter);
        assert sqlFiles != null;
        for (String sqlFile: sqlFiles) {
            // System.out.println(sqlFile);
            String basename = Utilities.getBaseName(sqlFile);
            String udf = basename + ".udf.rs";
            File udfFile = new File(dir.getPath() + "/" + udf);
            if (!udfFile.exists())
                this.compileMultiCrate(dir.getPath() + "/" + sqlFile);
        }
    }

    @Test
    public void testAsof() throws IOException, SQLException, InterruptedException {
        String sql = """
                create table TRANSACTION (
                    id bigint NOT NULL,
                    unix_time BIGINT LATENESS 100
                );

                create table FEEDBACK (
                    id bigint,
                    status int,
                    unix_time bigint NOT NULL LATENESS 100
                );

                CREATE VIEW TRANSACT AS
                    SELECT transaction.*, feedback.status
                    FROM
                    feedback LEFT ASOF JOIN transaction
                    MATCH_CONDITION(transaction.unix_time <= feedback.unix_time)
                    ON transaction.id = feedback.id;
                """;
        File file = createInputScript(sql);
        this.compileMultiCrate(file.getAbsolutePath());
    }

    @Test
    public void testMultiUdf() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value INTEGER) RETURNS BOOLEAN NOT NULL;
                CREATE VIEW V0 AS SELECT contains_number(CAST('YES: 10 NO:5 MAYBE: 2' AS VARCHAR), 5);""");

        // "x" is the name for the pipeline used by compileMultiCrate
        Path dir = Paths.get(BaseSQLTests.RUST_CRATES_DIRECTORY, MultiCrates.FILE_PREFIX + "x_globals", "src");
        File dirFile = dir.toFile();
        if (!dirFile.exists()) {
            boolean success = dirFile.mkdirs();
            if (!success) {
                throw new RuntimeException("Could not create directory " + dir);
            }
        }
        File udf = new File(dir.toFile(), "udf.rs");
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
        udf.delete();
    }

    @Test
    public void testMultiUdfUdt() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE TYPE X AS (a0 int, a1 int, a2 int, a3 int, a4 int, a5 int,
                                  a6 int, a7 int, a8 int, a9 int, a10 int, a11 int);
                CREATE FUNCTION f(arg X) RETURNS ROW(x int NULL) NULL;
                CREATE FUNCTION g(x int NOT NULL) RETURNS ROW(a INT, b INT) NOT NULL;
                CREATE VIEW V AS SELECT f(X(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)), g(2).a;""");

        // "x" is the name for the pipeline used by compileMultiCrate
        Path dir = Paths.get(BaseSQLTests.RUST_CRATES_DIRECTORY, MultiCrates.FILE_PREFIX + "x_globals", "src");
        File dirFile = dir.toFile();
        if (!dirFile.exists()) {
            boolean success = dirFile.mkdirs();
            if (!success) {
                throw new RuntimeException("Could not create directory " + dir);
            }
        }
        File udf = new File(dir.toFile(), "udf.rs");
        PrintWriter udfFile = new PrintWriter(udf, StandardCharsets.UTF_8);
        udfFile.println("""
                use crate::{Tup1, Tup2, Tup12};
                use feldera_sqllib::*;
                pub fn f(x: Option<Tup12<Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<i32>,
                                         Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<i32>>>) ->
                   Result<Option<Tup1<Option<i32>>>, Box<dyn std::error::Error>> {
                   match x {
                      None => Ok(None),
                      Some(x) => match x.0 {
                         None => Ok(Some(Tup1::new(None))),
                         Some(x) => Ok(Some(Tup1::new(Some(x + 1)))),
                      }
                   }
                }
                
                pub fn g(x: i32) -> Result<Tup2<i32, i32>, Box<dyn std::error::Error>> {
                   Ok(Tup2::new(x-1, x+1))
                }""");
        udfFile.close();
        this.compileMultiCrate(file.getAbsolutePath());
        udf.delete();
    }
}
