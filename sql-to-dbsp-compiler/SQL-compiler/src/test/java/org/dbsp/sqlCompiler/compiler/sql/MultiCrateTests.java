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
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;

public class MultiCrateTests extends BaseSQLTests {
    void compileToMultiCrate(String file, boolean check, boolean noUdfs) throws SQLException, IOException, InterruptedException {
        if (noUdfs) {
            // Make sure there is no stray udf.rs file from a previous test
            Path path = Paths.get(BaseSQLTests.RUST_CRATES_DIRECTORY,
                    MultiCrates.FILE_PREFIX + "x_globals", "src", "udf.rs");
            File udfFile = path.toFile();
            if (udfFile.exists()) {
                boolean deleted = udfFile.delete();
                Assert.assertTrue(deleted);
            }
        }

        CompilerMessages messages = CompilerMain.execute(
                "-i", "--alltables", "-q", "--ignoreOrder", "--crates", "x",
                "-o", BaseSQLTests.RUST_CRATES_DIRECTORY, file);
        if (messages.errorCount() > 0) {
            messages.print();
            throw new RuntimeException("Error during compilation");
        }
        if (check && !BaseSQLTests.skipRust)
            Utilities.compileAndCheckRust(BaseSQLTests.RUST_CRATES_DIRECTORY, true);
    }

    void compileToMultiCrate(String file, boolean check) throws SQLException, IOException, InterruptedException {
        this.compileToMultiCrate(file, check, true);
    }

    @Test
    public void issue4049() throws SQLException, IOException, InterruptedException {
        String sql = """
                CREATE TABLE tbl(c1 INT);
                CREATE MATERIALIZED VIEW v AS
                SELECT c1
                FROM tbl
                ORDER BY c1 ASC
                LIMIT 3;
                CREATE MATERIALIZED VIEW v1 AS
                SELECT c1
                FROM tbl
                ORDER BY c1 NULLS LAST
                LIMIT 3;""";
        File file = createInputScript(sql);
        this.compileToMultiCrate(file.getAbsolutePath(), true);
    }

    @Test
    public void testJsonString() throws IOException, SQLException, InterruptedException {
        String sql = """
                CREATE TYPE T0 AS (n VARCHAR, u VARCHAR);
                CREATE FUNCTION jsonstring_as_t0(line VARCHAR) RETURNS T0;
                CREATE TABLE T(x VARCHAR);
                CREATE VIEW V AS SELECT jsonstring_as_t0(x).n FROM T;""";
        File file = createInputScript(sql);
        this.compileToMultiCrate(file.getAbsolutePath(), true);
    }

    @Test
    public void testMultiCrate() throws IOException, SQLException, InterruptedException {
        String sql = """
                 CREATE TABLE T (C0 INT NOT NULL, C1 DOUBLE NOT NULL, C2 INT, C3 INT LATENESS 2, C4 INT, C5 INT);
                 CREATE VIEW V0 AS SELECT 'x', STDDEV(C1) FROM T;
                 CREATE VIEW V1 AS SELECT * FROM T JOIN T AS R ON T.C0 = R.C3;""";
        File file = createInputScript(sql);
        this.compileToMultiCrate(file.getAbsolutePath(), true);
    }

    @Test
    public void testInterned() throws IOException, SQLException, InterruptedException {
        String sql = """
                 CREATE TABLE T (C0 VARCHAR INTERNED, C1 INT);
                 CREATE VIEW V0 AS SELECT ARG_MAX(C0, C1) FROM T;""";
        File file = createInputScript(sql);
        this.compileToMultiCrate(file.getAbsolutePath(), true);
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
                ARRAY_AGG(c1) AS c1,
                ARRAY_AGG(c2) AS c2,
                ARRAY_AGG(c3) AS c3,
                ARRAY_AGG(c4) AS c4,
                ARRAY_AGG(c5) AS c5,
                ARRAY_AGG(c6) AS c6,
                ARRAY_AGG(c7) AS c7,
                ARRAY_AGG(c8) AS c8
                FROM int0_tbl;

                CREATE MATERIALIZED VIEW int_array_agg_where AS SELECT
                ARRAY_AGG(c1) FILTER(WHERE (c5+C6)> 3) AS f_c1,
                ARRAY_AGG(c2) FILTER(WHERE (c5+C6)> 3) AS f_c2,
                ARRAY_AGG(c3) FILTER(WHERE (c5+C6)> 3) AS f_c3,
                ARRAY_AGG(c4) FILTER(WHERE (c5+C6)> 3) AS f_c4,
                ARRAY_AGG(c5) FILTER(WHERE (c5+C6)> 3) AS f_c5,
                ARRAY_AGG(c6) FILTER(WHERE (c5+C6)> 3) AS f_c6,
                ARRAY_AGG(c7) FILTER(WHERE (c5+C6)> 3) AS f_c7,
                ARRAY_AGG(c8) FILTER(WHERE (c5+C6)> 3) AS f_c8
                FROM int0_tbl;""";
        File file = createInputScript(sql);
        this.compileToMultiCrate(file.getAbsolutePath(), true);
    }

    @Test
    public void testMultiCrateRecursive() throws IOException, SQLException, InterruptedException {
        String sql = """
                DECLARE RECURSIVE VIEW V(v INT);
                CREATE VIEW V AS SELECT v FROM V UNION SELECT 1;""";
        File file = createInputScript(sql);
        this.compileToMultiCrate(file.getAbsolutePath(), true);
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
        this.compileToMultiCrate(file.getAbsolutePath(), true);
    }
   
    @Test @Ignore
    public void testMultiCrateLarge() throws IOException, SQLException, InterruptedException {
        File file = new File("../extra/current_pipeline.sql");
        this.compileToMultiCrate(file.getAbsolutePath(), true);
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
        this.compileToMultiCrate(file.getAbsolutePath(), true);
    }

    @Test
    public void issue4483() throws IOException, SQLException, InterruptedException {
        String sql = """
                CREATE TABLE row_tbl(
                c1 INT NOT NULL,
                c2 VARCHAR,
                c3 VARCHAR)WITH ('append_only' = 'true');
                CREATE MATERIALIZED VIEW row_max AS SELECT
                MAX(ROW(c1, c2, c3)) AS c1
                FROM row_tbl;""";
        File file = createInputScript(sql);
        this.compileToMultiCrate(file.getAbsolutePath(), true);
    }

    @Test
    public void testPackagedDemos() throws SQLException, IOException, InterruptedException {
        // Also tests issue 3903
        final String projectsDirectory = "../../demo/packaged/sql";
        final File dir = new File(projectsDirectory);
        FilenameFilter filter = (_d, name) -> !name.contains("setup") && name.endsWith(".sql");
        String[] sqlFiles = dir.list(filter);
        Assert.assertNotNull(sqlFiles);
        Arrays.sort(sqlFiles);

        for (String sqlFile: sqlFiles) {
            String basename = Utilities.getBaseName(sqlFile);
            String originalUdf = basename + ".udf.rs";
            this.compileToMultiCrate(dir.getPath() + "/" + sqlFile, false);

            File udfFile = new File(dir.getPath() + "/" + originalUdf);
            File udf = null;
            if (udfFile.exists()) {
                String rs = Utilities.readFile(udfFile.getPath());
                udf = this.createUdfFile(rs);

                File cargo = new File(dir.getPath() + "/" + basename + ".udf.toml");
                if (cargo.exists()) {
                    String cargoContents = Utilities.readFile(cargo.getPath());
                    this.appendCargoDependencies(cargoContents);
                }
            }
            if (!BaseSQLTests.skipRust)
                Utilities.compileAndCheckRust(BaseSQLTests.RUST_CRATES_DIRECTORY, true);
            if (udf != null) {
                //noinspection ResultOfMethodCallIgnored
                udf.delete();
            }
        }
    }

    void appendCargoDependencies(String source) throws IOException {
        Path dir = Paths.get(BaseSQLTests.RUST_CRATES_DIRECTORY, MultiCrates.FILE_PREFIX + "x_globals");
        File cargo = new File(dir.toFile(), "Cargo.toml");
        FileWriter writer = new FileWriter(cargo, true);
        writer.append(source);
        writer.close();
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
        this.compileToMultiCrate(file.getAbsolutePath(), true);
    }

    File createUdfFile(String contents) throws IOException {
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
        udfFile.println(contents);
        udfFile.close();
        return udf;
    }

    @Test
    public void testMultiUdf() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE FUNCTION contains_number(str VARCHAR NOT NULL, value INTEGER) RETURNS BOOLEAN NOT NULL;
                CREATE VIEW V0 AS SELECT contains_number(CAST('YES: 10 NO:5 MAYBE: 2' AS VARCHAR), 5);""");

        File udf = this.createUdfFile("""
                use feldera_sqllib::*;
                pub fn contains_number(str: SqlString, value: Option<i32>) -> Result<bool, Box<dyn std::error::Error>> {
                   match value {
                       None => Err("null value".into()),
                       Some(value) => Ok(str.str().contains(&format!("{}", value).to_string())),
                   }
                }""");
        this.compileToMultiCrate(file.getAbsolutePath(), true, false);
        //noinspection ResultOfMethodCallIgnored
        udf.delete();
        // If we interrupt the test
        udf.deleteOnExit();
    }

    @Test
    public void testMultiSqlUdf() throws IOException, InterruptedException, SQLException {
        File file = createInputScript("""
                CREATE FUNCTION decode_boolean(str VARCHAR NOT NULL) RETURNS BOOLEAN NOT NULL AS CAST(str AS BOOLEAN);
                CREATE VIEW V0 AS SELECT decode_boolean(CAST('TRUE' AS VARCHAR));""");
        this.compileToMultiCrate(file.getAbsolutePath(), true);
    }

    @Test
    public void issue4430() throws IOException, SQLException, InterruptedException {
        File file = createInputScript("""
                CREATE TYPE row_type AS(r1 ROW(v11 VARCHAR NOT NULL));
                CREATE TABLE T(c1 row_type);
                CREATE VIEW V AS SELECT *, T.c1.r1.v11 FROM T;""");
        this.compileToMultiCrate(file.getAbsolutePath(), true);
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
        this.compileToMultiCrate(file.getAbsolutePath(), true, false);
        //noinspection ResultOfMethodCallIgnored
        udf.delete();
        udf.deleteOnExit();
    }
}
