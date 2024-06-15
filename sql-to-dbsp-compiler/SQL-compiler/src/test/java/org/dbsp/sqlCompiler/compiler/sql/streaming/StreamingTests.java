package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.MonotoneAnalyzer;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;

/** Tests that exercise streaming features. */
public class StreamingTests extends StreamingTest {
    @Test
    public void hoppingTest() {
        String sql = """
                CREATE TABLE series (
                    pickup TIMESTAMP NOT NULL
                );
                CREATE VIEW V AS
                SELECT * FROM TABLE(
                  HOP(
                    TABLE series,
                    DESCRIPTOR(pickup),
                    INTERVAL '2' MINUTE,
                    INTERVAL '5' MINUTE));""";

        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase("hoppingTest", ccs);
    }

    @Test
    public void tumblingTestLimits() {
        String sql = """
               CREATE TABLE series (
                   pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
               );
               CREATE VIEW V AS
               SELECT TUMBLE_START(pickup, INTERVAL 30 MINUTES, TIME '00:12:00'),
                      TUMBLE_END(pickup, INTERVAL 30 MINUTES, TIME '00:12:00')
               FROM series
               GROUP BY TUMBLE(pickup, INTERVAL 30 MINUTES, TIME '00:12:00');""";

        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);

        ccs.step("INSERT INTO series VALUES('2024-02-08 10:00:00')",
                """
                 start               | end                 | weight
                ----------------------------------------------------
                 2024-02-08 09:42:00 | 2024-02-08 10:12:00 | 1""");
        ccs.step("INSERT INTO series VALUES('2024-02-08 10:10:00')",
                """
                start              | end                 | weight
                ---------------------------------------------------"""); // same group
        ccs.step( "INSERT INTO series VALUES('2024-02-08 10:12:00')",
                """
                 start               | end                 | weight
                ----------------------------------------------------
                 2024-02-08 10:12:00 | 2024-02-08 10:42:00 | 1""");
        ccs.step("INSERT INTO series VALUES('2024-02-08 10:30:00')",
                """
                start              | end                 | weight
                ---------------------------------------------------"""); // same group as before

        this.addRustTestCase("tumblingTestLimits", ccs);
    }

    @Test
    public void tumblingTest() {
        String sql = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT AVG(distance), TUMBLE_START(pickup, INTERVAL '1' DAY) FROM series
                GROUP BY TUMBLE(pickup, INTERVAL '1' DAY)""";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(sql);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.step(
                "INSERT INTO series VALUES(10.0, '2023-12-30 10:00:00');",
                """
                 avg  | start | weight
                ----------------------
                 10.0 | 2023-12-30 00:00:00 | 1""");
        // Insert tuple before waterline, should be dropped
        ccs.step("INSERT INTO series VALUES(10.0, '2023-12-29 10:00:00');",
                """
                avg  | start | weight
                ----------------------""");
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        ccs.step("INSERT INTO series VALUES(20.0, '2023-12-30 10:10:00');",
                """
                 avg  | start | weight
                ----------------------
                 15.0 | 2023-12-30 00:00:00 | 1
                 10.0 | 2023-12-30 00:00:00 | -1""");
        // Insert tuple before last waterline, should be dropped
        ccs.step("INSERT INTO series VALUES(10.0, '2023-12-29 09:10:00');",
                """
                avg  | start | weight
                ----------------------""");
        // Insert tuple in the past, but before the last waterline
        ccs.step("INSERT INTO series VALUES(10.0, '2023-12-30 10:00:00');",
                """
                avg  | start | weight
                ----------------------
                13.333333333333334 | 2023-12-30 00:00:00 | 1
                15.0               | 2023-12-30 00:00:00 | -1""");
        // Insert tuple in the next tumbling window
        ccs.step("INSERT INTO series VALUES(10.0, '2023-12-31 10:00:00');",
                """
                avg  | start | weight
                ----------------------
                10.0 | 2023-12-31 00:00:00 | 1""");
        this.addRustTestCase("tumblingTest", ccs);
    }

    @Test
    public void blogTest() {
        String statements = """
                CREATE TABLE CUSTOMER(name VARCHAR NOT NULL, zipcode INT NOT NULL);
                CREATE VIEW DENSITY AS
                SELECT zipcode, COUNT(name)
                FROM CUSTOMER
                GROUP BY zipcode
                """;
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(statements);
        Assert.assertFalse(compiler.hasErrors());
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.step("",
                """
                 zipcode | count | weight
                --------------------------""");
        ccs.step("""
                 INSERT INTO customer VALUES('Bob', 1000);
                 INSERT INTO customer VALUES('Pam', 2000);
                 INSERT INTO customer VALUES('Sue', 3000);
                 INSERT INTO customer VALUES('Mike', 1000);""",
                """
                 zipcode | count | weight
                --------------------------
                 1000    | 2     | 1
                 2000    | 1     | 1
                 3000    | 1     | 1""");
        ccs.step("""
                REMOVE FROM customer VALUES('Bob', 1000);
                INSERT INTO customer VALUES('Bob', 2000);""",
                """
                 zipcode | count | weight
                --------------------------
                 1000    | 2     | -1
                 2000    | 1     | -1
                 2000    | 2     | 1
                 1000    | 1     | 1""");
        this.addRustTestCase("ivm blog post", ccs);
    }

    @Test
    public void nullableLatenessTest() {
        // LATENESS used on a nullable column
        String ddl = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP LATENESS INTERVAL '1:00' HOURS TO MINUTES
                )""";
        String query =
                "SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE)";
        DBSPCompiler compiler = testCompiler();
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        Assert.assertFalse(compiler.hasErrors());
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase("nullableLatenessTest", ccs);
    }

    @Test
    public void watermarkTest() {
        String ddl = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL WATERMARK INTERVAL '1:00' HOURS TO MINUTES
                )""";
        String query =
                "SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE)";
        DBSPCompiler compiler = testCompiler();
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.step("INSERT INTO series VALUES(10, '2023-12-30 10:00:00');",
                """
                         avg  | date       | weight
                        ---------------------------""");
        // Insert tuple before watermark, should be processed
        ccs.step("INSERT INTO series VALUES(10, '2023-12-29 10:00:00');",
                """
                         avg  | date       | weight
                        ---------------------------
                         10   | 2023-12-29 | 1""");
        // Insert tuple after waterline, but not after watermark
        // Waterline is advanced, no new outputs
        ccs.step("INSERT INTO series VALUES(20, '2023-12-30 10:10:00');",
                """
                         avg  | date        | weight
                        ---------------------------""");
        // Insert tuple before last waterline, should be processed
        // average does not change for 2023-12-19
        ccs.step("INSERT INTO series VALUES(10, '2023-12-29 09:10:00');",
                """
                 avg  | date       | weight
                ---------------------------""");
        // Insert tuple in the past, but before the last waterline
        // no new output
        ccs.step("INSERT INTO series VALUES(10, '2023-12-30 10:00:00');",
                """
                         avg  | date        | weight
                        ---------------------------""");
        // Insert one more tuple that accepts all buffered 3 tuples
        ccs.step("INSERT INTO series VALUES(10, '2023-12-31 10:00:00');",
                """
                         avg  | date        | weight
                        ---------------------------
                         13.333333333333334 | 2023-12-30 | 1""");
        this.addRustTestCase("latenessTest", ccs);
    }

    @Test
    public void latenessTest() {
        String ddl = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                )""";
        String query =
                "SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE)";
        DBSPCompiler compiler = testCompiler();
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.step("INSERT INTO series VALUES(10, '2023-12-30 10:00:00');",
                """
                         avg  | date       | weight
                        ---------------------------
                         10.0 | 2023-12-30 | 1""");
        // Insert tuple before waterline, should be dropped
        ccs.step("INSERT INTO series VALUES(10, '2023-12-29 10:00:00');",
                """
                         avg  | date       | weight
                        ---------------------------""");
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        ccs.step("INSERT INTO series VALUES(20, '2023-12-30 10:10:00');",
                """
                         avg  | date        | weight
                        ---------------------------
                         15.0 | 2023-12-30 | 1
                         10.0 | 2023-12-30 | -1""");
        // Insert tuple before last waterline, should be dropped
        ccs.step("INSERT INTO series VALUES(10, '2023-12-29 09:10:00');",
                        """
                         avg  | date       | weight
                        ---------------------------""");
        // Insert tuple in the past, but before the last waterline
        ccs.step("INSERT INTO series VALUES(10, '2023-12-30 10:00:00');",
                """
                         avg  | date        | weight
                        ---------------------------
                         15.0 | 2023-12-30 | -1
                         13.333333333333334 | 2023-12-30 | 1""");
        this.addRustTestCase("latenessTest", ccs);
    }

    Long[] profile(String program) throws IOException, InterruptedException, SQLException {
        // Rust program which profiles the circuit.
        String main = """
                use dbsp::{
                    algebra::F64,
                    circuit::CircuitConfig,
                    utils::Tup2,
                    zset,
                };

                use sqllib::{
                    append_to_collection_handle,
                    read_output_handle,
                    casts::cast_to_Timestamp_s,
                };

                use std::{
                    io::Write,
                    ops::Add,
                    fs::File,
                    time::SystemTime,
                };

                use temp::circuit;
                use dbsp::circuit::Layout;
                use uuid::Uuid;

                #[test]
                // Run the circuit generated by 'circuit' for a while then measure the
                // memory consumption.  Write the time taken and the memory used into
                // a file called "mem.txt".
                pub fn test() {
                    let (mut circuit, streams) = circuit(
                         CircuitConfig {
                             layout: Layout::new_solo(2),
                             storage: None,
                             min_storage_rows: usize::MAX,
                             init_checkpoint: Uuid::nil(),
                         }).expect("could not build circuit");
                    let start = SystemTime::now();

                    // Initial data value for timestamp
                    let mut timestamp = cast_to_Timestamp_s("2024-01-10 10:10:10".to_string());
                    for i in 0..1000000 {
                        let value = Some(F64::new(i.into()));
                        timestamp = timestamp.add(20000);
                        let input = zset!(Tup2::new(value, timestamp) => 1);
                        append_to_collection_handle(&input, &streams.0);
                        if i % 1000 == 0 {
                            let _ = circuit.step().expect("could not run circuit");
                            let _ = &read_output_handle(&streams.1);
                            /*
                            let end = SystemTime::now();
                            let profile = circuit.retrieve_profile().expect("could not get profile");
                            let duration = end.duration_since(start).expect("could not get time");
                            println!("{:?},{:?}", duration.as_millis(), profile.total_used_bytes().unwrap().bytes);
                            */
                        }
                    }
                    let profile = circuit.retrieve_profile().expect("could not get profile");
                    let end = SystemTime::now();
                    let duration = end.duration_since(start).expect("could not get time");

                    let mut data = String::new();
                    data.push_str(&format!("{},{}\\n",
                                           duration.as_millis(),
                                           profile.total_used_bytes().unwrap().bytes));
                    let mut file = File::create("mem.txt").expect("Could not create file");
                    file.write_all(data.as_bytes()).expect("Could not write data");
                    // println!("{:?},{:?}", duration, profile.total_used_bytes().unwrap());
                }""";
        File script = createInputScript(program);
        CompilerMessages messages = CompilerMain.execute(
                "-o", BaseSQLTests.testFilePath, "--handles", "-i",
                // "-TMonotoneAnalyzer=3",
                script.getPath());
        System.out.println(messages);
        Assert.assertEquals(0, messages.errorCount());

        String mainFilePath = rustDirectory + "/main.rs";
        File file = new File(mainFilePath);
        try (PrintWriter mainFile = new PrintWriter(file, StandardCharsets.UTF_8)) {
            mainFile.print(main);
        }
        file.deleteOnExit();
        Utilities.compileAndTestRust(rustDirectory, true, "--release");
        File mainFile = new File(mainFilePath);
        boolean deleted = mainFile.delete();
        Assert.assertTrue(deleted);

        // After executing this Rust program the output is in file "mem.txt"
        // It contains two numbers: time taken (ms) and memory used (bytes).
        String outFile = "mem.txt";
        Path outFilePath = Paths.get(rustDirectory, "..", outFile);
        List<String> strings = Files.readAllLines(outFilePath);
        // System.out.println(strings);
        Assert.assertEquals(1, strings.size());
        String[] split = strings.get(0).split(",");
        Assert.assertEquals(2, split.length);
        deleted = outFilePath.toFile().delete();
        Assert.assertTrue(deleted);
        return Linq.map(split, Long::parseLong, Long.class);
    }

    @Test
    public void profileLateness() throws IOException, InterruptedException, SQLException {
        String ddlLateness = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                """;
        String ddl = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL
                );
                """;
        String query = """
                CREATE VIEW V AS
                SELECT AVG(distance), CAST(pickup AS DATE)
                FROM series GROUP BY CAST(pickup AS DATE);
                """;

        Long[] p0 = this.profile(ddl + query);
        Long[] p1 = this.profile(ddlLateness + query);
        // Memory consumption of program with lateness is expected to be higher
        Assert.assertTrue(p0[1] > 1.5 * p1[1]);
    }

    @Test
    public void testJoin() {
        String ddl = """
            CREATE TABLE series (
                    metadata VARCHAR,
                    event_time TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
            );
            
            CREATE TABLE shift(
                    person VARCHAR,
                    on_call DATE
            );
            """;
        String query =
                "SELECT metadata, person FROM series " +
                        "JOIN shift ON CAST(series.event_time AS DATE) = shift.on_call";
        DBSPCompiler compiler = testCompiler();
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatements(ddl);
        compiler.compileStatement(query);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase("testJoin", ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.count);
            }
        };
        visitor.apply(ccs.circuit);
    }

    // Test for https://github.com/feldera/feldera/issues/1462
    @Test
    public void testJoinNonMonotoneColumn() {
        String script = """
            CREATE TABLE series (
                    metadata VARCHAR NOT NULL,
                    event_time TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
            );
            
            CREATE TABLE shift(
                    person VARCHAR NOT NULL,
                    on_call DATE
            );
        
            CREATE VIEW V AS
            (SELECT * FROM series JOIN shift ON series.metadata = shift.person);
            """;
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(script);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase("testJoinNonMonotoneColumn", ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(0, this.count);
            }
        };
        visitor.apply(ccs.circuit);
    }

    @Test
    public void testJoinTwoColumns() {
        // One joined column is monotone, the other one isn't.
        String script = """
            CREATE TABLE series (
                    metadata VARCHAR NOT NULL,
                    event_time TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
            );
            
            CREATE TABLE shift(
                    person VARCHAR NOT NULL,
                    on_call DATE
            );
        
            CREATE VIEW V AS
            (SELECT * FROM series JOIN shift
             ON series.metadata = shift.person AND CAST(series.event_time AS DATE) = shift.on_call);
            """;
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(script);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase("testJoinTwoColumns", ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.count);
            }
        };
        visitor.apply(ccs.circuit);
    }

    @Test
    public void testJoinFilter() {
        // Logger.INSTANCE.setLoggingLevel(MonotoneAnalyzer.class, 2);
        // Join two streams with lateness, and filter based on lateness column
        String script = """
            CREATE TABLE series (
                    metadata VARCHAR NOT NULL,
                    event_date DATE NOT NULL LATENESS INTERVAL 1 DAYS
            );
            
            CREATE TABLE shift(
                    person VARCHAR NOT NULL,
                    on_call DATE NOT NULL LATENESS INTERVAL 1 DAYS
            );
        
            CREATE VIEW V AS
            (SELECT metadata, event_date FROM series JOIN shift
             ON series.metadata = shift.person AND event_date > on_call);
            """;
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(script);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            // TODO: should be 1
            public void endVisit() {
                Assert.assertEquals(0, this.count);
            }
        };
        visitor.apply(ccs.circuit);
    }

    @Test
    public void testAggregate() {
        String sql = """
                CREATE TABLE event_t (
                    event_type_id BIGINT NOT NULL
                );

                -- running total of event types
                CREATE VIEW event_type_count_v AS
                SELECT count(DISTINCT event_type_id) as event_type_count
                from   event_t
                ;""";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(sql);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.step("",
                """
                 event_type_count | weight
                ---------------------------
                 0                | 1""");
        ccs.step("",
                """
                 event_type_count | weight
                ---------------------------""");
        ccs.step("INSERT INTO event_t VALUES(1);",
                 """
                 event_type_count | weight
                ---------------------------
                 0                | -1
                 1                | 1""");
        ccs.step("",
                """
                 event_type_count | weight
                ---------------------------""");
        ccs.step("INSERT INTO event_t VALUES(2);",
                """
                 event_type_count | weight
                ---------------------------
                 1                | -1
                 2                | 1""");
        this.addRustTestCase("testAggregate", ccs);
    }
}
