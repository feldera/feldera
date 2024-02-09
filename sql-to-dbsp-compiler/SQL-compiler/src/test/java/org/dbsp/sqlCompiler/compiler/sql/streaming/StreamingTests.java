package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputPair;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.Linq;
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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Tests that exercise streaming features. */
public class StreamingTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions options = super.testOptions(incremental, optimize);
        options.languageOptions.incrementalize = true;
        return options;
    }

    @Test
    public void tumblingTest() {
        String ddl = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                )""";
        String query =
                "SELECT AVG(distance), TUMBLE_START(pickup, INTERVAL '1' DAY) FROM series" +
                        " GROUP BY TUMBLE(pickup, INTERVAL '1' DAY)";
        DBSPCompiler compiler = testCompiler();
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        InputOutputPair[] data = new InputOutputPair[6];
        data[0] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-12-30 10:00:00", false))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-12-30 00:00:00", false)
                        )));
        // Insert tuple before waterline, should be dropped
        data[1] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-12-29 10:00:00", false))),
                DBSPZSetLiteral.Contents.emptyWithElementType(data[0].outputs[0].elementType));
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        DBSPZSetLiteral.Contents addSub = DBSPZSetLiteral.Contents.emptyWithElementType(data[0].outputs[0].elementType);
        addSub.add(new DBSPTupleExpression(
                new DBSPDoubleLiteral(15.0, true),
                new DBSPTimestampLiteral("2023-12-30 00:00:00", false)));
        addSub.add(new DBSPTupleExpression(
                new DBSPDoubleLiteral(10.0, true),
                new DBSPTimestampLiteral("2023-12-30 00:00:00", false)), -1);
        data[2] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(20.0, true),
                                new DBSPTimestampLiteral("2023-12-30 10:10:00", false))),
                addSub);
        // Insert tuple before last waterline, should be dropped
        data[3] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-12-29 09:10:00", false))),
                DBSPZSetLiteral.Contents.emptyWithElementType(data[0].outputs[0].elementType));
        // Insert tuple in the past, but before the last waterline
        addSub = DBSPZSetLiteral.Contents.emptyWithElementType(data[0].outputs[0].elementType);
        addSub.add(new DBSPTupleExpression(
                new DBSPDoubleLiteral(13.333333333333334, true),
                new DBSPTimestampLiteral("2023-12-30 00:00:00", false)), 1);
        addSub.add(new DBSPTupleExpression(
                new DBSPDoubleLiteral(15.0, true),
                new DBSPTimestampLiteral("2023-12-30 00:00:00", false)), -1);
        data[4] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-12-30 10:00:00", false))),
                addSub);
        // Insert tuple in the next tumbling window
        data[5] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-13-30 10:00:00", false))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-13-30 00:00:00", false)
                        )));
        this.addRustTestCase("latenessTest", compiler, getCircuit(compiler), data);
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
        InputOutputPair[] data = new InputOutputPair[5];
        data[0] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-12-30 10:00:00", false))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPDateLiteral("2023-12-30")
                        )));
        // Insert tuple before waterline, should be dropped
        data[1] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-12-29 10:00:00", false))),
                DBSPZSetLiteral.Contents.emptyWithElementType(data[0].outputs[0].elementType));
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        DBSPZSetLiteral.Contents addSub = DBSPZSetLiteral.Contents.emptyWithElementType(data[0].outputs[0].elementType);
        addSub.add(new DBSPTupleExpression(
                new DBSPDoubleLiteral(15.0, true),
                new DBSPDateLiteral("2023-12-30")));
        addSub.add(new DBSPTupleExpression(
                new DBSPDoubleLiteral(10.0, true),
                new DBSPDateLiteral("2023-12-30")), -1);
        data[2] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(20.0, true),
                                new DBSPTimestampLiteral("2023-12-30 10:10:00", false))),
                addSub);
        // Insert tuple before last waterline, should be dropped
        data[3] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-12-29 09:10:00", false))),
                DBSPZSetLiteral.Contents.emptyWithElementType(data[0].outputs[0].elementType));
        // Insert tuple in the past, but before the last waterline
        addSub = DBSPZSetLiteral.Contents.emptyWithElementType(data[0].outputs[0].elementType);
        addSub.add(new DBSPTupleExpression(
                new DBSPDoubleLiteral(13.333333333333334, true),
                new DBSPDateLiteral("2023-12-30")), 1);
        addSub.add(new DBSPTupleExpression(
                new DBSPDoubleLiteral(15.0, true),
                new DBSPDateLiteral("2023-12-30")), -1);
        data[4] = new InputOutputPair(
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPDoubleLiteral(10.0, true),
                                new DBSPTimestampLiteral("2023-12-30 10:00:00", false))),
                addSub);
        this.addRustTestCase("latenessTest", compiler, getCircuit(compiler), data);
    }

    Long[] profile(String program) throws IOException, InterruptedException {
        // Rust program which profiles the circuit.
        String main = """
               use dbsp::{
                   algebra::F64,
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

               #[test]
               // Run the circuit generated by 'circuit' for a while then measure the
               // memory consumption.  Write the time taken and the memory used into
               // a file called "mem.txt".
               pub fn test() {
                   let (mut circuit, streams) = circuit(2).expect("could not build circuit");
                   let start = SystemTime::now();

                   // Initial data value for timestamp
                   let mut timestamp = cast_to_Timestamp_s("2024-01-10 10:10:10".to_string());
                   for i in 0..1000000 {
                       let value = Some(F64::new(i.into()));
                       timestamp = timestamp.add(20000);
                       let input = zset!(Tup2::new(value, timestamp) => 1);
                       append_to_collection_handle(&input, &streams.0);
                       if i % 100 == 0 {
                           let _ = circuit.step().expect("could not run circuit");
                           let _ = &read_output_handle(&streams.1);
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
                "-o", BaseSQLTests.testFilePath, "--handles", "-i", script.getPath());
        System.out.println(messages);
        Assert.assertEquals(0, messages.errorCount());

        String mainFilePath = rustDirectory + "/main.rs";
        File file = new File(mainFilePath);
        try (PrintWriter mainFile = new PrintWriter(file, StandardCharsets.UTF_8)) {
            mainFile.print(main);
        }
        Utilities.compileAndTestRust(rustDirectory, true, "--release");
        File mainFile = new File(mainFilePath);
        boolean deleted = mainFile.delete();
        Assert.assertTrue(deleted);

        // After executing this Rust program the output is in file "mem.txt"
        // It contains two numbers: time taken (ms) and memory used (bytes).
        String outFile = "mem.txt";
        Path outFilePath = Paths.get(rustDirectory, "..", outFile);
        List<String> strings = Files.readAllLines(outFilePath);
        Assert.assertEquals(1, strings.size());
        String[] split = strings.get(0).split(",");
        Assert.assertEquals(2, split.length);
        deleted = outFilePath.toFile().delete();
        Assert.assertTrue(deleted);
        return Linq.map(split, Long::parseLong, Long.class);
    }

    @Test
    public void profileLateness() throws IOException, InterruptedException {
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
        // Memory consumption of program with lateness is expected to be much higher
        Assert.assertTrue(p0[1] > 5 * p1[1]);
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
                "SELECT metadata, person FROM series JOIN shift ON CAST(series.event_time AS DATE) = shift.on_call";
        DBSPCompiler compiler = testCompiler();
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatements(ddl);
        compiler.compileStatement(query);
        InputOutputPair[] data = new InputOutputPair[0];
        this.addRustTestCase("latenessTest", compiler, getCircuit(compiler), data);
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
        List<InputOutputPair> data = new ArrayList<>();

        DBSPTupleExpression zero = new DBSPTupleExpression(new DBSPI64Literal(0));
        DBSPExpression one = new DBSPTupleExpression(new DBSPI64Literal(1));
        DBSPExpression two = new DBSPTupleExpression(new DBSPI64Literal(2));

        Consumer<InputOutputPair> add = data::add;

        add.accept(new InputOutputPair(
                DBSPZSetLiteral.Contents.emptyWithElementType(one.getType()),
                new DBSPZSetLiteral.Contents(zero)));

        add.accept(new InputOutputPair(
                DBSPZSetLiteral.Contents.emptyWithElementType(one.getType()),
                DBSPZSetLiteral.Contents.emptyWithElementType(one.getType())));

        DBSPZSetLiteral.Contents expected1 = new DBSPZSetLiteral.Contents(one);
        expected1.add(zero, -1);
        add.accept(new InputOutputPair(
                new DBSPZSetLiteral.Contents(one),
                expected1));

        add.accept(new InputOutputPair(
                DBSPZSetLiteral.Contents.emptyWithElementType(one.getType()),
                DBSPZSetLiteral.Contents.emptyWithElementType(one.getType())));
        DBSPZSetLiteral.Contents expected3 = new DBSPZSetLiteral.Contents(two);
        expected3.add(one, -1);

        add.accept(new InputOutputPair(
                new DBSPZSetLiteral.Contents(two),
                expected3));

        InputOutputPair[] array = data.toArray(new InputOutputPair[0]);
        this.addRustTestCase("testAggregate", compiler, getCircuit(compiler), array);
    }
}
