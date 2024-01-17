package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputPair;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.junit.Test;

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
    public void latenessTest() {
        String ddl = """
                CREATE TABLE series (
                        distance DOUBLE PRECISION,
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
                new DBSPDoubleLiteral(13.0, true),
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
