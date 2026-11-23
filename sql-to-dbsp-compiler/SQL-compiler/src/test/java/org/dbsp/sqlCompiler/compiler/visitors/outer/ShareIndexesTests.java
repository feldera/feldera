package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.ShareIndexes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.FindSharedIndexes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.ReplaceSharedIndexes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.ShareOutputIntegrators;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeoPoint;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.Linq;
import org.dbsp.util.IdShuffle;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Tests for {@link ShareIndexes} and {@link ShareOutputIntegrators}. */
public class ShareIndexesTests extends StreamingTestBase {
    private static DBSPExpression nestedCalls(DBSPExpression value) {
        for (int i = 0; i < 160; i++)
            value = new DBSPApplyExpression("transform", value.getType(), value);
        return value;
    }

    @Test
    public void aDeepSharedIndexReaderUsesTheRemappedColumn() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustSharedIndexes"));
        var integer = new DBSPI32Literal(0).getType();
        var narrow = new DBSPTypeTuple(integer).ref().var();
        var wide = new DBSPTypeTuple(integer, integer).ref().var();
        var reader = nestedCalls(narrow.deref().field(0)).closure(narrow);
        var actual = RustCircuitPass.rewriteIndexReader(this.testCompiler(),
                List.of(reader, reader.parameters[0], wide), List.of(List.of(1))).to(DBSPClosureExpression.class);
        var expected = nestedCalls(wide.deref().field(1)).closure(wide);
        Assert.assertTrue(expected.equivalent(actual));
    }

    @Test
    public void wideIndexFieldsShareADeepProjectionAcrossNarrowIndexes() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustSharedIndexes"));
        var integer = new DBSPI32Literal(0).getType();
        var type = new DBSPTypeTuple(integer, integer);
        var functions = new ArrayList<DBSPClosureExpression>();
        for (int column : List.of(0, 1)) {
            var row = type.ref().var();
            var key = new DBSPTupleExpression(row.deref().field(0));
            var fields = new DBSPTupleExpression(nestedCalls(row.deref().field(1)), row.deref().field(column));
            functions.add(new DBSPRawTupleExpression(key, fields).closure(row));
        }
        var wide = RustCircuitPass.wideIndexFields(this.testCompiler(), functions);
        Assert.assertEquals(List.of(List.of(0, 1), List.of(0, 2)), wide.layout());
        Assert.assertEquals(3, wide.fields().size());
        var expected = nestedCalls(wide.variable().deref().field(1)).closure(wide.variable());
        Assert.assertTrue(expected.equivalent(wide.fields().get(0).closure(wide.variable())));
    }

    private static final String TABLES = """
            CREATE TABLE T(k INT NOT NULL, a INT, s VARCHAR, d DOUBLE);
            CREATE TABLE S(k INT NOT NULL, b INT);
            """;

    /** A table with a timestamp, for the rolling aggregate and window tests */
    private static final String TABLE2 = """
            CREATE TABLE T2(k INT NOT NULL, j INT NOT NULL, ts INT NOT NULL, a INT);
            CREATE TABLE act(id INT NOT NULL, version INT NOT NULL);
            """;

    private static final String JOIN = "CREATE VIEW J AS SELECT T.s, S.b FROM T JOIN S ON T.k = S.k;\n";

    /** Records the operator that each join, aggregate, and TopK operator reads in the circuit
     * it visits, and counts the aggregates that carry no aggregate list */
    private static class ConsumerInputs extends CircuitVisitor {
        final Set<DBSPOperator> joinInputs = new HashSet<>();
        final Set<DBSPOperator> aggregateInputs = new HashSet<>();
        final Set<DBSPOperator> topKInputs = new HashSet<>();
        final Set<DBSPOperator> lagInputs = new HashSet<>();
        final Set<DBSPOperator> antiJoinInputs = new HashSet<>();
        final Set<DBSPOperator> sinkInputs = new HashSet<>();
        int loweredAggregates = 0;
        int rollingAggregates = 0;
        boolean ran = false;

        ConsumerInputs(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public DBSPCircuit apply(DBSPCircuit circuit) {
            this.ran = true;
            return super.apply(circuit);
        }

        @Override
        public void postorder(DBSPJoinBaseOperator join) {
            this.joinInputs.add(join.left().node());
            this.joinInputs.add(join.right().node());
        }

        @Override
        public void postorder(DBSPAggregateOperatorBase aggregate) {
            this.aggregateInputs.add(aggregate.input().node());
            if (aggregate.is(DBSPPartitionedRollingAggregateOperator.class))
                this.rollingAggregates++;
            if (aggregate.aggregateList == null)
                this.loweredAggregates++;
        }

        @Override
        public void postorder(DBSPIndexedTopKOperator topK) {
            this.topKInputs.add(topK.input().node());
        }

        @Override
        public void postorder(DBSPAntiJoinOperator antiJoin) {
            this.antiJoinInputs.add(antiJoin.left().node());
            this.antiJoinInputs.add(antiJoin.right().node());
        }

        @Override
        public void postorder(DBSPLagOperator lag) {
            this.lagInputs.add(lag.input().node());
        }

        @Override
        public void postorder(DBSPSinkOperator sink) {
            this.sinkInputs.add(sink.input().node());
        }

        /** True if a join and an aggregate read the same operator */
        boolean joinSharesWithAggregate() {
            return !Collections.disjoint(this.joinInputs, this.aggregateInputs);
        }

        /** True if a join and a TopK read the same operator */
        boolean joinSharesWithTopK() {
            return !Collections.disjoint(this.joinInputs, this.topKInputs);
        }

        /** True if a join and a LAG read the same operator */
        boolean joinSharesWithLag() {
            return !Collections.disjoint(this.joinInputs, this.lagInputs);
        }

        /** True if a join and an antijoin read the same operator */
        boolean joinSharesWithAntiJoin() {
            return !Collections.disjoint(this.joinInputs, this.antiJoinInputs);
        }

        /** True if a join and the sink of a view index read the same operator */
        boolean joinSharesWithSink() {
            return !Collections.disjoint(this.joinInputs, this.sinkInputs);
        }
    }

    /** Make {@code compiler} record the consumer inputs right after {@link ShareIndexes} */
    private static ConsumerInputs recordAfterSharing(DBSPCompiler compiler) {
        ConsumerInputs inputs = new ConsumerInputs(compiler);
        compiler.optimizerHook = optimizer -> optimizer.insertAfter(ShareIndexes.class, inputs);
        return inputs;
    }

    /** Records the consumer inputs of a circuit that has run all optimizations */
    private static ConsumerInputs atEnd(DBSPCompiler compiler, DBSPCircuit circuit) {
        ConsumerInputs inputs = new ConsumerInputs(compiler);
        inputs.apply(circuit);
        return inputs;
    }

    /** The consumer inputs recorded right after {@link ShareIndexes} and in the final circuit.
     * The passes that follow must leave the sharing alone, so each query compares the two. */
    private record Sharing(ConsumerInputs afterSharing, ConsumerInputs atEnd) {
        /** True if a join and an aggregate read the same operator */
        boolean joinSharesWithAggregate() {
            boolean shared = this.afterSharing.joinSharesWithAggregate();
            Assert.assertEquals("The passes after ShareIndexes changed the sharing between " +
                    "a join and an aggregate", shared, this.atEnd.joinSharesWithAggregate());
            return shared;
        }

        /** True if a join and an antijoin read the same operator */
        boolean joinSharesWithAntiJoin() {
            boolean shared = this.afterSharing.joinSharesWithAntiJoin();
            Assert.assertEquals("The passes after ShareIndexes changed the sharing between " +
                    "a join and an antijoin", shared, this.atEnd.joinSharesWithAntiJoin());
            return shared;
        }

        /** True if a join and a LAG read the same operator */
        boolean joinSharesWithLag() {
            boolean shared = this.afterSharing.joinSharesWithLag();
            Assert.assertEquals("The passes after ShareIndexes changed the sharing between " +
                    "a join and a LAG", shared, this.atEnd.joinSharesWithLag());
            return shared;
        }

        /** True if a join and the sink of a view index read the same operator */
        boolean joinSharesWithSink() {
            boolean shared = this.afterSharing.joinSharesWithSink();
            Assert.assertEquals("The passes after ShareIndexes changed the sharing between " +
                    "a join and a view index", shared, this.atEnd.joinSharesWithSink());
            return shared;
        }

        /** True if a join and a TopK read the same operator */
        boolean joinSharesWithTopK() {
            boolean shared = this.afterSharing.joinSharesWithTopK();
            Assert.assertEquals("The passes after ShareIndexes changed the sharing between " +
                    "a join and a TopK", shared, this.atEnd.joinSharesWithTopK());
            return shared;
        }
    }

    /** Compile a program and record the consumer inputs after {@link ShareIndexes} and in the
     * final circuit.
     * @param viewDefinitions  SQL statements that define views, compiled after {@link #TABLES}. */
    private Sharing afterSharing(String viewDefinitions) {
        DBSPCompiler compiler = this.testCompiler();
        ConsumerInputs inputs = recordAfterSharing(compiler);
        compiler.submitStatementsForCompilation(TABLES + TABLE2 + viewDefinitions);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        Assert.assertTrue("The optimizer did not reach ShareIndexes", inputs.ran);
        return new Sharing(inputs, atEnd(compiler, circuit));
    }

    @Test
    public void joinAndAggregateShareIndex() {
        // SUM over DOUBLE is not linear, so the aggregate integrates its input.  The join and
        // the aggregate share that integral, the one in front of the aggregate.
        Assert.assertTrue(this.afterSharing(JOIN + """
                CREATE VIEW A AS SELECT k, SUM(d) AS d FROM T GROUP BY k;""").joinSharesWithAggregate());
    }

    @Test
    public void twoAggregatesShareIndex() {
        // The integrals the two aggregates keep of their inputs are shared, holding 'd' and
        // 'a'.  Neither aggregate is linear, and both group by 'k'.
        Sharing inputs = this.afterSharing("""
                CREATE VIEW A1 AS SELECT k, SUM(d) AS d FROM T GROUP BY k;
                CREATE VIEW A2 AS SELECT k, BIT_OR(a) AS o FROM T GROUP BY k;""");
        Assert.assertEquals(1, inputs.afterSharing().aggregateInputs.size());
        Assert.assertEquals(1, inputs.atEnd().aggregateInputs.size());
    }

    @Test
    public void rollingAggregatesShareIndex() {
        // Both rolling aggregates read T2 indexed on 'ts', the column they order by, so the
        // integrals they keep of their inputs are shared; the shared value holds the partition
        // columns 'k' and 'j' and the aggregated column 'a'.
        Sharing inputs = this.afterSharing("""
                CREATE VIEW W AS SELECT k, j, ts,
                  SUM(a) OVER (PARTITION BY k ORDER BY ts RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS sk,
                  SUM(a) OVER (PARTITION BY j ORDER BY ts RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS sj
                FROM T2;""");
        Assert.assertEquals(2, inputs.afterSharing().rollingAggregates);
        Assert.assertEquals(1, inputs.afterSharing().aggregateInputs.size());
        Assert.assertEquals(1, inputs.atEnd().aggregateInputs.size());
    }

    @Test
    public void lagSharesIndexWithJoin() {
        // LAG reads T2 indexed on 'k' with the value it orders by, and the join reads a subset
        // of that value, so the integral LAG keeps of its input and the one the join keeps of
        // its T2 input are shared.
        Assert.assertTrue(this.afterSharing("""
                CREATE VIEW L AS SELECT k, ts, LAG(a) OVER (PARTITION BY k ORDER BY ts) AS prev FROM T2;
                CREATE VIEW J AS SELECT T2.a, S.b FROM T2 JOIN S ON T2.k = S.k;""")
                .joinSharesWithLag());
    }

    @Test
    public void viewIndexSharesIndexWithJoin() {
        // The sink of the index over materialized view M reads T indexed on 'k', and the join
        // reads the same index, so the integral behind it is shared.
        Assert.assertTrue(this.afterSharing("""
                CREATE MATERIALIZED VIEW M AS SELECT k, a FROM T;
                CREATE INDEX mi ON M(k);
                CREATE VIEW J AS SELECT T.a, S.b FROM T JOIN S ON T.k = S.k;""")
                .joinSharesWithSink());
    }

    @Test
    public void antiJoinSharesIndexWithJoin() {
        // A recursive view with NOT EXISTS compiles to an antijoin.  The integral it keeps of
        // its input and the one the join in the same component keeps are shared.
        Assert.assertTrue(this.afterSharing("""
                DECLARE RECURSIVE VIEW st(id INT NOT NULL, version INT NOT NULL);
                CREATE LOCAL VIEW next_state AS
                SELECT act.id AS id, act.version AS version FROM st
                JOIN act ON act.id = st.id AND act.version = st.version + 1;
                CREATE VIEW st AS
                SELECT id, 0 AS version FROM act WHERE version = 0
                  AND NOT EXISTS (SELECT 1 FROM st WHERE id = st.id)
                UNION
                SELECT * FROM next_state
                UNION
                SELECT * FROM st WHERE NOT EXISTS (SELECT 1 FROM next_state WHERE id = next_state.id);""")
                .joinSharesWithAntiJoin());
    }

    @Test
    public void differentKeysDoNotShare() {
        // Sharing is not possible: the integral the aggregate keeps of T indexed on 'a' and
        // the one the join keeps of T indexed on 'k' hold different collections.
        Assert.assertFalse(this.afterSharing(JOIN + """
                CREATE VIEW A AS SELECT a, SUM(d) AS d FROM T GROUP BY a;""").joinSharesWithAggregate());
    }

    @Test
    public void minMaxDoesNotShare() {
        // Sharing is not possible: the integral MAX keeps of its input and the one the join
        // keeps of its T input cannot be shared.
        Assert.assertFalse(this.afterSharing(JOIN + """
                CREATE VIEW A AS SELECT k, MAX(a) AS m FROM T GROUP BY k;""").joinSharesWithAggregate());
    }

    @Test
    public void minMaxSharesIdenticalIndex() {
        // The integral MAX keeps of its input and the one the join keeps of its T input
        // are shared: both index T on 'k' with the same value.  That is the only case 
        // when a MAX input integral can be shared, since its index cannot be rewritten.
        Assert.assertTrue(this.afterSharing("""
                CREATE VIEW J AS SELECT T.a, S.b FROM T JOIN S ON T.k = S.k;
                CREATE VIEW A AS SELECT k, MAX(a) AS m FROM T GROUP BY k;""").joinSharesWithAggregate());
    }

    @Test
    public void argMaxPairValueIsNotWidened() {
        // Sharing is not possible: the integral ARG_MAX keeps of its input and the one the
        // join keeps of its T input cannot be shared.
        var inputs = this.afterSharing("""
                CREATE VIEW J AS SELECT T.a, T.s, S.b FROM T JOIN S ON T.k = S.k;
                CREATE VIEW A AS SELECT k, ARG_MAX(s, a) AS m FROM T GROUP BY k;""");
        Assert.assertEquals(1, inputs.afterSharing().loweredAggregates);
        Assert.assertFalse(inputs.joinSharesWithAggregate());
    }

    @Test
    public void topKSharesIndex() {
        // The integral TopK keeps of its input and the one the join keeps of its T input
        // are shared.
        Assert.assertTrue(this.afterSharing("""
                CREATE VIEW TK AS SELECT k, a FROM (
                  SELECT k, a, ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) AS rn FROM T) X WHERE rn <= 2;
                CREATE VIEW J AS SELECT T.a, S.b FROM T JOIN S ON T.k = S.k;""").joinSharesWithTopK());
    }

    @Test
    public void topKValueIsNotWidened() {
        // Sharing is not possible: the integral TopK keeps of its input and the one the join
        // keeps of its T input cannot be shared, because TopK orders by the whole value it
        // reads, and the join needs one more field.
        Assert.assertFalse(this.afterSharing("""
                CREATE VIEW TK AS SELECT k, a FROM (
                  SELECT k, a, ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) AS rn FROM T) X WHERE rn <= 2;
                CREATE VIEW J AS SELECT T.s, S.b FROM T JOIN S ON T.k = S.k;""").joinSharesWithTopK());
    }

    @Test
    public void topKResultsWithSharedIndex() {
        // The integral TopK keeps of its input and the one the join keeps of its T input
        // are shared.
        DBSPCompiler compiler = this.testCompiler();
        ConsumerInputs inputs = recordAfterSharing(compiler);
        compiler.submitStatementsForCompilation(TABLES + """
                CREATE LOCAL VIEW TK AS SELECT k, a FROM (
                  SELECT k, a, ROW_NUMBER() OVER (PARTITION BY k ORDER BY a) AS rn FROM T) X WHERE rn <= 2;
                CREATE LOCAL VIEW J AS SELECT T.k, T.a FROM T JOIN S ON T.k = S.k;
                CREATE VIEW V AS (SELECT * FROM TK) UNION ALL (SELECT * FROM J);""");
        var ccs = this.getCCS(compiler);
        Assert.assertTrue(inputs.joinSharesWithTopK());
        ConsumerInputs atEnd = new ConsumerInputs(compiler);
        ccs.visit(atEnd);
        Assert.assertTrue(atEnd.joinSharesWithTopK());
        // Key 1 has three rows, so TopK keeps its two smallest; the join keeps all of them
        ccs.stepWeightOne("""
                INSERT INTO T VALUES (1, 30, 'x', 0.5), (1, 10, 'y', 1.5), (1, 20, 'z', 2.5), (2, 40, 'w', 3.5);
                INSERT INTO S VALUES (1, 10);""", """
                 k | a
                -------
                 1 | 10
                 1 | 20
                 2 | 40
                 1 | 30
                 1 | 10
                 1 | 20""");
    }

    @Test
    public void sharedIndexResults() {
        // The integral the aggregate keeps of its input and the one the join keeps of its T
        // input are shared; the join reads 's' and the aggregate reads 'a' from its value.
        DBSPCompiler compiler = this.testCompiler();
        ConsumerInputs inputs = recordAfterSharing(compiler);
        compiler.submitStatementsForCompilation(TABLES + """
                CREATE LOCAL VIEW J AS SELECT T.s, S.b FROM T JOIN S ON T.k = S.k;
                CREATE LOCAL VIEW A AS SELECT CAST(k AS VARCHAR) AS s, BIT_OR(a) AS b FROM T GROUP BY k;
                CREATE VIEW V AS (SELECT * FROM J) UNION ALL (SELECT * FROM A);""");
        var ccs = this.getCCS(compiler).withStringTrim();
        Assert.assertTrue(inputs.joinSharesWithAggregate());
        ConsumerInputs atEnd = new ConsumerInputs(compiler);
        ccs.visit(atEnd);
        Assert.assertTrue(atEnd.joinSharesWithAggregate());
        ccs.stepWeightOne("""
                INSERT INTO T VALUES (1, 1, 'x', 0.5), (1, 2, 'y', 1.5), (2, 4, 'z', 2.5);
                INSERT INTO S VALUES (1, 10), (3, 30);""", """
                 s | b
                -------
                 x | 10
                 y | 10
                 1 | 3
                 2 | 4""");
        ccs.step("""
                REMOVE FROM T VALUES (1, 1, 'x', 0.5);
                INSERT INTO S VALUES (2, 20);""", """
                 s | b  | weight
                -----------------
                 x | 10 | -1
                 1 | 3  | -1
                 1 | 2  | 1
                 z | 20 | 1""");
    }

    // ShareOutputIntegrators: a join reads an integrated output directly instead of re-indexing it

    @Test
    public void joinReadsMaterializedAggregateDirectly() {
        // The join reads the integral of the aggregate output.
        DBSPCompiler compiler = this.testCompiler();
        List<String> joinInputsBefore = new ArrayList<>();
        CircuitVisitor recordBefore = new CircuitVisitor(compiler) {
            @Override
            public void postorder(DBSPJoinBaseOperator join) {
                joinInputsBefore.add(join.left().node().getClass().getSimpleName());
            }
        };
        compiler.optimizerHook = optimizer -> optimizer.insertBefore(ShareOutputIntegrators.class, recordBefore);
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(k INT NOT NULL, d DOUBLE);
                CREATE TABLE S(k INT NOT NULL, b INT);
                CREATE MATERIALIZED VIEW A AS SELECT k, SUM(d) AS sd, SUM(d * 2) AS sd2 FROM T GROUP BY k;
                CREATE VIEW J AS SELECT A.sd, S.b FROM A JOIN S ON A.k = S.k;""");
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        Assert.assertEquals(List.of("DBSPMapIndexOperator"), joinInputsBefore);
        DBSPJoinBaseOperator join = only(circuit, DBSPJoinBaseOperator.class);
        Assert.assertTrue(join.left().node().is(DBSPAggregateOperatorBase.class));
    }

    // SQL produces star joins only by combining the aggregates of one GROUP BY, whose inputs are
    // aggregate outputs rather than indexes, so we synthesize a star join in code.

    private static final DBSPType INT = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false);

    /** Add to {@code circuit} a source table whose columns are all INT. */
    private static DBSPSourceMultisetOperator table(DBSPCircuit circuit, String name, String... columns) {
        ProgramIdentifier tableName = new ProgramIdentifier(name);
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        List<InputColumnMetadata> columnMetadata = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            ProgramIdentifier column = new ProgramIdentifier(columns[i]);
            fields.add(new DBSPTypeStruct.Field(CalciteObject.EMPTY, column, i, INT));
            columnMetadata.add(new InputColumnMetadata(
                    CalciteObject.EMPTY, column, INT, false, null, null, null));
        }
        DBSPTypeStruct struct = new DBSPTypeStruct(CalciteObject.EMPTY, tableName, fields, false);
        DBSPSourceMultisetOperator result = new DBSPSourceMultisetOperator(
                CalciteEmptyRel.INSTANCE, CalciteObject.EMPTY, new DBSPTypeZSet(struct.toTuple()), struct,
                new TableMetadata(tableName, columnMetadata, new ArrayList<>(), null, false, false, null),
                tableName, StreamKind.DELTA, null);
        circuit.addOperator(result);
        return result;
    }

    /** Add to {@code circuit} an index of {@code source} on its first column whose value is column {@code valueColumn} */
    private static OutputPort index(DBSPCircuit circuit, DBSPSourceMultisetOperator source, int valueColumn) {
        DBSPVariablePath row = source.getOutputZSetElementType().ref().var();
        DBSPClosureExpression function = new DBSPRawTupleExpression(
                new DBSPTupleExpression(row.deref().field(0)),
                new DBSPTupleExpression(row.deref().field(valueColumn))).closure(row);
        DBSPMapIndexOperator result = new DBSPMapIndexOperator(CalciteEmptyRel.INSTANCE, function, source.outputPort());
        circuit.addOperator(result);
        return result.outputPort();
    }

    /** Add to {@code circuit} a view of {@code input} */
    private static void view(DBSPCircuit circuit, String name, DBSPSimpleOperator input) {
        ProgramIdentifier viewName = new ProgramIdentifier(name);
        DBSPTypeTuple row = input.getOutputZSetElementType().to(DBSPTypeTuple.class);
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        for (int i = 0; i < row.size(); i++)
            fields.add(new DBSPTypeStruct.Field(
                    CalciteObject.EMPTY, new ProgramIdentifier("c" + i), i, row.getFieldType(i)));
        DBSPTypeStruct struct = new DBSPTypeStruct(CalciteObject.EMPTY, viewName, fields, false);
        ViewMetadata metadata = new ViewMetadata(
                viewName, new ArrayList<>(), SqlCreateView.ViewKind.STANDARD, -1, false, false, null);
        circuit.addOperator(new DBSPSinkOperator(
                CalciteEmptyRel.INSTANCE, viewName, name, struct, metadata, input.outputPort()));
    }

    /** A join function that returns the first value field of each input */
    private static DBSPClosureExpression firstFields(List<OutputPort> inputs) {
        List<DBSPVariablePath> parameters = new ArrayList<>();
        parameters.add(inputs.get(0).getOutputIndexedZSetType().keyType.ref().var());
        List<DBSPExpression> fields = new ArrayList<>();
        for (OutputPort input : inputs) {
            DBSPVariablePath value = input.getOutputIndexedZSetType().elementType.ref().var();
            parameters.add(value);
            fields.add(value.deref().field(0));
        }
        return new DBSPTupleExpression(fields, false).closure(parameters.toArray(new DBSPVariablePath[0]));
    }

    private static <T extends DBSPOperator> List<T> operators(DBSPCircuit circuit, Class<T> clazz) {
        return circuit.allOperators.stream().filter(clazz::isInstance).map(clazz::cast).toList();
    }

    /** The only operator of class {@code clazz} in {@code circuit} */
    private static <T extends DBSPOperator> T only(DBSPCircuit circuit, Class<T> clazz) {
        List<T> found = operators(circuit, clazz);
        Assert.assertEquals(clazz.getSimpleName(), 1, found.size());
        return found.get(0);
    }

    private static DBSPFlatmap flatmap(IdShuffle shuffle) {
        var type = new DBSPTypeTuple(new DBSPTypeArray(INT, false));
        var row = type.ref().var();
        return new DBSPFlatmap(CalciteObject.EMPTY, type, row.deref().field(0).closure(row),
                List.of(), null, null, shuffle);
    }

    @Test
    public void sharedIndexesCompareShuffleIdentity() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustSharedIndexes"));
        for (boolean nativePasses : List.of(false, true)) {
            var compiler = this.testCompiler();
            compiler.options.ioOptions.rustOptimizerPasses = nativePasses;
            var decisions = new HashSet<String>();
            compiler.stateObserver = new DBSPCompiler.StateObserver() {
                @Override
                public void nativeDecision(String operation, boolean answered) {
                    Assert.assertTrue(operation, answered);
                    decisions.add(operation);
                }

                @Override
                public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {}

                @Override
                public void after(String parent, int index, int iteration, CircuitTransform pass,
                                  DBSPCircuit circuit, boolean completed) {}
            };
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            var source = table(circuit, "T", "k");
            var shared = new IdShuffle(1);
            var indexes = new ArrayList<OutputPort>();
            var functions = new ArrayList<DBSPClosureExpression>();
            for (int i = 0; i < 4; i++) {
                var row = source.getOutputZSetElementType().ref().var();
                var tag = new DBSPI32Literal(i == 2 ? 2 : 1);
                var key = new DBSPTupleExpression(tag, flatmap(i == 3 ? new IdShuffle(1) : shared));
                var value = new DBSPTupleExpression(flatmap(shared), flatmap(new IdShuffle(1)));
                var function = new DBSPRawTupleExpression(key, value).closure(row);
                functions.add(function);
                var index = new DBSPMapIndexOperator(CalciteEmptyRel.INSTANCE, function, source.outputPort());
                circuit.addOperator(index);
                indexes.add(index.outputPort());
            }
            for (int i = 0; i < indexes.size(); i += 2) {
                var inputs = indexes.subList(i, i + 2);
                var reader = firstFields(inputs);
                circuit.addOperator(new DBSPJoinOperator(CalciteEmptyRel.INSTANCE,
                        new DBSPTypeZSet(reader.getResultType()), reader, inputs.get(0), inputs.get(1), false));
            }

            var graph = new Graph(compiler);
            graph.apply(circuit);
            var find = new FindSharedIndexes(compiler, graph.getGraphs());
            var snapshots = new OptimizerSnapshots(compiler);
            (nativePasses ? new RustCircuitPass(compiler, find, snapshots) : find).apply(circuit);
            var groups = find.candidateIds();
            Assert.assertEquals(1, groups.size());
            Assert.assertEquals(indexes.subList(0, 2).stream().map(port -> port.operator.id).toList(),
                    groups.get(0).stream().map(member -> member[0]).toList());

            var replace = new ReplaceSharedIndexes(compiler, find);
            var result = (nativePasses ? new RustCircuitPass(compiler, replace, snapshots) : replace).apply(circuit);
            var join = operators(result, DBSPJoinOperator.class).getFirst();
            Assert.assertSame(join.left().node(), join.right().node());
            Assert.assertEquals(3, join.left().getOutputIndexedZSetType().elementType.to(DBSPTypeTuple.class).size());
            if (nativePasses) {
                var pair = functions.subList(0, 2);
                Assert.assertArrayEquals(new int[][]{{0, 1}},
                        RustCircuitPass.splitSharedIndexes(compiler, pair, List.of(false, false)));
                Assert.assertEquals(List.of(List.of(0, 1), List.of(0, 2)),
                        RustCircuitPass.wideIndexFields(compiler, pair).layout());
                Assert.assertTrue(decisions.containsAll(Set.of("SharedIndexes/Discover", "ReplaceSharedIndexes",
                        "SharedIndexes/Split", "SharedIndexes/WideFields")));
            }
        }
    }

    @Test
    public void sharedIndexColumnErrorPreservesTheComparisonPrefix() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustSharedIndexes"));
        var compiler = this.testCompiler();
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var source = table(circuit, "T", "k");
        var inputs = new ArrayList<OutputPort>();
        for (int i = 0; i < 2; i++) {
            var row = source.getOutputZSetElementType().ref().var();
            // A projected column can retain a free path. Comparing distinct
            // paths in the empty index-field scope diagnoses the new one first.
            var path = new DBSPVariablePath("free" + i, INT);
            var divided = new DBSPBinaryExpression(CalciteObject.EMPTY, INT, i == 0 ? DBSPOpcode.DIV : DBSPOpcode.MOD,
                    new DBSPI32Literal(i + 1), new DBSPI32Literal(0));
            var value = i == 0 ? new DBSPTupleExpression(divided, path) : new DBSPTupleExpression(path, divided);
            var function = new DBSPRawTupleExpression(new DBSPTupleExpression(row.deref().field(0)), value).closure(row);
            var index = new DBSPMapIndexOperator(CalciteEmptyRel.INSTANCE, function, source.outputPort());
            circuit.addOperator(index);
            inputs.add(index.outputPort());
        }
        var reader = firstFields(inputs);
        var join = new DBSPJoinOperator(CalciteEmptyRel.INSTANCE, new DBSPTypeZSet(reader.getResultType()),
                reader, inputs.get(0), inputs.get(1), false);
        circuit.addOperator(join);
        var left = inputs.get(0).node();
        var right = inputs.get(1).node();
        // Supply the finder result at the replacement boundary so both passes
        // see the same malformed value columns before either tries to widen them.
        var find = new FindSharedIndexes(compiler, new Graph(compiler).getGraphs());
        find.record(new FindSharedIndexes.IndexReader[][]{{
                new FindSharedIndexes.IndexReader(left.id, join.id, 0),
                new FindSharedIndexes.IndexReader(right.id, join.id, 1)}},
                new long[0], Map.of(left.id, left, right.id, right, join.id, join));
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = Assert.assertThrows(InternalCompilerError.class,
                () -> new ReplaceSharedIndexes(compiler, find).apply(circuit));
        String diagnostic = direct.getMessage().lines().findFirst().orElseThrow();
        Assert.assertEquals("Declaration for variable 'free1' not found", diagnostic);
        long names = DBSPVariablePath.nextGeneratedName() - start;
        var warnings = compiler.messages.replaySnapshot();
        Assert.assertTrue(compiler.messages.warningCount() > 0);

        var hybridCompiler = this.testCompiler();
        hybridCompiler.options.ioOptions.rustOptimizerPasses = true;
        var decisions = new ArrayList<String>();
        hybridCompiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override
            public void nativeDecision(String operation, boolean answered) {
                decisions.add(operation + "=" + answered);
            }

            @Override
            public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {}

            @Override
            public void after(String parent, int index, int iteration, CircuitTransform pass,
                              DBSPCircuit circuit, boolean completed) {}
        };
        start = DBSPVariablePath.nextGeneratedName();
        var hybrid = Assert.assertThrows(direct.getClass(), () -> new RustCircuitPass(
                hybridCompiler, new ReplaceSharedIndexes(hybridCompiler, find), new OptimizerSnapshots(hybridCompiler)).apply(circuit));
        Assert.assertEquals(diagnostic, hybrid.getMessage().lines().findFirst().orElseThrow());
        Assert.assertEquals(direct.getPositionRange(), hybrid.getPositionRange());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
        Assert.assertEquals(warnings, hybridCompiler.messages.replaySnapshot());
        Assert.assertEquals(List.of("ReplaceSharedIndexes=true"), decisions);
    }

    /** Two joins read indexes of T whose values are {@code first} and {@code second}.
     * The shared index keeps both values, each with its own type. */
    private void sharedIndexKeepsBothValues(DBSPExpression first, DBSPExpression second) {
        DBSPCompiler compiler = this.testCompiler();
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), false);
        DBSPSourceMultisetOperator t = table(circuit, "T", "k");
        DBSPSourceMultisetOperator s = table(circuit, "S", "k", "b");
        List<DBSPExpression> values = List.of(first, second);
        for (int i = 0; i < values.size(); i++) {
            DBSPVariablePath row = t.getOutputZSetElementType().ref().var();
            DBSPMapIndexOperator index = new DBSPMapIndexOperator(CalciteEmptyRel.INSTANCE,
                    new DBSPRawTupleExpression(
                            new DBSPTupleExpression(row.deref().field(0)),
                            new DBSPTupleExpression(values.get(i))).closure(row), t.outputPort());
            circuit.addOperator(index);
            List<OutputPort> joinInputs = Linq.list(index.outputPort(), index(circuit, s, 1));
            DBSPClosureExpression joinFunction = firstFields(joinInputs);
            DBSPJoinOperator join = new DBSPJoinOperator(CalciteEmptyRel.INSTANCE,
                    new DBSPTypeZSet(joinFunction.getResultType()), joinFunction,
                    joinInputs.get(0), joinInputs.get(1), false);
            circuit.addOperator(join);
            view(circuit, "J" + i, join);
        }
        DBSPCircuit shared = new ShareIndexes(compiler).apply(circuit);
        List<DBSPJoinBaseOperator> joins = operators(shared, DBSPJoinBaseOperator.class);
        Assert.assertEquals(2, joins.size());
        Assert.assertSame(joins.get(0).left().node(), joins.get(1).left().node());
        Assert.assertEquals(2, joins.get(0).left().getOutputIndexedZSetType().elementType
                .to(DBSPTypeTuple.class).size());
    }

    /** Index values that differ only in nullability. */
    @Test
    public void issue7832() {
        // The circuits are built by hand: no SQL program is known that gives two indexes
        // of one input whose values differ only in nullability
        this.sharedIndexKeepsBothValues(new DBSPI32Literal(1), new DBSPI32Literal(1, true));
        this.sharedIndexKeepsBothValues(
                new DBSPArrayExpression(false, new DBSPI32Literal(1)),
                new DBSPArrayExpression(true, new DBSPI32Literal(1)));
        this.sharedIndexKeepsBothValues(
                new DBSPMapExpression(new DBSPTypeMap(INT, INT, false),
                        Linq.list(new DBSPI32Literal(1)), Linq.list(new DBSPI32Literal(2))),
                new DBSPMapExpression(new DBSPTypeMap(INT, INT, true),
                        Linq.list(new DBSPI32Literal(1)), Linq.list(new DBSPI32Literal(2))));
        this.sharedIndexKeepsBothValues(
                new DBSPVariantExpression(new DBSPI32Literal(1), false),
                new DBSPVariantExpression(new DBSPI32Literal(1), true));
        this.sharedIndexKeepsBothValues(
                new DBSPGeoPointConstructor(CalciteObject.EMPTY, new DBSPDoubleLiteral(1.0),
                        new DBSPDoubleLiteral(2.0), DBSPTypeGeoPoint.create(CalciteObject.EMPTY, false)),
                new DBSPGeoPointConstructor(CalciteObject.EMPTY, new DBSPDoubleLiteral(1.0),
                        new DBSPDoubleLiteral(2.0), DBSPTypeGeoPoint.create(CalciteObject.EMPTY, true)));
    }

    @Test
    public void starJoinSharesIndex() {
        // The integrals the star join and the join keep of their T inputs are shared, and so
        // are the ones they keep of their S inputs.
        DBSPCompiler compiler = this.testCompiler();
        DBSPCircuit circuit = new DBSPCircuit(new ProgramMetadata(), false);
        DBSPSourceMultisetOperator t = table(circuit, "T", "k", "a", "s");
        DBSPSourceMultisetOperator s = table(circuit, "S", "k", "b");
        DBSPSourceMultisetOperator u = table(circuit, "U", "k", "c");

        // T indexed on k twice: value 'a' for the star join and value 's' for the join
        List<OutputPort> starInputs = Linq.list(index(circuit, t, 1), index(circuit, s, 1), index(circuit, u, 1));
        DBSPClosureExpression starFunction = firstFields(starInputs);
        DBSPStarJoinOperator star = new DBSPStarJoinOperator(CalciteEmptyRel.INSTANCE,
                new DBSPTypeZSet(starFunction.getResultType()), starFunction, starInputs);
        circuit.addOperator(star);
        view(circuit, "SJ", star);

        List<OutputPort> joinInputs = Linq.list(index(circuit, t, 2), index(circuit, s, 1));
        DBSPClosureExpression joinFunction = firstFields(joinInputs);
        DBSPJoinOperator join = new DBSPJoinOperator(CalciteEmptyRel.INSTANCE,
                new DBSPTypeZSet(joinFunction.getResultType()), joinFunction,
                joinInputs.get(0), joinInputs.get(1), false);
        circuit.addOperator(join);
        view(circuit, "J", join);

        DBSPCircuit shared = new ShareIndexes(compiler).apply(circuit);
        DBSPStarJoinBaseOperator sharedStar = only(shared, DBSPStarJoinBaseOperator.class);
        DBSPJoinBaseOperator sharedJoin = only(shared, DBSPJoinBaseOperator.class);
        // The two indexes of T became one, read by both consumers; so did the two of S
        Assert.assertSame(sharedStar.inputs.get(0).node(), sharedJoin.left().node());
        Assert.assertSame(sharedStar.inputs.get(1).node(), sharedJoin.right().node());
        Assert.assertEquals(3, operators(shared, DBSPMapIndexOperator.class).size());
        // The shared value of T holds 'a' and 's'
        Assert.assertEquals(2, sharedStar.inputs.get(0).getOutputIndexedZSetType().elementType
                .to(DBSPTypeTuple.class).size());
        // Each star join parameter reads the value of its input
        DBSPClosureExpression function = sharedStar.getClosureFunction();
        for (int i = 0; i < sharedStar.inputs.size(); i++)
            Assert.assertTrue(function.parameters[i + 1].getType().deref()
                    .sameType(sharedStar.inputs.get(i).getOutputIndexedZSetType().elementType));
    }
}
