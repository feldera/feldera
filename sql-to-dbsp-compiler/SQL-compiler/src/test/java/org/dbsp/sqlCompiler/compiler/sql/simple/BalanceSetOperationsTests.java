package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.BalanceSetOperations;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.IntFunction;

/** Tests for {@link BalanceSetOperations} */
public class BalanceSetOperationsTests extends SqlIoTest {
    /** Counts the distinct operators in the circuit */
    static class CountDistinct extends CircuitVisitor {
        int count = 0;

        CountDistinct(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPDistinctOperator operator) {
            this.count++;
        }

        @Override
        public void postorder(DBSPStreamDistinctOperator operator) {
            this.count++;
        }
    }

    /** The queries query(0), ..., query(count - 1) combined with 'operator' */
    static String chain(int count, IntFunction<String> query, String operator) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0)
                builder.append(" ").append(operator).append(" ");
            builder.append(query.apply(i));
        }
        return builder.toString();
    }

    /** A view that combines tables t0, ..., t{count - 1} using 'operator'.  For count 3 and UNION ALL:
     * <pre>
     * CREATE VIEW v AS SELECT x FROM t0 UNION ALL SELECT x FROM t1 UNION ALL SELECT x FROM t2;
     * </pre> */
    static String chainView(int count, String operator) {
        return "CREATE VIEW v AS " + chain(count, i -> "SELECT x FROM t" + i, operator) + ";\n";
    }

    /** Local views 'oracle' and 'tested' with queries 'oracle' and 'tested', and view D with the rows
     * that only one of them produces.  Each step expects D to stay empty.
     * <pre>
     * CREATE LOCAL VIEW oracle AS ...;
     * CREATE LOCAL VIEW tested AS ...;
     * CREATE VIEW D AS
     * SELECT 'oracle' AS side, * FROM (SELECT * FROM oracle EXCEPT ALL SELECT * FROM tested) AS oracle_extra
     * UNION ALL
     * SELECT 'tested' AS side, * FROM (SELECT * FROM tested EXCEPT ALL SELECT * FROM oracle) AS tested_extra;
     * </pre> */
    static String differenceViews(String oracle, String tested) {
        return "CREATE LOCAL VIEW oracle AS " + oracle + ";\n" +
                "CREATE LOCAL VIEW tested AS " + tested + ";\n" + """
                CREATE VIEW D AS
                SELECT 'oracle' AS side, * FROM (SELECT * FROM oracle EXCEPT ALL SELECT * FROM tested) AS oracle_extra
                UNION ALL
                SELECT 'tested' AS side, * FROM (SELECT * FROM tested EXCEPT ALL SELECT * FROM oracle) AS tested_extra;""";
    }

    /** The chainView over tables with the same column type.  For count 2 and UNION:
     * <pre>
     * CREATE TABLE t0(x INT);
     * CREATE TABLE t1(x INT);
     * CREATE VIEW v AS SELECT x FROM t0 UNION SELECT x FROM t1;
     * </pre> */
    static String chainOfTables(int count, String operator) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++)
            builder.append("CREATE TABLE t").append(i).append("(x INT);\n");
        return builder.append(chainView(count, operator)).toString();
    }

    /** The number of nested set operations at the root of 'node' */
    static int setOperationDepth(SqlNode node) {
        if (!(node instanceof SqlCall call) || !(call.getOperator() instanceof SqlSetOperator))
            return 0;
        int depth = 0;
        for (SqlNode operand : call.getOperandList())
            depth = Math.max(depth, setOperationDepth(operand));
        return depth + 1;
    }

    /** The depth of the set operations of the parsed chainView */
    int parsedChainDepth(int count) throws SqlParseException {
        DBSPCompiler compiler = this.testCompiler();
        List<ParsedStatement> statements = compiler.sqlToRelCompiler.parseStatements(chainView(count, "UNION ALL"));
        SqlCreateView view = (SqlCreateView) statements.get(0).statement();
        return setOperationDepth(view.query);
    }

    /** The depth of the set operations of the validated 'query' */
    int validatedDepth(String query) throws SqlParseException {
        DBSPCompiler compiler = this.testCompiler();
        List<ParsedStatement> statements = compiler.sqlToRelCompiler.parseStatements(
                "CREATE VIEW v AS " + query + ";");
        SqlCreateView view = (SqlCreateView) statements.get(0).statement();
        return setOperationDepth(compiler.sqlToRelCompiler.validate(view.query));
    }

    /** A long chain becomes a balanced tree; a chain of MAX_CHAIN queries keeps the left-deep shape */
    @Test
    public void testBalanced() throws SqlParseException {
        Assert.assertEquals(10, this.parsedChainDepth(1000));
        Assert.assertEquals(BalanceSetOperations.MAX_CHAIN - 1, this.parsedChainDepth(BalanceSetOperations.MAX_CHAIN));
    }

    /** The balanced tree becomes one set operation, so the UNION needs a single distinct */
    @Test
    public void testUnionDistinct() {
        CompilerCircuit cc = this.getCC(chainOfTables(300, "UNION"));
        CountDistinct counter = new CountDistinct(cc.compiler);
        cc.visit(counter);
        Assert.assertEquals(1, counter.count);
    }

    /** A chain with mixed column types keeps the SQL grouping, in which each BIGINT is cast once, to DOUBLE.
     * In a balanced tree, the BIGINT columns in the subtree with the REAL column would be cast to REAL first,
     * and 123456789 would become 123456792.  Validated on Postgres.  The program:
     * <pre>
     * CREATE TABLE t(b BIGINT, r REAL, d DOUBLE);
     * CREATE VIEW v AS SELECT DISTINCT CAST(c AS BIGINT) AS c FROM (
     *     SELECT d AS c FROM t                   -- query 0
     *     UNION ALL SELECT b FROM t              -- queries 1 to 98
     *     ...
     *     UNION ALL SELECT r FROM t              -- query 99
     *     UNION ALL SELECT b FROM t              -- query 100
     * ) AS s;
     * </pre> */
    @Test
    public void testMixedTypes() {
        String chain = chain(101, i -> i == 0 ? "SELECT d AS c FROM t" : i == 99 ? "SELECT r FROM t" : "SELECT b FROM t",
                "UNION ALL");
        var ccs = this.getCCS("CREATE TABLE t(b BIGINT, r REAL, d DOUBLE);\n" +
                "CREATE VIEW v AS SELECT DISTINCT CAST(c AS BIGINT) AS c FROM (" + chain + ") AS s;");
        ccs.stepWeightOne("INSERT INTO t VALUES(123456789, 0, 0);", """
                 c
                -----------
                 0
                 123456789""");
    }

    /** Strings of different lengths keep the balanced tree during validation;
     * numbers of different types make the validator use the left-deep form */
    @Test
    public void testValidatedShape() throws SqlParseException {
        String[] strings = { "'x'", "'xy'", "CAST('a' AS CHAR(3))", "CAST('abc' AS VARCHAR(5))" };
        Assert.assertEquals(8, this.validatedDepth(
                chain(200, i -> "SELECT " + strings[i % strings.length], "UNION ALL")));
        Assert.assertEquals(199, this.validatedDepth(
                chain(200, i -> i % 2 == 0 ? "SELECT 1" : "SELECT CAST(1 AS BIGINT)", "UNION ALL")));
    }

    /** A long chain of strings with different lengths has the rows of a short, left-deep, chain.
     * The program:
     * <pre>
     * CREATE TABLE t(c CHAR(3), v VARCHAR(5));
     * CREATE LOCAL VIEW oracle AS SELECT DISTINCT s FROM (
     *     SELECT 'x' AS s UNION ALL SELECT 'xy' UNION ALL SELECT c FROM t UNION ALL SELECT v FROM t) AS q;
     * CREATE LOCAL VIEW tested AS SELECT DISTINCT s FROM (
     *     SELECT 'x' AS s UNION ALL SELECT 'xy' UNION ALL SELECT c FROM t UNION ALL SELECT v FROM t
     *     UNION ALL SELECT 'x' ...                -- 200 queries
     * ) AS q;
     * CREATE VIEW D AS ...
     * </pre> */
    @Test
    public void testStrings() {
        String[] queries = { "SELECT 'x' AS s", "SELECT 'xy'", "SELECT c FROM t", "SELECT v FROM t" };
        String oracle = chain(queries.length, i -> queries[i], "UNION ALL");
        String tested = chain(200, i -> queries[i % queries.length], "UNION ALL");
        var ccs = this.getCCS("CREATE TABLE t(c CHAR(3), v VARCHAR(5));\n" + differenceViews(
                "SELECT DISTINCT s FROM (" + oracle + ") AS q",
                "SELECT DISTINCT s FROM (" + tested + ") AS q"));
        ccs.stepWeightOne("INSERT INTO t VALUES('a', 'ab'), ('abc', 'abcde'), (NULL, NULL);", """
                 side | s
                -----------""");
    }

    /** Long INTERSECT and INTERSECT ALL chains have the rows of the short chains.  The program for INTERSECT:
     * <pre>
     * CREATE TABLE t(x INT);
     * CREATE TABLE s(x INT);
     * CREATE LOCAL VIEW oracle AS SELECT x FROM t INTERSECT SELECT x FROM s;
     * CREATE LOCAL VIEW tested AS SELECT x FROM t INTERSECT SELECT x FROM t ...
     *     INTERSECT SELECT x FROM s ...           -- 300 queries, s in query 150
     * CREATE VIEW D AS ...
     * </pre> */
    @Test
    public void testIntersect() {
        for (String operator : List.of("INTERSECT", "INTERSECT ALL")) {
            String oracle = "SELECT x FROM t " + operator + " SELECT x FROM s";
            String tested = chain(300, i -> i == 150 ? "SELECT x FROM s" : "SELECT x FROM t", operator);
            var ccs = this.getCCS("CREATE TABLE t(x INT);\nCREATE TABLE s(x INT);\n" +
                    differenceViews(oracle, tested));
            ccs.stepWeightOne("INSERT INTO t VALUES(1), (1), (2), (3);\nINSERT INTO s VALUES(1), (1), (1), (2), (4);", """
                     side | x
                    -----------""");
        }
    }

    /** A long chain with mixed column types in an IN sub-query uses the left-deep form,
     * and has the rows of a short chain.  The program:
     * <pre>
     * CREATE TABLE t(b BIGINT);
     * CREATE TABLE s(b BIGINT, d DOUBLE);
     * CREATE LOCAL VIEW oracle AS SELECT b FROM t WHERE b IN (SELECT b FROM s UNION ALL SELECT d FROM s);
     * CREATE LOCAL VIEW tested AS SELECT b FROM t WHERE b IN (
     *     SELECT b FROM s UNION ALL SELECT b FROM s ...
     *     UNION ALL SELECT d FROM s ...           -- 200 queries, d in query 150
     * );
     * CREATE VIEW D AS ...
     * </pre> */
    @Test
    public void testMixedTypesInSubQuery() {
        String oracle = "SELECT b FROM s UNION ALL SELECT d FROM s";
        String tested = chain(200, i -> i == 150 ? "SELECT d FROM s" : "SELECT b FROM s", "UNION ALL");
        var ccs = this.getCCS("CREATE TABLE t(b BIGINT);\nCREATE TABLE s(b BIGINT, d DOUBLE);\n" + differenceViews(
                "SELECT b FROM t WHERE b IN (" + oracle + ")",
                "SELECT b FROM t WHERE b IN (" + tested + ")"));
        ccs.stepWeightOne("INSERT INTO t VALUES(1), (2), (3);\nINSERT INTO s VALUES(1, 2.0);", """
                 side | b
                -----------""");
    }
}
