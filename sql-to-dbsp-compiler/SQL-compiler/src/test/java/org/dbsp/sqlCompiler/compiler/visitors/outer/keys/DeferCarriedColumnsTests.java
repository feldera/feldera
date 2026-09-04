package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.CollectionShape;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Tests for {@link DeferCarriedColumns}: the columns a chain of LEFT JOINs only carries are
 * fetched once at the end instead. */
public class DeferCarriedColumnsTests extends SqlIoTest {
    static final String TABLES = """
            CREATE TABLE fact(id INT NOT NULL PRIMARY KEY, a INT, b INT, c INT, d INT,
                              p1 VARCHAR, p2 VARCHAR, p3 VARCHAR);
            CREATE TABLE l1(k INT NOT NULL PRIMARY KEY, v1 VARCHAR);
            CREATE TABLE l2(k INT NOT NULL PRIMARY KEY, v2 VARCHAR);
            CREATE TABLE l3(k INT NOT NULL PRIMARY KEY, v3 VARCHAR);
            CREATE TABLE l4(k INT NOT NULL, v4 VARCHAR);
            """;

    static final String CHAIN = """
            CREATE VIEW v AS SELECT fact.id, fact.p1, fact.p2, fact.p3,
                                    l1.v1, l2.v2, l3.v3, l4.v4
            FROM fact
            LEFT JOIN l1 ON fact.a = l1.k
            LEFT JOIN l2 ON fact.b = l2.k
            LEFT JOIN l3 ON fact.c = l3.k
            LEFT JOIN l4 ON fact.d = l4.k;""";

    /** How many columns every join's output row holds. */
    private static class JoinWidths extends CircuitVisitor {
        final List<Integer> widths = new ArrayList<>();

        JoinWidths(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPJoinBaseOperator operator) {
            CollectionShape shape = operator.outputPort().getShape();
            this.widths.add(shape == null ? -1 : shape.width());
        }
    }

    /** The width of every join's output row, sorted, so the check does not depend on the
     * order in which other optimizations leave the joins. */
    private List<Integer> joinWidths(CompilerCircuitStream ccs) {
        JoinWidths widths = new JoinWidths(ccs.compiler);
        ccs.visit(widths);
        Collections.sort(widths.widths);
        return widths.widths;
    }

    /** A compiled program and what the rewrite reported for it. */
    private record Rewritten(CompilerCircuitStream ccs, String log) {}

    private Rewritten compile(String view) {
        StringBuilder builder = new StringBuilder();
        Appendable save = Logger.INSTANCE.setDebugStream(builder);
        Logger.INSTANCE.setLoggingLevel(DeferCarriedColumns.class, 1);
        try {
            DBSPCompiler compiler = this.testCompiler();
            compiler.submitStatementsForCompilation(TABLES + view);
            // Expected tables pad cells to the column width; compare strings trimmed
            return new Rewritten(this.getCCS(compiler).withStringTrim(), builder.toString());
        } finally {
            Logger.INSTANCE.setLoggingLevel(DeferCarriedColumns.class, 0);
            Logger.INSTANCE.setDebugStream(save);
        }
    }

    /** p1, p2 and p3 cross four joins unread, so they are fetched from fact at the end.
     * Row 2 matches no lookup and keeps its payload. */
    @Test
    public void payloadIsDeferred() {
        Rewritten rewritten = this.compile(CHAIN);
        Assert.assertTrue(rewritten.log(),
                rewritten.log().contains("Deferred 3 columns across 4 left joins"));
        // Without the rewrite the result would be (8, 8, 8, 8).
        // The fifth join puts the carried columns back.
        Assert.assertEquals(List.of(5, 5, 5, 6, 8), this.joinWidths(rewritten.ccs()));
        // Results validated using postgres
        rewritten.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 30, 40, 'x', 'y', 'z'),
                                                (2, 99, 99, 99, 99, 'p', 'q', 'r');
                        INSERT INTO l1 VALUES (10, 'A');
                        INSERT INTO l2 VALUES (20, 'B');
                        INSERT INTO l3 VALUES (30, 'C');
                        INSERT INTO l4 VALUES (40, 'D');""",
                """
                         id | p1 | p2 | p3 | v1 | v2 | v3 | v4
                        ---------------------------------------
                         1  | x  | y  | z  | A  | B  | C  | D
                         2  | p  | q  | r  |NULL|NULL|NULL|NULL""");
    }

    /** A chain inside a recursive view: the payload of each edge crosses three lookups
     * unread and is fetched from edge at the end of the chain, on every iteration. */
    @Test
    public void chainInsideARecursiveView() {
        Rewritten rewritten = this.compile("""
                CREATE TABLE edge(src INT NOT NULL, dst INT NOT NULL,
                                  p1 VARCHAR, p2 VARCHAR, p3 VARCHAR, p4 VARCHAR, PRIMARY KEY(src, dst));
                DECLARE RECURSIVE VIEW reach(src INT NOT NULL, dst INT NOT NULL,
                                             p1 VARCHAR, p2 VARCHAR, p3 VARCHAR, p4 VARCHAR,
                                             v1 VARCHAR, v2 VARCHAR, v3 VARCHAR);
                CREATE VIEW reach AS
                SELECT e.src, e.dst, e.p1, e.p2, e.p3, e.p4, l1.v1, l2.v2, l3.v3
                FROM edge e LEFT JOIN l1 ON e.dst = l1.k LEFT JOIN l2 ON e.dst = l2.k LEFT JOIN l3 ON e.dst = l3.k
                UNION
                SELECT r.src, e.dst, e.p1, e.p2, e.p3, e.p4, l1.v1, l2.v2, l3.v3
                FROM edge e LEFT JOIN l1 ON e.dst = l1.k LEFT JOIN l2 ON e.dst = l2.k LEFT JOIN l3 ON e.dst = l3.k
                JOIN reach r ON r.dst = e.src;""");
        Assert.assertTrue(rewritten.log(),
                rewritten.log().contains("Deferred 4 columns across 3 left joins"));
        // Results validated using postgres
        rewritten.ccs().stepWeightOne("""
                        INSERT INTO edge VALUES (1, 2, 'a', 'b', 'c', 'd'), (2, 3, 'e', 'f', 'g', 'h'),
                                                (3, 4, 'i', 'j', 'k', 'l');
                        INSERT INTO l1 VALUES (2, 'A'), (4, 'AA');
                        INSERT INTO l2 VALUES (3, 'B');
                        INSERT INTO l3 VALUES (4, 'C');""",
                """
                         src | dst | p1 | p2 | p3 | p4 | v1 | v2 | v3
                        ---------------------------------------------
                         1   | 2   | a  | b  | c  | d  | A  |NULL|NULL
                         1   | 3   | e  | f  | g  | h  |NULL| B  |NULL
                         1   | 4   | i  | j  | k  | l  | AA |NULL| C
                         2   | 3   | e  | f  | g  | h  |NULL| B  |NULL
                         2   | 4   | i  | j  | k  | l  | AA |NULL| C
                         3   | 4   | i  | j  | k  | l  | AA |NULL| C""");
    }

    /** The start has two keys: the primary key id, which no join reads, and the TOP-1
     * partition columns a and b, which every join reads.  The analysis picks a and b, so id
     * is deferred too and the lookup reuses the start's index. */
    @Test
    public void keyReadByTheChainIsPreferred() {
        Rewritten rewritten = this.compile("""
                CREATE TABLE keyed(id INT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NOT NULL,
                                   p1 VARCHAR, p2 VARCHAR, p3 VARCHAR);
                CREATE LOCAL VIEW first AS SELECT id, a, b, p1, p2, p3 FROM keyed
                QUALIFY row_number() OVER (PARTITION BY a, b ORDER BY id) = 1;
                CREATE VIEW v AS SELECT first.id, first.a, first.b, first.p1, first.p2, first.p3,
                                        l1.v1, l2.v2, l3.v3, l4.v4
                FROM first
                LEFT JOIN l1 ON first.a = l1.k
                LEFT JOIN l2 ON first.b = l2.k
                LEFT JOIN l3 ON first.a = l3.k
                LEFT JOIN l4 ON first.b = l4.k;""");
        Assert.assertTrue(rewritten.log(),
                rewritten.log().contains("Deferred 4 columns across 4 left joins, reusing the start's index"));
        // Results validated using postgres
        rewritten.ccs().stepWeightOne("""
                        INSERT INTO keyed VALUES (1, 10, 20, 'x', 'y', 'z'), (2, 10, 20, 'p', 'q', 'r'),
                                                 (3, 11, 21, 'u', 'v', 'w');
                        INSERT INTO l1 VALUES (10, 'A'), (11, 'AA');
                        INSERT INTO l2 VALUES (20, 'B');
                        INSERT INTO l3 VALUES (11, 'C');
                        INSERT INTO l4 VALUES (20, 'D'), (21, 'DD');""",
                """
                         id | a  | b  | p1 | p2 | p3 | v1 | v2 | v3 | v4
                        -----------------------------------------------
                         1  | 10 | 20 | x  | y  | z  | A  | B  |NULL| D
                         3  | 11 | 21 | u  | v  | w  | AA |NULL| C  | DD""");
    }

    /** The chain of joins does not have to convert each input row to one output row. */
    @Test
    public void fanOutGetsTheSamePayload() {
        Rewritten rewritten = this.compile(CHAIN);
        Assert.assertEquals(List.of(5, 5, 5, 6, 8), this.joinWidths(rewritten.ccs()));
        // Results validated using postgres
        rewritten.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 30, 40, 'x', 'y', 'z');
                        INSERT INTO l1 VALUES (10, 'A');
                        INSERT INTO l2 VALUES (20, 'B');
                        INSERT INTO l3 VALUES (30, 'C');
                        INSERT INTO l4 VALUES (40, 'D'), (40, 'E');""",
                """
                         id | p1 | p2 | p3 | v1 | v2 | v3 | v4
                        ---------------------------------------
                         1  | x  | y  | z  | A  | B  | C  | D
                         1  | x  | y  | z  | A  | B  | C  | E""");
    }

    /** A filter drops row 2. */
    @Test
    public void droppedRowStaysDropped() {
        Rewritten rewritten = this.compile("""
                CREATE TABLE wide(id INT NOT NULL PRIMARY KEY, a INT, b INT, c INT, d INT,
                                  p1 VARCHAR, p2 VARCHAR, p3 VARCHAR, p4 VARCHAR, p5 VARCHAR);
                CREATE VIEW v AS SELECT wide.id, wide.p1, wide.p2, wide.p3, wide.p4, wide.p5,
                                        l1.v1, l2.v2, l3.v3, l4.v4
                FROM wide
                LEFT JOIN l1 ON wide.a = l1.k
                LEFT JOIN l2 ON wide.b = l2.k
                LEFT JOIN l3 ON wide.c = l3.k
                LEFT JOIN l4 ON wide.d = l4.k
                WHERE wide.p5 <> 'drop';""");
        Assert.assertTrue(rewritten.log(),
                rewritten.log().contains("Deferred 4 columns across 4 left joins"));
        // Without the rewrite each of the four joins carries 10 columns
        Assert.assertEquals(List.of(6, 6, 6, 7, 10), this.joinWidths(rewritten.ccs()));
        // Results validated using postgres
        rewritten.ccs().stepWeightOne("""
                        INSERT INTO wide VALUES (1, 10, 20, 30, 40, 'v', 'w', 'x', 'y', 'z'),
                                                (2, 10, 20, 30, 40, 'a', 'b', 'c', 'd', 'drop'),
                                                (3, 99, 99, 99, 99, 'e', 'f', 'g', 'h', 'i');
                        INSERT INTO l1 VALUES (10, 'A');
                        INSERT INTO l2 VALUES (20, 'B');
                        INSERT INTO l3 VALUES (30, 'C');
                        INSERT INTO l4 VALUES (40, 'D');""",
                """
                         id | p1 | p2 | p3 | p4 | p5 | v1 | v2 | v3 | v4
                        -------------------------------------------------
                         1  | v  | w  | x  | y  | z  | A  | B  | C  | D
                         3  | e  | f  | g  | h  | i  |NULL|NULL|NULL|NULL""");
    }

    /** A chain headed by a TOP-1 */
    @Test
    public void lookupReusesTheStartIndex() {
        Rewritten rewritten = this.compile("""
                CREATE TABLE raw(id INT NOT NULL, ver INT NOT NULL, a INT, b INT, c INT, d INT,
                                 p1 VARCHAR, p2 VARCHAR, p3 VARCHAR);
                CREATE LOCAL VIEW latest AS SELECT id, a, b, c, d, p1, p2, p3 FROM raw
                QUALIFY row_number() OVER (PARTITION BY id ORDER BY ver DESC) = 1;
                CREATE VIEW v AS SELECT latest.id, latest.p1, latest.p2, latest.p3,
                                        l1.v1, l2.v2, l3.v3, l4.v4
                FROM latest
                LEFT JOIN l1 ON latest.a = l1.k
                LEFT JOIN l2 ON latest.b = l2.k
                LEFT JOIN l3 ON latest.c = l3.k
                LEFT JOIN l4 ON latest.d = l4.k;""");
        Assert.assertTrue(rewritten.log(),
                rewritten.log().contains("Deferred 3 columns across 4 left joins, reusing the start's index"));
        Assert.assertEquals(List.of(5, 5, 5, 6, 8), this.joinWidths(rewritten.ccs()));
        // Results validated using postgres
        rewritten.ccs().stepWeightOne("""
                        INSERT INTO raw VALUES (1, 1, 10, 20, 30, 40, 'old', 'old', 'old'),
                                               (1, 2, 10, 20, 30, 40, 'x', 'y', 'z'),
                                               (2, 1, 99, 99, 99, 99, 'p', 'q', 'r');
                        INSERT INTO l1 VALUES (10, 'A');
                        INSERT INTO l2 VALUES (20, 'B');
                        INSERT INTO l3 VALUES (30, 'C');
                        INSERT INTO l4 VALUES (40, 'D');""",
                """
                         id | p1 | p2 | p3 | v1 | v2 | v3 | v4
                        ---------------------------------------
                         1  | x  | y  | z  | A  | B  | C  | D
                         2  | p  | q  | r  |NULL|NULL|NULL|NULL""");
    }

    /** The first two joins are shared by two branches.  This creates two disjoint chains,
     * of lengths 4 and 2 respectively. */
    @Test
    public void forkedChainDefersEachBranch() {
        Rewritten rewritten = this.compile("""
                CREATE LOCAL VIEW shared AS
                SELECT fact.id, fact.a, fact.b, fact.c, fact.d, fact.p1, fact.p2, fact.p3, l1.v1, l2.v2
                FROM fact LEFT JOIN l1 ON fact.a = l1.k LEFT JOIN l2 ON fact.b = l2.k;
                CREATE VIEW v AS
                SELECT shared.id, shared.p1, shared.p2, shared.p3, shared.v1, shared.v2, l3.v3, l4.v4
                FROM shared LEFT JOIN l3 ON shared.c = l3.k LEFT JOIN l4 ON shared.d = l4.k
                UNION ALL
                SELECT shared.id, shared.p1, shared.p2, shared.p3, shared.v1, shared.v2, l4.v4, l3.v3
                FROM shared LEFT JOIN l4 ON shared.c = l4.k LEFT JOIN l3 ON shared.d = l3.k;""");
        Assert.assertTrue(rewritten.log(),
                rewritten.log().contains("Deferred 3 columns across 4 left joins"));
        Assert.assertTrue(rewritten.log(),
                rewritten.log().contains("Deferred 5 columns across 2 left joins"));
        // Results validated using postgres
        rewritten.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 30, 40, 'x', 'y', 'z');
                        INSERT INTO l1 VALUES (10, 'A');
                        INSERT INTO l2 VALUES (20, 'B');
                        INSERT INTO l3 VALUES (30, 'C');
                        INSERT INTO l4 VALUES (40, 'D');""",
                """
                         id | p1 | p2 | p3 | v1 | v2 | v3 | v4
                        ---------------------------------------
                         1  | x  | y  | z  | A  | B  | C  | D
                         1  | x  | y  | z  | A  | B  |NULL|NULL""");
    }
}
