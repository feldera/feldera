package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Tests for {@link LeftJoinChainsVisitor} */
public class LeftJoinChainsTests extends SqlIoTest {
    static final String TABLES = """
            CREATE TABLE fact(id INT NOT NULL PRIMARY KEY, a INT, b INT, p1 VARCHAR, p2 VARCHAR);
            CREATE TABLE l1(k INT NOT NULL PRIMARY KEY, v1 VARCHAR);
            CREATE TABLE l2(k INT NOT NULL PRIMARY KEY, v2 VARCHAR);
            """;

    /** A compiled program and what the LeftJoinChainsVisitor analysis logged about it. */
    private record Analyzed(CompilerCircuitStream ccs, String log) {}

    /** Whether the circuit holds a distinct operator. */
    private boolean hasDistinct(CompilerCircuitStream ccs) {
        boolean[] found = { false };
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPStreamDistinctOperator operator) { found[0] = true; }
            @Override
            public void postorder(DBSPDistinctOperator operator) { found[0] = true; }
        });
        return found[0];
    }

    /** Compile the program with the analysis logging. */
    private Analyzed compile(String view) {
        StringBuilder builder = new StringBuilder();
        Appendable save = Logger.INSTANCE.setDebugStream(builder);
        Logger.INSTANCE.setLoggingLevel(LeftJoinChainsVisitor.class, 1);
        try {
            DBSPCompiler compiler = this.testCompiler();
            compiler.submitStatementsForCompilation(TABLES + view);
            return new Analyzed(this.getCCS(compiler).withStringTrim(), builder.toString());
        } finally {
            Logger.INSTANCE.setLoggingLevel(LeftJoinChainsVisitor.class, 0);
            Logger.INSTANCE.setDebugStream(save);
        }
    }

    /** One chain as the analysis reported it: its length and the columns it only carries. */
    private record ChainDescription(int length, String onlyCarried) implements Comparable<ChainDescription> {
        /** Longer chains first, then by the columns, so a list of these has one order. */
        @Override
        public int compareTo(ChainDescription other) {
            if (this.length != other.length)
                return Integer.compare(other.length, this.length);
            return this.onlyCarried.compareTo(other.onlyCarried);
        }

        @Override
        public String toString() {
            return this.length + " " + this.onlyCarried;
        }
    }

    /** Every chain the analysis reported, sorted by length". */
    private List<String> chains(String log) {
        List<ChainDescription> result = new ArrayList<>();
        for (String line : log.split("\n")) {
            Matcher match = Pattern.compile(
                    "chain of (\\d+) left joins .* only carries \\d+ columns: (\\[[^\\]]*\\])").matcher(line);
            if (match.find())
                result.add(new ChainDescription(Integer.parseInt(match.group(1)), match.group(2)));
        }
        Collections.sort(result);
        return Linq.map(result, ChainDescription::toString);
    }

    /** Two left joins in a row form a chain. */
    @Test
    public void twoLeftJoinsAreOneChain() {
        Analyzed analyzed = this.compile("""
                CREATE VIEW v AS SELECT fact.*, l1.v1, l2.v2 FROM fact
                LEFT JOIN l1 ON fact.a = l1.k
                LEFT JOIN l2 ON fact.b = l2.k;""");
        // fact has columns id, a, b, p1, p2
        Assert.assertEquals(List.of("2 [0, 3, 4]"), this.chains(analyzed.log()));
        Assert.assertTrue(analyzed.log(), analyzed.log().contains("carries key [0]"));
        // A rewrite reads the deferred columns back into these positions of the chain output
        Assert.assertTrue(analyzed.log(), analyzed.log().contains("[0, 3, 4] at [0, 3, 4]"));
        // Row 2 matches neither lookup, so both lookup columns are null
        // Results validated using postgres
        analyzed.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 'x', 'y'), (2, 11, 21, 'z', 'w');
                        INSERT INTO l1 VALUES (10, 'A');
                        INSERT INTO l2 VALUES (20, 'B');""", """
                         id | a  | b  | p1 | p2 | v1 | v2
                        ----------------------------------
                         1  | 10 | 20 | x  | y  | A  | B
                         2  | 11 | 21 | z  | w  |NULL|NULL""");
    }

    @Test
    public void conditionColumnIsRead() {
        Analyzed analyzed = this.compile("""
                CREATE VIEW v AS SELECT fact.*, l1.v1, l2.v2 FROM fact
                LEFT JOIN l1 ON fact.a = l1.k
                LEFT JOIN l2 ON fact.p1 = l2.v2;""");
        Assert.assertEquals(List.of("2 [0, 2, 4]"), this.chains(analyzed.log()));
        // Results validated using postgres
        analyzed.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 'x', 'y'), (2, 11, 21, 'z', 'w');
                        INSERT INTO l1 VALUES (10, 'A');
                        INSERT INTO l2 VALUES (30, 'x');""", """
                         id | a  | b  | p1 | p2 | v1 | v2
                        ----------------------------------
                         1  | 10 | 20 | x  | y  | A  | x
                         2  | 11 | 21 | z  | w  |NULL|NULL""");
    }

    /** A single left join is not a chain. */
    @Test
    public void singleLeftJoinIsNotAChain() {
        Analyzed analyzed = this.compile("""
                CREATE VIEW v AS SELECT fact.*, l1.v1 FROM fact
                LEFT JOIN l1 ON fact.a = l1.k;""");
        Assert.assertEquals(List.of(), this.chains(analyzed.log()));
        // Results validated using postgres
        analyzed.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 'x', 'y'), (2, 11, 21, 'z', 'w');
                        INSERT INTO l1 VALUES (10, 'A');""", """
                         id | a  | b  | p1 | p2 | v1
                        -----------------------------
                         1  | 10 | 20 | x  | y  | A
                         2  | 11 | 21 | z  | w  |NULL""");
    }

    /** Chain that duplicates an input row. */
    @Test
    public void fanOutKeepsColumnsCarried() {
        Analyzed analyzed = this.compile("""
                CREATE TABLE many(k INT NOT NULL, w VARCHAR);
                CREATE VIEW v AS SELECT fact.id, fact.p1, many.w, l1.v1 FROM fact
                LEFT JOIN many ON fact.a = many.k
                LEFT JOIN l1 ON fact.b = l1.k;""");
        Assert.assertEquals(List.of("2 [0, 3]"), this.chains(analyzed.log()));
        // Results validated using postgres
        analyzed.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 'x', 'y');
                        INSERT INTO many VALUES (10, 'A'), (10, 'B');
                        INSERT INTO l1 VALUES (20, 'C');""", """
                         id | p1 | w | v1
                        ------------------
                         1  | x  | A | C
                         1  | x  | B | C""");
    }

    /** An aggregate is not an operator the analysis can carry columns across, so the joins
     * before it and the joins after it are separate chains. */
    @Test
    public void aggregateEndsAChain() {
        Analyzed analyzed = this.compile("""
                CREATE LOCAL VIEW agg AS
                SELECT fact.id, COUNT(*) AS cnt FROM fact
                LEFT JOIN l1 ON fact.a = l1.k
                LEFT JOIN l2 ON fact.b = l2.k
                GROUP BY fact.id;
                CREATE VIEW v AS SELECT agg.id, agg.cnt, l1.v1, l2.v2 FROM agg
                LEFT JOIN l1 ON agg.cnt = l1.k
                LEFT JOIN l2 ON agg.id = l2.k;""");
        // Two chains of two, not one of four.  The first only carries fact.id: the aggregate
        // after the chain groups by it.  The second reads both of its columns in join conditions
        Assert.assertEquals(List.of("2 [0]", "2 []"), this.chains(analyzed.log()));
        // Results validated using postgres
        analyzed.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 'x', 'y');
                        INSERT INTO l1 VALUES (1, 'A'), (10, 'B');
                        INSERT INTO l2 VALUES (1, 'C'), (20, 'D');""",
                """
                         id | cnt | v1 | v2
                        --------------------
                         1  | 1   | A  | C""");
    }

    /** A filter's predicate reads a column. */
    @Test
    public void filterPredicateReadsAColumn() {
        Analyzed analyzed = this.compile("""
                CREATE VIEW v AS SELECT fact.*, l1.v1, l2.v2 FROM fact
                LEFT JOIN l1 ON fact.a = l1.k
                LEFT JOIN l2 ON fact.b = l2.k
                WHERE fact.p2 <> 'q';""");
        Assert.assertEquals(List.of("2 [0, 3]"), this.chains(analyzed.log()));
        // Results validated using postgres
        analyzed.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 'x', 'y'), (2, 11, 21, 'z', 'q');
                        INSERT INTO l1 VALUES (10, 'A');
                        INSERT INTO l2 VALUES (20, 'B');""",
                """
                         id | a  | b  | p1 | p2 | v1 | v2
                        ----------------------------------
                         1  | 10 | 20 | x  | y  | A  | B""");
    }

    /** A distinct collapses can be part of a chain. */
    @Test
    public void distinctContinuesAChain() {
        Analyzed analyzed = this.compile("""
                CREATE TABLE many(k INT NOT NULL, w VARCHAR);
                CREATE LOCAL VIEW d AS SELECT DISTINCT fact.id, fact.b, fact.p1 FROM fact
                LEFT JOIN many ON fact.a = many.k;
                CREATE VIEW v AS SELECT d.*, l2.v2 FROM d
                LEFT JOIN l2 ON d.b = l2.k;""");
        // fact has columns id, a, b, p1, p2; b is read by the second condition
        Assert.assertEquals(List.of("2 [0, 3]"), this.chains(analyzed.log()));
        Assert.assertTrue(this.hasDistinct(analyzed.ccs()));
        // Results validated using postgres
        analyzed.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 'x', 'y');
                        INSERT INTO many VALUES (10, 'A'), (10, 'B');
                        INSERT INTO l2 VALUES (20, 'D');""",
                """
                         id | b  | p1 | v2
                        -------------------
                         1  | 20 | x  | D""");
    }

    /** A chain that forks into two. */
    @Test
    public void forkedChainStartsANewChain() {
        Analyzed analyzed = this.compile("""
                CREATE TABLE l3(k INT NOT NULL PRIMARY KEY, v3 VARCHAR);
                CREATE LOCAL VIEW shared AS SELECT fact.*, l1.v1, l2.v2 FROM fact
                LEFT JOIN l1 ON fact.a = l1.k
                LEFT JOIN l2 ON fact.b = l2.k;
                CREATE VIEW v AS
                SELECT shared.id, shared.p1, shared.v1, l3.v3 FROM shared
                LEFT JOIN l3 ON shared.a = l3.k
                LEFT JOIN l1 x ON shared.b = x.k
                UNION ALL
                SELECT shared.id, shared.p2, shared.v2, l3.v3 FROM shared
                LEFT JOIN l3 ON shared.b = l3.k
                LEFT JOIN l2 y ON shared.a = y.k;""");
        Assert.assertEquals(List.of("4 [0, 3]", "2 [0, 4, 6]"), this.chains(analyzed.log()));
        // Results validated using postgres
        analyzed.ccs().stepWeightOne("""
                        INSERT INTO fact VALUES (1, 10, 20, 'x', 'y');
                        INSERT INTO l1 VALUES (10, 'A');
                        INSERT INTO l2 VALUES (20, 'B');
                        INSERT INTO l3 VALUES (10, 'C');""",
                """
                         id | p1 | v1 | v3
                        -------------------
                         1  | x  | A  | C
                         1  | y  | B  |NULL""");
    }
}
