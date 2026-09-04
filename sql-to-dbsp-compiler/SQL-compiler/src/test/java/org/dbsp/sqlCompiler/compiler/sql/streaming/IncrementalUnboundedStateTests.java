package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindUnboundedState;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Tests for the {@link FindUnboundedState#WARNING} warnings.
 * Only incremental circuits contain the garbage collection operators that bound state,
 * so these tests compile incrementally, like the pipeline manager does. */
public class IncrementalUnboundedStateTests extends StreamingTestBase {
    /** A join over two tables that neither garbage collection nor a size bound keeps small.
     * Tests prepend the declaration of the 'orders' table to inject stream-processing indicators. */
    static final String UNBOUNDED_JOIN = """
            CREATE TABLE customers(id INT, name VARCHAR);
            CREATE VIEW V AS
            SELECT o.id, o.ts, c.name FROM orders o JOIN customers c ON o.customer = c.id;""";

    /** The warning for {@link #UNBOUNDED_JOIN} when a one-line declaration of 'orders' precedes it */
    static final String JOIN_WARNING = """
            (no input file): Unbounded state
            (no input file):4:61: warning: Unbounded state: The state of a JOIN in the code implementing view 'v' may grow without bound
                3|CREATE VIEW V AS
                4|SELECT o.id, o.ts, c.name FROM orders o JOIN customers c ON o.customer = c.id;
                                                                              ^^^^^^^^^^^^^^^^^
            Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
            See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
            """;

    /** Two tables without lateness; 'append_only' marks the program as stream processing */
    static final String TABLES = """
            CREATE TABLE T(x INT, y INT) WITH ('append_only' = 'true');
            CREATE TABLE S(x INT, y INT);
            """;

    /** Compile {@code sql} and check that the {@link FindUnboundedState#WARNING} warnings,
     * rendered as the compiler prints them, are exactly {@code expected}. */
    void assertUnboundedStateWarnings(String sql, String expected) {
        var cc = this.getCC(sql);
        StringBuilder rendered = new StringBuilder();
        for (CompilerMessages.Message message : cc.compiler.messages.messages)
            if (message.warning && message.errorType.equals(FindUnboundedState.WARNING))
                rendered.append(message);
        Assert.assertEquals(expected, rendered.toString());
    }

    void assertNoUnboundedStateWarnings(String sql) {
        this.assertUnboundedStateWarnings(sql, "");
    }

    /** The anchor that the documentation site generates for a Markdown heading */
    static String anchor(String heading) {
        return heading.toLowerCase(Locale.ENGLISH)
                .replaceAll("[^a-z0-9 ]", "")
                .trim()
                .replaceAll(" +", "-");
    }

    @Test
    public void documentationLinkIsValid() throws IOException {
        // Check that the URL in the documentation exists
        // The hint links to https://docs.feldera.com/<page>#<anchor>, which the site
        // builds from docs.feldera.com/docs/<page>.md and one of its headings
        Matcher url = Pattern.compile("https://docs\\.feldera\\.com/([^#\\s]+)#(\\S+)").matcher(FindUnboundedState.HINT);
        Assert.assertTrue(FindUnboundedState.HINT, url.find());
        Path page = Path.of(BaseSQLTests.PROJECT_DIRECTORY, "..", "docs.feldera.com", "docs", url.group(1) + ".md");
        Assert.assertTrue("Documentation page not found: " + page.normalize(), Files.exists(page));
        List<String> headings = Files.readAllLines(page).stream()
                .filter(line -> line.startsWith("#"))
                .map(line -> anchor(line.replaceFirst("^#+", "")))
                .toList();
        Assert.assertTrue("No heading for anchor '" + url.group(2) + "' in " + page.normalize() + ": " + headings,
                headings.contains(url.group(2)));
    }

    // ---- Programs that must not produce warnings ----

    @Test
    public void batchProgram() {
        // Without LATENESS, append_only, or NOW() the program is not a
        // stream-processing program, so the unbounded join is not reported
        this.assertNoUnboundedStateWarnings("""
                CREATE TABLE orders(id INT, customer INT, ts TIMESTAMP NOT NULL);
                """ + UNBOUNDED_JOIN);
    }

    @Test
    public void silenced() {
        this.assertNoUnboundedStateWarnings("""
                SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON;
                CREATE TABLE orders(id INT, customer INT, ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                """ + UNBOUNDED_JOIN);
    }

    @Test
    public void boundedProgram() {
        // Lateness lets the compiler garbage-collect both aggregates and the join
        // that combines them, so this stream-processing program has no unbounded state
        this.assertNoUnboundedStateWarnings("""
                CREATE TABLE T(ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR, x INT);
                CREATE VIEW V AS
                SELECT CAST(ts AS DATE) AS d, COUNT(*) AS c, MAX(x) AS m
                FROM T GROUP BY CAST(ts AS DATE);""");
    }

    @Test
    public void countWithoutGroupBy() {
        // A linear aggregate keeps one accumulator per group, and without
        // GROUP BY there is a single group, however large the input
        this.assertNoUnboundedStateWarnings("""
                CREATE TABLE input(id BIGINT NOT NULL, ts TIMESTAMP LATENESS INTERVAL 10 DAYS);
                CREATE VIEW output AS SELECT COUNT(*) AS cnt FROM input;""");
    }

    @Test
    public void maxWithoutGroupByAppendOnly() {
        // Without deletions MAX only has to remember the current maximum
        this.assertNoUnboundedStateWarnings("""
                CREATE TABLE input(id BIGINT NOT NULL, ts TIMESTAMP NOT NULL) WITH ('append_only' = 'true');
                CREATE VIEW output AS SELECT MAX(id) AS m FROM input;""");
    }

    @Test
    public void nowOutsideTemporalFilter() {
        // NOW() marks a program as stream processing only inside a temporal filter;
        // computing the current time in a projection does not, and the compiler
        // reports that use separately as an inefficient pattern
        this.assertNoUnboundedStateWarnings("""
                CREATE TABLE orders(id INT, customer INT, ts TIMESTAMP NOT NULL);
                CREATE TABLE customers(id INT, name VARCHAR);
                CREATE VIEW V AS
                SELECT o.id, o.ts, c.name, NOW() AS at FROM orders o JOIN customers c ON o.customer = c.id;""");
    }

    @Test
    public void groupByBooleans() {
        // Boolean keys have a bounded number of values, so the linear aggregate
        // keeps a bounded number of accumulators
        this.assertNoUnboundedStateWarnings("""
                CREATE TABLE input(paid BOOLEAN NOT NULL, shipped BOOLEAN, ts TIMESTAMP LATENESS INTERVAL 10 DAYS);
                CREATE VIEW output AS SELECT paid, shipped, COUNT(*) AS cnt FROM input GROUP BY paid, shipped;""");
    }

    @Test
    public void groupByTenBooleans() {
        // Ten booleans admit exactly MAX_KEY_VALUES = 1024 groups, which is still bounded
        this.assertNoUnboundedStateWarnings("""
                CREATE TABLE input(b0 BOOLEAN NOT NULL, b1 BOOLEAN NOT NULL, b2 BOOLEAN NOT NULL, b3 BOOLEAN NOT NULL,
                                   b4 BOOLEAN NOT NULL, b5 BOOLEAN NOT NULL, b6 BOOLEAN NOT NULL, b7 BOOLEAN NOT NULL,
                                   b8 BOOLEAN NOT NULL, b9 BOOLEAN NOT NULL, ts TIMESTAMP LATENESS INTERVAL 10 DAYS);
                CREATE VIEW output AS
                SELECT b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, COUNT(*) AS cnt
                FROM input GROUP BY b0, b1, b2, b3, b4, b5, b6, b7, b8, b9;""");
    }

    // ---- Programs that must produce warnings ----

    @Test
    public void tableLateness() {
        // The only warning carries the silencing hint
        this.assertUnboundedStateWarnings("""
                CREATE TABLE orders(id INT, customer INT, ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                """ + UNBOUNDED_JOIN, JOIN_WARNING);
    }

    @Test
    public void viewLateness() {
        this.assertUnboundedStateWarnings("""
                CREATE TABLE orders(id INT, customer INT, ts TIMESTAMP NOT NULL);
                """ + UNBOUNDED_JOIN + """

                LATENESS V.ts INTERVAL 1 HOUR;""",
                """
                (no input file): Unbounded state
                (no input file):4:61: warning: Unbounded state: The state of a JOIN in the code implementing view 'v' may grow without bound
                    4|SELECT o.id, o.ts, c.name FROM orders o JOIN customers c ON o.customer = c.id;
                                                                                  ^^^^^^^^^^^^^^^^^
                    5|LATENESS V.ts INTERVAL 1 HOUR;
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void appendOnly() {
        this.assertUnboundedStateWarnings("""
                CREATE TABLE orders(id INT, customer INT, ts TIMESTAMP NOT NULL) WITH ('append_only' = 'true');
                """ + UNBOUNDED_JOIN, JOIN_WARNING);
    }

    @Test
    public void asError() {
        this.statementsFailingInCompilation("""
                SET FELDERA_WARNINGS_ARE_ERRORS = ON;
                CREATE TABLE orders(id INT, customer INT, ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                """ + UNBOUNDED_JOIN,
                """
                (no input file): Unbounded state
                (no input file):5:61: error: Unbounded state: The state of a JOIN in the code implementing view 'v' may grow without bound
                    4|CREATE VIEW V AS
                    5|SELECT o.id, o.ts, c.name FROM orders o JOIN customers c ON o.customer = c.id;
                                                                                  ^^^^^^^^^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void localView() {
        // The optimizer removes the local view, but the warning still names it, because the
        // compiler remembers which view each Calcite relational operator was compiled for
        this.assertUnboundedStateWarnings("""
                CREATE TABLE orders(id INT, customer INT, ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                CREATE TABLE customers(id INT, name VARCHAR);
                CREATE LOCAL VIEW joined AS
                SELECT o.id, o.ts, c.name FROM orders o JOIN customers c ON o.customer = c.id;
                CREATE VIEW V AS SELECT id, name FROM joined WHERE id > 0;""",
                """
                (no input file): Unbounded state
                (no input file):4:61: warning: Unbounded state: The state of a JOIN in the code implementing view 'joined' may grow without bound
                    4|SELECT o.id, o.ts, c.name FROM orders o JOIN customers c ON o.customer = c.id;
                                                                                  ^^^^^^^^^^^^^^^^^
                    5|CREATE VIEW V AS SELECT id, name FROM joined WHERE id > 0;
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void temporalFilter() {
        // The temporal filter bounds the orders side of the join; the customers side stays unbounded
        this.assertUnboundedStateWarnings("""
                CREATE TABLE orders(id INT, customer INT, ts TIMESTAMP NOT NULL);
                CREATE TABLE customers(id INT, name VARCHAR);
                CREATE VIEW V AS
                SELECT o.id, o.ts, c.name FROM orders o JOIN customers c ON o.customer = c.id
                WHERE o.ts >= NOW() - INTERVAL 1 HOUR;""",
                """
                (no input file): Unbounded state
                (no input file):4:61: warning: Unbounded state: The state of a JOIN in the code implementing view 'v' may grow without bound
                    4|SELECT o.id, o.ts, c.name FROM orders o JOIN customers c ON o.customer = c.id
                                                                                  ^^^^^^^^^^^^^^^^^
                    5|WHERE o.ts >= NOW() - INTERVAL 1 HOUR;
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void recursiveView() {
        // The recursion reads table E directly, so its history of deltas is unbounded.
        // The compiler synthesizes the operators inside the recursion without source
        // positions, so the warning points at the statement of the closest output view.
        this.assertUnboundedStateWarnings("""
                CREATE TABLE E(x INT, ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                DECLARE RECURSIVE VIEW R(x INT);
                CREATE VIEW R AS SELECT x FROM E UNION SELECT x + 1 FROM R WHERE x < 10;""",
                """
                (no input file): Unbounded state
                (no input file):3:1: warning: Unbounded state: The state of a UNION in the code implementing view 'r' may grow without bound
                    2|DECLARE RECURSIVE VIEW R(x INT);
                    3|CREATE VIEW R AS SELECT x FROM E UNION SELECT x + 1 FROM R WHERE x < 10;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void recursiveViewBoundedInput() {
        // A temporal filter bounds the stream entering the recursion, but not the fixpoint:
        // the operators on the recursion itself are reported, and only silencing removes them
        this.assertUnboundedStateWarnings("""
                CREATE TABLE E(x INT, ts TIMESTAMP NOT NULL);
                CREATE LOCAL VIEW recent AS SELECT x FROM E WHERE ts >= NOW() - INTERVAL 1 HOUR;
                DECLARE RECURSIVE VIEW R(x INT);
                CREATE VIEW R AS SELECT x FROM recent UNION SELECT x + 1 FROM R WHERE x < 10;""",
                """
                (no input file): Unbounded state
                (no input file):4:1: warning: Unbounded state: The state of a UNION in the code implementing view 'r' may grow without bound
                    3|DECLARE RECURSIVE VIEW R(x INT);
                    4|CREATE VIEW R AS SELECT x FROM recent UNION SELECT x + 1 FROM R WHERE x < 10;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void mutualRecursionBoundedInput() {
        this.assertUnboundedStateWarnings("""
                CREATE TABLE E(x INT, ts TIMESTAMP NOT NULL);
                CREATE LOCAL VIEW recent AS SELECT x FROM E WHERE ts >= NOW() - INTERVAL 1 HOUR;
                DECLARE RECURSIVE VIEW A(x INT);
                DECLARE RECURSIVE VIEW B(x INT);
                CREATE VIEW A AS SELECT x FROM recent UNION SELECT x FROM B;
                CREATE VIEW B AS SELECT x + 1 AS x FROM A WHERE x < 10;""",
                """
                (no input file): Unbounded state
                (no input file):5:1: warning: Unbounded state: The state of a UNION in the code implementing view 'a' may grow without bound
                    5|CREATE VIEW A AS SELECT x FROM recent UNION SELECT x FROM B;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                    6|CREATE VIEW B AS SELECT x + 1 AS x FROM A WHERE x < 10;
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void recursiveViewBoundedOperatorsBesideCycle() {
        // The MIN aggregate and the join depend only on the bounded stream that enters the
        // recursion, so they are not reported; the UNION on the recursion itself is
        this.assertUnboundedStateWarnings("""
                CREATE TABLE E(x INT, ts TIMESTAMP NOT NULL);
                CREATE LOCAL VIEW recent AS SELECT x FROM E WHERE ts >= NOW() - INTERVAL 1 HOUR;
                DECLARE RECURSIVE VIEW R(x INT);
                CREATE VIEW R AS
                SELECT x FROM recent WHERE x > (SELECT MIN(x) FROM recent)
                UNION SELECT x + 1 FROM R WHERE x < 10;""",
                """
                (no input file): Unbounded state
                (no input file):4:1: warning: Unbounded state: The state of a UNION in the code implementing view 'r' may grow without bound
                    4|CREATE VIEW R AS
                    5|SELECT x FROM recent WHERE x > (SELECT MIN(x) FROM recent)
                    6|UNION SELECT x + 1 FROM R WHERE x < 10;
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void recursiveViewJoinsUnboundedTable() {
        // The join with table G brings an unbounded stream into the recursion and is reported
        // together with the UNION on the recursion itself
        this.assertUnboundedStateWarnings("""
                CREATE TABLE E(x INT, ts TIMESTAMP NOT NULL);
                CREATE TABLE G(x INT, y INT);
                CREATE LOCAL VIEW recent AS SELECT x FROM E WHERE ts >= NOW() - INTERVAL 1 HOUR;
                DECLARE RECURSIVE VIEW R(x INT);
                CREATE VIEW R AS SELECT x FROM recent UNION SELECT G.y FROM R JOIN G ON R.x = G.x;""",
                """
                (no input file): Unbounded state
                (no input file):5:73: warning: Unbounded state: The state of a JOIN in the code implementing view 'r' may grow without bound
                    4|DECLARE RECURSIVE VIEW R(x INT);
                    5|CREATE VIEW R AS SELECT x FROM recent UNION SELECT G.y FROM R JOIN G ON R.x = G.x;
                                                                                              ^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                (no input file): Unbounded state
                (no input file):5:1: warning: Unbounded state: The state of a UNION in the code implementing view 'r' may grow without bound
                    4|DECLARE RECURSIVE VIEW R(x INT);
                    5|CREATE VIEW R AS SELECT x FROM recent UNION SELECT G.y FROM R JOIN G ON R.x = G.x;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                """);
    }

    @Test
    public void primaryKey() {
        // The index of a table with a PRIMARY KEY grows with the table,
        // unless the declaration bounds the table size
        String program = """
                CREATE TABLE events(id INT, ts TIMESTAMP NOT NULL) WITH ('append_only' = 'true');
                CREATE TABLE customers(id INT NOT NULL PRIMARY KEY, name VARCHAR)%s;
                CREATE VIEW V AS SELECT e.ts, c.name FROM events e JOIN customers c ON e.id = c.id;""";
        // Only the first warning explains how to silence them
        this.assertUnboundedStateWarnings(String.format(program, ""),
                """
                (no input file): Unbounded state
                (no input file):2:14: warning: Unbounded state: The index of table 'customers' may grow without bound
                    2|CREATE TABLE customers(id INT NOT NULL PRIMARY KEY, name VARCHAR);
                                   ^^^^^^^^^
                    3|CREATE VIEW V AS SELECT e.ts, c.name FROM events e JOIN customers c ON e.id = c.id;
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                (no input file): Unbounded state
                (no input file):3:72: warning: Unbounded state: The state of a JOIN in the code implementing view 'v' may grow without bound
                    2|CREATE TABLE customers(id INT NOT NULL PRIMARY KEY, name VARCHAR);
                    3|CREATE VIEW V AS SELECT e.ts, c.name FROM events e JOIN customers c ON e.id = c.id;
                                                                                             ^^^^^^^^^^^
                """);
        this.assertUnboundedStateWarnings(String.format(program, " WITH ('expected_size' = '1000')"),
                """
                (no input file): Unbounded state
                (no input file):3:72: warning: Unbounded state: The state of a JOIN in the code implementing view 'v' may grow without bound
                    2|CREATE TABLE customers(id INT NOT NULL PRIMARY KEY, name VARCHAR) WITH ('expected_size' = '1000');
                    3|CREATE VIEW V AS SELECT e.ts, c.name FROM events e JOIN customers c ON e.id = c.id;
                                                                                             ^^^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void maxWithoutGroupBy() {
        // Unlike COUNT, MAX is not linear: to handle deletions it keeps the whole input
        this.assertUnboundedStateWarnings("""
                CREATE TABLE input(id BIGINT NOT NULL, ts TIMESTAMP LATENESS INTERVAL 10 DAYS);
                CREATE VIEW output AS SELECT MAX(id) AS m FROM input;""",
                """
                (no input file): Unbounded state
                (no input file):2:30: warning: Unbounded state: The state of an aggregate in the code implementing view 'output' may grow without bound
                    1|CREATE TABLE input(id BIGINT NOT NULL, ts TIMESTAMP LATENESS INTERVAL 10 DAYS);
                    2|CREATE VIEW output AS SELECT MAX(id) AS m FROM input;
                                                   ^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void groupByElevenBooleans() {
        // Eleven booleans admit 2048 groups, one doubling past MAX_KEY_VALUES
        this.assertUnboundedStateWarnings("""
                CREATE TABLE input(b0 BOOLEAN NOT NULL, b1 BOOLEAN NOT NULL, b2 BOOLEAN NOT NULL, b3 BOOLEAN NOT NULL,
                                   b4 BOOLEAN NOT NULL, b5 BOOLEAN NOT NULL, b6 BOOLEAN NOT NULL, b7 BOOLEAN NOT NULL,
                                   b8 BOOLEAN NOT NULL, b9 BOOLEAN NOT NULL, b10 BOOLEAN NOT NULL,
                                   ts TIMESTAMP LATENESS INTERVAL 10 DAYS);
                CREATE VIEW output AS
                SELECT b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, COUNT(*) AS cnt
                FROM input GROUP BY b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10;""",
                """
                (no input file): Unbounded state
                (no input file):5:1: warning: Unbounded state: The state of an aggregate in the code implementing view 'output' may grow without bound
                    5|CREATE VIEW output AS
                    6|SELECT b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, COUNT(*) AS cnt
                    7|FROM input GROUP BY b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10;
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void groupByManyBooleans() {
        // Seven nullable booleans allow 3^7 = 2187 groups, more than the bound the analysis accepts
        this.assertUnboundedStateWarnings("""
                CREATE TABLE input(b0 BOOLEAN, b1 BOOLEAN, b2 BOOLEAN, b3 BOOLEAN, b4 BOOLEAN, b5 BOOLEAN, b6 BOOLEAN,
                                   ts TIMESTAMP LATENESS INTERVAL 10 DAYS);
                CREATE VIEW output AS
                SELECT b0, b1, b2, b3, b4, b5, b6, COUNT(*) AS cnt FROM input GROUP BY b0, b1, b2, b3, b4, b5, b6;""",
                """
                (no input file): Unbounded state
                (no input file):3:1: warning: Unbounded state: The state of an aggregate in the code implementing view 'output' may grow without bound
                    3|CREATE VIEW output AS
                    4|SELECT b0, b1, b2, b3, b4, b5, b6, COUNT(*) AS cnt FROM input GROUP BY b0, b1, b2, b3, b4, b5, b6;
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void orderByLimit() {
        this.assertUnboundedStateWarnings(TABLES + "CREATE VIEW V AS SELECT * FROM T ORDER BY x LIMIT 5;",
                """
                (no input file): Unbounded state
                (no input file):3:1: warning: Unbounded state: The state of an ORDER BY with LIMIT in the code implementing view 'v' may grow without bound
                    2|CREATE TABLE S(x INT, y INT);
                    3|CREATE VIEW V AS SELECT * FROM T ORDER BY x LIMIT 5;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void distinct() {
        this.assertUnboundedStateWarnings(TABLES + "CREATE VIEW V AS SELECT DISTINCT x FROM T;",
                """
                (no input file): Unbounded state
                (no input file):3:1: warning: Unbounded state: The state of a DISTINCT in the code implementing view 'v' may grow without bound
                    2|CREATE TABLE S(x INT, y INT);
                    3|CREATE VIEW V AS SELECT DISTINCT x FROM T;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void windowFunction() {
        this.assertUnboundedStateWarnings(TABLES + """
                CREATE VIEW V AS SELECT * FROM (SELECT x, y, ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) AS rn FROM T) WHERE rn <= 2;""",
                """
                (no input file): Unbounded state
                (no input file):3:46: warning: Unbounded state: The state of a window function in the code implementing view 'v' may grow without bound
                    2|CREATE TABLE S(x INT, y INT);
                    3|CREATE VIEW V AS SELECT * FROM (SELECT x, y, ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) AS rn FROM T) WHERE rn <= 2;
                                                                   ^^^^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }

    @Test
    public void except() {
        // EXCEPT applies DISTINCT to both inputs; the two identical warnings are reported once
        this.assertUnboundedStateWarnings("""
                CREATE TABLE T(x INT) WITH ('append_only' = 'true');
                CREATE TABLE S(x INT);
                CREATE VIEW V AS SELECT x FROM T EXCEPT SELECT x FROM S;""",
                """
                (no input file): Unbounded state
                (no input file):3:1: warning: Unbounded state: The state of a DISTINCT in the code implementing view 'v' may grow without bound
                    2|CREATE TABLE S(x INT);
                    3|CREATE VIEW V AS SELECT x FROM T EXCEPT SELECT x FROM S;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                (no input file): Unbounded state
                (no input file):3:1: warning: Unbounded state: The state of an EXCEPT in the code implementing view 'v' may grow without bound
                    2|CREATE TABLE S(x INT);
                    3|CREATE VIEW V AS SELECT x FROM T EXCEPT SELECT x FROM S;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                """);
    }

    @Test
    public void exceptAll() {
        this.assertUnboundedStateWarnings(TABLES + "CREATE VIEW V AS SELECT x FROM T EXCEPT ALL SELECT x FROM S;",
                """
                (no input file): Unbounded state
                (no input file):3:1: warning: Unbounded state: The state of an EXCEPT ALL in the code implementing view 'v' may grow without bound
                    2|CREATE TABLE S(x INT, y INT);
                    3|CREATE VIEW V AS SELECT x FROM T EXCEPT ALL SELECT x FROM S;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                Silence these warnings with SET FELDERA_IGNORE_WARNING_UNBOUNDED_STATE = ON
                See https://docs.feldera.com/sql/streaming#unbounded-state-warnings
                """);
    }
}
