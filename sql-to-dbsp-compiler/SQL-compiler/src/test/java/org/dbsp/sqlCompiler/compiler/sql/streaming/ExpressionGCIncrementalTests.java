package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.ColumnWithLateness;
import org.dbsp.sqlCompiler.compiler.sql.tools.CountGCOperators;
import org.dbsp.sqlCompiler.compiler.sql.tools.DateColumn;
import org.dbsp.sqlCompiler.compiler.sql.tools.DifferentialTester;
import org.dbsp.sqlCompiler.compiler.sql.tools.TablePair;
import org.dbsp.sqlCompiler.compiler.sql.tools.TimestampColumn;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** GC of aggregates grouped by expressions of a column ts with LATENESS. */
public class ExpressionGCIncrementalTests extends StreamingTestBase {
    /** A row of an INT input: time ts, value x. */
    record IntRow(@Nullable Integer ts, int x) {}

    /** A row of a TIMESTAMP input: time ts, value x. */
    record TimestampRow(@Nullable LocalDateTime ts, int x) {}

    /** A row of a DATE input: time ts, value x. */
    record DateRow(@Nullable LocalDate ts, int x) {}

    /** The LATENESS of column ts, in the unit of its type: a number, minutes, or days. */
    static final int LATENESS = 10;

    /** A grouping key of an aggregate over table T.
     * @param label       Name of the key in the tables of results.
     * @param expression  The key, an expression of the columns of the FROM clause.
     * @param from        The FROM clause, which reads table T. */
    record Key(String label, String expression, String from) {
        /** The key {@code expression} of the columns of T. */
        static Key of(String expression) {
            return new Key(expression, expression, "T");
        }

        /** The FROM clause over table {@code table}. */
        String from(String table) {
            return this.from.replaceAll("\\bT\\b", table);
        }

        /** An aggregate over {@code table} grouped by the key. */
        String aggregate(String table) {
            return "SELECT " + this.expression + " AS g, SUM(x) AS s FROM " + this.from(table) +
                    " GROUP BY " + this.expression;
        }
    }

    /** The grouping keys of column ts of one kind of type.
     * @param types     The types of ts to compile the aggregates with.
     * @param lateness  The LATENESS clause of ts.
     * @param keys      The grouping keys. */
    record Family(List<String> types, String lateness, List<Key> keys) {}

    static final Family NUMERIC = new Family(
            List.of("INT", "BIGINT", "DOUBLE", "DECIMAL(10, 2)"),
            "LATENESS " + LATENESS,
            List.of(Key.of("ts * 2"), Key.of("2 * ts"), Key.of("2 * ts * 3"), Key.of("ts / 2"),
                    Key.of("ts * CAST(2 AS TINYINT)"), Key.of("-2 * ts"), Key.of("2 / ts"),
                    Key.of("ts + 1"), Key.of("1 + ts"), Key.of("ts - 1"), Key.of("1 - ts"), Key.of("-ts"),
                    Key.of("CAST(ts AS BIGINT)"), Key.of("CAST(ts AS DECIMAL(12, 2))"),
                    Key.of("CAST(ts AS DOUBLE)"), Key.of("CAST(ts AS REAL)"), Key.of("CAST(ts AS TIMESTAMP)"),
                    new Key("ts hours after 2020", "TIMESTAMP '2020-01-01 00:00:00' + " +
                            "CAST(CAST(ts AS BIGINT) AS INTERVAL HOUR)", "T"),
                    Key.of("CAST(ts AS VARCHAR)"),
                    Key.of("ROUND(ts, -1)"), Key.of("TRUNCATE(ts, -1)"), Key.of("FLOOR(ts / 3)"),
                    Key.of("CEIL(ts / 3)"), Key.of("SIGN(ts - 150)"), Key.of("ABS(ts - 150)"),
                    Key.of("SQRT(ts)"), Key.of("LN(ts)"), Key.of("LOG10(ts)"),
                    Key.of("LOG(ts)"), Key.of("LOG(ts, 10)"), Key.of("LOG(ts, 0.5)"),
                    Key.of("POWER(2, ts / 100)"), Key.of("POWER(0.5, ts / 100)"),
                    Key.of("CBRT(ts)"), Key.of("EXP(ts / 100)"), Key.of("ATAN(ts)"), Key.of("SINH(ts / 100)"),
                    Key.of("ASINH(ts)"), Key.of("TANH(ts / 100)"), Key.of("DEGREES(ts)"), Key.of("RADIANS(ts)"),
                    Key.of("BROUND(ts, -1)"), Key.of("TRUNC(ts, -1)"),
                    Key.of("GREATEST(ts, x)"), Key.of("GREATEST(ts, 120)"), Key.of("GREATEST_IGNORE_NULLS(ts, x)"),
                    Key.of("LEAST(ts, ts + 1)"), Key.of("LEAST_IGNORE_NULLS(ts, ts + 1)"),
                    Key.of("LEAST(ts, x)"), Key.of("LEAST_IGNORE_NULLS(ts, x)"),
                    Key.of("COALESCE(GREATEST(ts, x), ts)"), Key.of("COALESCE(GREATEST(ts, x), x)"),
                    // The left operand is NULL when x = 7, and the result is below its bound
                    new Key("COALESCE, left NULL at x = 7", "COALESCE(GREATEST(ts, NULLIF(x, 7)), ts - 9)", "T")));

    static final Family TIMESTAMP = new Family(
            List.of("TIMESTAMP"),
            "LATENESS INTERVAL " + LATENESS + " MINUTES",
            List.of(Key.of("ts + INTERVAL 1 HOUR"), Key.of("INTERVAL 1 HOUR + ts"), Key.of("ts - INTERVAL 1 HOUR"),
                    Key.of("TIMESTAMP_TRUNC(ts, HOUR)"), Key.of("FLOOR(ts TO HOUR)"), Key.of("CEIL(ts TO HOUR)"),
                    Key.of("CAST(ts AS DATE)"), Key.of("CAST(ts AS TIMESTAMP WITH TIME ZONE)"),
                    Key.of("CAST(ts AS BIGINT)"), Key.of("CAST(ts AS BIGINT UNSIGNED)"), Key.of("CAST(ts AS DOUBLE)"),
                    Key.of("CAST(ts AS DECIMAL(20, 3))"),
                    new Key("hours after 2020 as DECIMAL", "CAST((ts - TIMESTAMP '2020-01-01 00:00:00') " +
                            "HOUR AS DECIMAL(20, 2))", "T"),
                    Key.of("YEAR(ts)"), Key.of("EXTRACT(EPOCH FROM ts)"),
                    Key.of("EXTRACT(DECADE FROM ts)"), Key.of("EXTRACT(ISOYEAR FROM ts)"),
                    Key.of("TIMESTAMPDIFF(MINUTE, TIMESTAMP '2020-01-01 00:00:00', ts)"),
                    Key.of("GREATEST(ts, TIMESTAMP '2021-01-01 00:00:00')"),
                    Key.of("GREATEST_IGNORE_NULLS(ts, TIMESTAMP '2021-01-01 00:00:00')"),
                    new Key("TUMBLE window_start", "window_start",
                            "TABLE(TUMBLE(TABLE T, DESCRIPTOR(ts), INTERVAL 1 HOUR))"),
                    new Key("HOP window_start", "window_start",
                            "TABLE(HOP(TABLE T, DESCRIPTOR(ts), INTERVAL 30 MINUTES, INTERVAL 1 HOUR))"),
                    Key.of("TIMESTAMPDIFF(MINUTE, ts, TIMESTAMP '2030-01-01 00:00:00')"),
                    Key.of("HOUR(ts)"), Key.of("CAST(ts AS TIME)"), Key.of("CAST(ts AS VARCHAR)")));

    static final Family DATE = new Family(
            List.of("DATE"),
            "LATENESS INTERVAL " + LATENESS + " DAYS",
            List.of(Key.of("ts + INTERVAL 1 DAY"), Key.of("ts - INTERVAL 1 DAY"),
                    Key.of("DATE_TRUNC(ts, MONTH)"), Key.of("YEAR(ts)"), Key.of("CAST(ts AS TIMESTAMP)"),
                    Key.of("DATEDIFF(DAY, DATE '2020-01-01', ts)"), Key.of("EXTRACT(EPOCH FROM ts)"),
                    Key.of("EXTRACT(DECADE FROM ts)"), Key.of("EXTRACT(ISOYEAR FROM ts)"),
                    Key.of("DATEDIFF(DAY, ts, DATE '2030-01-01')"), Key.of("DAYOFWEEK(ts)"), Key.of("MONTH(ts)"),
                    Key.of("CAST(ts AS VARCHAR)")));

    /** The GC operators of the aggregate grouped by each key, for each type of ts: K is a RetainKeys
     * operator, V a RetainValues operator, and N a RetainNValues operator. */
    static final String EXPECTED_NUMERIC_GC = """
            key                            | INT    | BIGINT | DOUBLE | DECIMAL(10, 2)
            ts * 2                         | K      | K      | K      | K
            2 * ts                         | K      | K      | K      | K
            2 * ts * 3                     | K      | K      | K      | K
            ts / 2                         | K      | K      | K      | K
            ts * CAST(2 AS TINYINT)        | K      | K      | K      | K
            -2 * ts                        | -      | -      | -      | -
            2 / ts                         | -      | -      | -      | -
            ts + 1                         | K      | K      | K      | K
            1 + ts                         | K      | K      | K      | K
            ts - 1                         | K      | K      | K      | K
            1 - ts                         | -      | -      | -      | -
            -ts                            | -      | -      | -      | -
            CAST(ts AS BIGINT)             | K      | K      | K      | K
            CAST(ts AS DECIMAL(12, 2))     | K      | K      | K      | K
            CAST(ts AS DOUBLE)             | K      | K      | K      | K
            CAST(ts AS REAL)               | K      | K      | K      | K
            CAST(ts AS TIMESTAMP)          | K      | K      | K      | K
            ts hours after 2020            | K      | K      | K      | K
            CAST(ts AS VARCHAR)            | -      | -      | -      | -
            ROUND(ts, -1)                  | K      | K      | K      | K
            TRUNCATE(ts, -1)               | K      | K      | K      | K
            FLOOR(ts / 3)                  | K      | K      | K      | K
            CEIL(ts / 3)                   | K      | K      | K      | K
            SIGN(ts - 150)                 | K      | K      | K      | K
            ABS(ts - 150)                  | -      | -      | -      | -
            SQRT(ts)                       | K      | K      | K      | K
            LN(ts)                         | K      | K      | K      | K
            LOG10(ts)                      | K      | K      | K      | K
            LOG(ts)                        | K      | K      | K      | K
            LOG(ts, 10)                    | K      | K      | K      | K
            LOG(ts, 0.5)                   | -      | -      | -      | -
            POWER(2, ts / 100)             | K      | K      | K      | K
            POWER(0.5, ts / 100)           | -      | -      | -      | -
            CBRT(ts)                       | K      | K      | K      | K
            EXP(ts / 100)                  | K      | K      | K      | K
            ATAN(ts)                       | K      | K      | K      | K
            SINH(ts / 100)                 | K      | K      | K      | K
            ASINH(ts)                      | K      | K      | K      | K
            TANH(ts / 100)                 | K      | K      | K      | K
            DEGREES(ts)                    | K      | K      | K      | K
            RADIANS(ts)                    | K      | K      | K      | K
            BROUND(ts, -1)                 | K      | K      | K      | K
            TRUNC(ts, -1)                  | K      | K      | K      | K
            GREATEST(ts, x)                | K      | K      | K      | K
            GREATEST(ts, 120)              | K      | K      | K      | K
            GREATEST_IGNORE_NULLS(ts, x)   | K      | K      | K      | K
            LEAST(ts, ts + 1)              | K      | K      | K      | K
            LEAST_IGNORE_NULLS(ts, ts + 1) | K      | K      | K      | K
            LEAST(ts, x)                   | -      | -      | -      | -
            LEAST_IGNORE_NULLS(ts, x)      | -      | -      | -      | -
            COALESCE(GREATEST(ts, x), ts)  | K      | K      | K      | K
            COALESCE(GREATEST(ts, x), x)   | -      | -      | -      | -
            COALESCE, left NULL at x = 7   | K      | K      | K      | K
            """;

    static final String EXPECTED_TIMESTAMP_GC = """
            key                                                        | TIMESTAMP
            ts + INTERVAL 1 HOUR                                       | K
            INTERVAL 1 HOUR + ts                                       | K
            ts - INTERVAL 1 HOUR                                       | K
            TIMESTAMP_TRUNC(ts, HOUR)                                  | K
            FLOOR(ts TO HOUR)                                          | K
            CEIL(ts TO HOUR)                                           | K
            CAST(ts AS DATE)                                           | K
            CAST(ts AS TIMESTAMP WITH TIME ZONE)                       | K
            CAST(ts AS BIGINT)                                         | K
            CAST(ts AS BIGINT UNSIGNED)                                | K
            CAST(ts AS DOUBLE)                                         | K
            CAST(ts AS DECIMAL(20, 3))                                 | K
            hours after 2020 as DECIMAL                                | K
            YEAR(ts)                                                   | K
            EXTRACT(EPOCH FROM ts)                                     | K
            EXTRACT(DECADE FROM ts)                                    | K
            EXTRACT(ISOYEAR FROM ts)                                   | K
            TIMESTAMPDIFF(MINUTE, TIMESTAMP '2020-01-01 00:00:00', ts) | K
            GREATEST(ts, TIMESTAMP '2021-01-01 00:00:00')              | K
            GREATEST_IGNORE_NULLS(ts, TIMESTAMP '2021-01-01 00:00:00') | K
            TUMBLE window_start                                        | K
            HOP window_start                                           | K
            TIMESTAMPDIFF(MINUTE, ts, TIMESTAMP '2030-01-01 00:00:00') | -
            HOUR(ts)                                                   | -
            CAST(ts AS TIME)                                           | -
            CAST(ts AS VARCHAR)                                        | -
            """;

    static final String EXPECTED_DATE_GC = """
            key                                  | DATE
            ts + INTERVAL 1 DAY                  | K
            ts - INTERVAL 1 DAY                  | K
            DATE_TRUNC(ts, MONTH)                | K
            YEAR(ts)                             | K
            CAST(ts AS TIMESTAMP)                | K
            DATEDIFF(DAY, DATE '2020-01-01', ts) | K
            EXTRACT(EPOCH FROM ts)               | K
            EXTRACT(DECADE FROM ts)              | K
            EXTRACT(ISOYEAR FROM ts)             | K
            DATEDIFF(DAY, ts, DATE '2030-01-01') | -
            DAYOFWEEK(ts)                        | -
            MONTH(ts)                            | -
            CAST(ts AS VARCHAR)                  | -
            """;

    /** The GC operators of the aggregate grouped by each key of {@code family}, one line per key. */
    String gcTable(Family family) {
        int width = "key".length();
        for (Key key : family.keys)
            width = Math.max(width, key.label.length());
        String keyFormat = "%-" + width + "s";
        StringBuilder header = new StringBuilder(String.format(keyFormat, "key"));
        for (String type : family.types)
            header.append(String.format(" | %-6s", type));
        StringBuilder result = new StringBuilder(header.toString().stripTrailing()).append("\n");
        for (Key key : family.keys) {
            StringBuilder line = new StringBuilder(String.format(keyFormat, key.label));
            for (String type : family.types) {
                String sql = "CREATE TABLE T(ts " + type + " NOT NULL " + family.lateness + ", x INT);\n" +
                        "CREATE VIEW V AS " + key.aggregate("T") + ";";
                var cc = this.getCC(sql);
                CountGCOperators operators = new CountGCOperators(cc.compiler);
                cc.visit(operators);
                line.append(String.format(" | %-6s", operators.kinds()));
            }
            // A text block has no trailing spaces
            result.append(line.toString().stripTrailing()).append("\n");
        }
        return result.toString();
    }

    /** Compiles an aggregate grouped by each expression of ts, for each type of ts, and compares its GC
     * operators with the expected tables.  An example program:
     * <pre>
     * CREATE TABLE T(ts BIGINT NOT NULL LATENESS 10, x INT);
     * CREATE VIEW V AS SELECT 2 * ts AS g, SUM(x) AS s FROM T GROUP BY 2 * ts;
     * </pre> */
    @Test
    public void gcOnlyForMonotoneKeys() {
        Assert.assertEquals(EXPECTED_NUMERIC_GC, this.gcTable(NUMERIC));
        Assert.assertEquals(EXPECTED_TIMESTAMP_GC, this.gcTable(TIMESTAMP));
        Assert.assertEquals(EXPECTED_DATE_GC, this.gcTable(DATE));
    }

    /** Check that only a division by a positive constant gets GC, so the bound of a key never
     * divides by zero. */
    @Test
    public void gcOnlyForDivisionByPositiveConstant() {
        Family numbers = new Family(
                List.of("INT", "BIGINT", "DOUBLE", "DECIMAL(10, 2)"),
                "LATENESS " + LATENESS,
                List.of(Key.of("ts / 2"), Key.of("ts / 0"), Key.of("ts / -2"), Key.of("ts / x")));
        Assert.assertEquals("""
                key     | INT    | BIGINT | DOUBLE | DECIMAL(10, 2)
                ts / 2  | K      | K      | K      | K
                ts / 0  | -      | -      | -      | -
                ts / -2 | -      | -      | -      | -
                ts / x  | -      | -      | -      | -
                """, this.gcTable(numbers));
        String hours = "(ts - TIMESTAMP '2020-01-01 00:00:00') HOUR";
        Family intervals = new Family(
                List.of("TIMESTAMP"),
                "LATENESS INTERVAL " + LATENESS + " MINUTES",
                List.of(new Key("hours / 2", hours + " / 2", "T"),
                        new Key("hours / 0", hours + " / 0", "T"),
                        new Key("hours / -2", hours + " / -2", "T")));
        Assert.assertEquals("""
                key        | TIMESTAMP
                hours / 2  | K
                hours / 0  | -
                hours / -2 | -
                """, this.gcTable(intervals));
    }

    /** The first INT time of the inputs; TIMESTAMP_BASE plus t minutes and DATE_BASE plus t days are the
     * other times of the same row.  A day and a year begin 145 minutes and 145 days after the bases. */
    static final LocalDateTime TIMESTAMP_BASE = LocalDateTime.of(2020, 12, 31, 21, 35, 0);
    static final LocalDate DATE_BASE = LocalDate.of(2020, 8, 9);

    /** A change with time t and value x. */
    record Change(@Nullable Integer t, int x) {}

    static Change c(@Nullable Integer t, int x) {
        return new Change(t, x);
    }

    /** The rows of {@code changes} for the INT, TIMESTAMP, and DATE inputs. */
    static Record[] rows(Change... changes) {
        List<Record> result = new ArrayList<>();
        for (Change change : changes) {
            Integer t = change.t;
            result.add(new IntRow(t, change.x));
            result.add(new TimestampRow(t == null ? null : TIMESTAMP_BASE.plusMinutes(t), change.x));
            result.add(new DateRow(t == null ? null : DATE_BASE.plusDays(t), change.x));
        }
        return result.toArray(new Record[0]);
    }

    static final List<String> COLUMNS = List.of("e", "g", "s");

    /** The branches of view ALL over {@code table}: an aggregate for each key of {@code family}, whose
     * results are cast to VARCHAR and labeled with the key. */
    static List<String> branches(Family family, String table) {
        List<String> result = new ArrayList<>();
        for (Key key : family.keys) {
            String label = key.label.replace("'", "''");
            result.add("SELECT '" + label + "' AS e, CAST(g AS VARCHAR) AS g, CAST(s AS VARCHAR) AS s FROM (" +
                    key.aggregate(table) + ") agg");
        }
        return result;
    }

    /** Check that each aggregate gives the same output with and without LATENESS. */
    @Test
    public void sameOutputWithoutLateness() {
        List<TablePair<?>> pairs = List.of(
                new TablePair<>("INT", true, IntRow.class, Set.of("ts"), LATENESS),
                new TablePair<>("TS", true, TimestampRow.class, List.<ColumnWithLateness<TimestampRow, ?>>of(
                        new TimestampColumn<>("ts", TimestampRow::ts, Duration.ofMinutes(LATENESS)))),
                new TablePair<>("DT", true, DateRow.class, List.<ColumnWithLateness<DateRow, ?>>of(
                        new DateColumn<>("ts", DateRow::ts, LATENESS))));
        List<Family> families = List.of(NUMERIC, TIMESTAMP, DATE);
        StringBuilder sql = new StringBuilder();
        for (TablePair<?> pair : pairs) {
            sql.append(pair.create("LATE_", true));
            sql.append(pair.create("PLAIN_", false));
        }
        for (String prefix : TablePair.PREFIXES) {
            List<String> branches = new ArrayList<>();
            for (int i = 0; i < pairs.size(); i++)
                branches.addAll(branches(families.get(i), prefix + pairs.get(i).name));
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", branches)).append(";\n");
        }
        sql.append(DifferentialTester.differenceView(COLUMNS));
        var tester = new DifferentialTester(this.getCCS(sql.toString()), pairs, COLUMNS);
        Set<String> all = Set.of("INT", "TS", "DT");

        tester.insert(rows(c(100, 1), c(101, 2), c(105, 3), c(null, 4), c(110, 5)));
        // The waterlines move to 140
        tester.insert(rows(c(150, 6)));
        // At the waterlines; ts / 2 puts both INT rows in group 70
        tester.insert(all, rows(c(140, 7), c(141, 8)));
        tester.delete(all, rows(c(141, 8)));
        // Late changes, which the tester skips
        tester.insert(rows(c(139, 9)));
        tester.delete(rows(c(100, 1)));
        // A day and a year begin at 145
        tester.insert(all, rows(c(null, 10), c(144, 11), c(145, 12)));
        // The waterlines move to 190
        tester.insert(rows(c(200, 13)));
        tester.insert(all, rows(c(190, 14), c(195, 15)));
        tester.insert(all, rows(c(190, 14)));
        tester.delete(all, rows(c(null, 4), c(195, 15)));
    }

    /** Check that an aggregate grouped by SQRT(ts) keeps its groups when the waterline of ts is
     * negative: SQRT of a negative number is NaN, the largest value, but the bound is the
     * saturating form, -infinity. */
    @Test
    public void sqrtOfNegativeWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT NOT NULL LATENESS 10, x INT NOT NULL);
                CREATE VIEW V AS SELECT CAST(SQRT(ts) AS VARCHAR) AS g, SUM(x) AS s FROM T GROUP BY SQRT(ts);""")
                .compactAfterEachStep().withStringTrim();
        // The waterline moves to -6
        ccs.step("INSERT INTO T VALUES (4, 1), (-20, 2);", """
                 g   | s | weight
                ------------------
                 2.0 | 1 | 1
                 NaN | 2 | 1""");
        ccs.step("INSERT INTO T VALUES (4, 10);", """
                 g   | s  | weight
                -------------------
                 2.0 | 1  | -1
                 2.0 | 11 | 1""");
    }

    /** Check that an aggregate grouped by a TIME minus an INTERVAL keeps its groups: the
     * subtraction wraps around midnight, so it is not monotone. */
    @Test
    public void timeSubtractionIsNotMonotone() {
        var ccs = this.getCCS("""
                CREATE TABLE T(t TIME NOT NULL LATENESS INTERVAL 1 HOUR, x INT NOT NULL);
                CREATE VIEW V AS SELECT t - INTERVAL 2 HOURS AS g, SUM(x) AS s
                FROM T GROUP BY t - INTERVAL 2 HOURS;""")
                .compactAfterEachStep();
        // After the second insert the waterline of t is 01:30; 01:30 - 2 hours is 23:30,
        // above the new group 00:30
        ccs.step("INSERT INTO T VALUES ('01:30:00', 1);", """
                 g        | s | weight
                -----------------------
                 23:30:00 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES ('02:30:00', 1);", """
                 g        | s | weight
                -----------------------
                 00:30:00 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES ('02:30:00', 10);", """
                 g        | s  | weight
                ------------------------
                 00:30:00 | 1  | -1
                 00:30:00 | 11 | 1""");
    }

    /** Check that an aggregate grouped by an expression of ts that is always NULL keeps its only group. */
    @Test
    public void issue7685() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT NOT NULL LATENESS 10, x INT NOT NULL);
                CREATE VIEW V AS SELECT ts + CAST(NULL AS INT) AS g, SUM(x) AS s
                FROM T GROUP BY ts + CAST(NULL AS INT);""")
                .compactAfterEachStep();
        ccs.step("INSERT INTO T VALUES (100, 1);", """
                 g    | s | weight
                -------------------
                 NULL | 1 | 1""");
        // The waterline moves to 190
        ccs.step("INSERT INTO T VALUES (200, 2);", """
                 g    | s | weight
                -------------------
                 NULL | 1 | -1
                 NULL | 3 | 1""");
        ccs.step("INSERT INTO T VALUES (195, 4);", """
                 g    | s | weight
                -------------------
                 NULL | 3 | -1
                 NULL | 7 | 1""");
    }

    /** CEIL of a TIME wraps around midnight, so it is not monotone. */
    @Test
    public void timeCeilingIsNotMonotone() {
        var ccs = this.getCCS("""
                CREATE TABLE T(t TIME NOT NULL LATENESS INTERVAL 1 HOUR, x INT NOT NULL);
                CREATE VIEW V AS SELECT CEIL(t TO HOUR) AS g, SUM(x) AS s
                FROM T GROUP BY CEIL(t TO HOUR);""")
                .compactAfterEachStep();
        ccs.step("INSERT INTO T VALUES ('22:30:00', 1);", """
                 g        | s | weight
                -----------------------
                 23:00:00 | 1 | 1""");
        // After this insert the waterline of t is 22:30, whose ceiling 23:00 is above the
        // new group 00:00
        ccs.step("INSERT INTO T VALUES ('23:30:00', 1);", """
                 g        | s | weight
                -----------------------
                 00:00:00 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES ('23:30:00', 10);", """
                 g        | s  | weight
                ------------------------
                 00:00:00 | 1  | -1
                 00:00:00 | 11 | 1""");
    }

    /** Check that an aggregate grouped by the hour of a TIMESTAMP keeps its groups: the hour
     * wraps every day, so it is not monotone. */
    @Test
    public void timestampHourIsNotMonotone() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR, x INT NOT NULL);
                CREATE VIEW V AS SELECT EXTRACT(HOUR FROM ts) AS h, SUM(x) AS s
                FROM T GROUP BY EXTRACT(HOUR FROM ts);""")
                .compactAfterEachStep();
        ccs.step("INSERT INTO T VALUES ('2024-01-01 00:10:00', 1);", """
                 h | s | weight
                ----------------
                 0 | 1 | 1""");
        // After this insert the waterline of ts is 22:30, whose hour 22 is above the group 0
        ccs.step("INSERT INTO T VALUES ('2024-01-01 23:30:00', 1);", """
                 h  | s | weight
                -----------------
                 23 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES ('2024-01-02 00:20:00', 10);", """
                 h | s  | weight
                -----------------
                 0 | 1  | -1
                 0 | 11 | 1""");
    }

    /** Check that an aggregate grouped by LN(ts) works when the waterline of ts is negative and
     * every row of ts is positive. */
    @Test
    public void lnOfNegativeWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT NOT NULL LATENESS 10, x INT NOT NULL);
                CREATE VIEW V AS SELECT CAST(LN(ts) AS VARCHAR) AS g, SUM(x) AS s FROM T GROUP BY LN(ts);""")
                .compactAfterEachStep().withStringTrim();
        // The waterline moves to -9
        ccs.step("INSERT INTO T VALUES (1, 1);", """
                 g   | s | weight
                ------------------
                 0.0 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES (1, 10);", """
                 g   | s  | weight
                -------------------
                 0.0 | 1  | -1
                 0.0 | 11 | 1""");
    }

    /** Check that aggregates grouped by LOG(ts, 10) and LOG(ts) work when the waterline of ts is
     * negative and every row of ts is positive. */
    @Test
    public void logOfNegativeWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT NOT NULL LATENESS 10, x INT NOT NULL);
                CREATE VIEW V AS SELECT CAST(LOG(ts, 10) AS VARCHAR) AS g, SUM(x) AS s
                FROM T GROUP BY LOG(ts, 10);
                CREATE VIEW W AS SELECT CAST(LOG(ts) AS VARCHAR) AS g, SUM(x) AS s
                FROM T GROUP BY LOG(ts);""")
                .compactAfterEachStep().withStringTrim();
        // The waterline moves to -9
        String inserted = """
                 g   | s | weight
                ------------------
                 0.0 | 1 | 1""";
        ccs.stepMultiView("INSERT INTO T VALUES (1, 1);", Map.of("v", inserted, "w", inserted));
        String updated = """
                 g   | s  | weight
                -------------------
                 0.0 | 1  | -1
                 0.0 | 11 | 1""";
        ccs.stepMultiView("INSERT INTO T VALUES (1, 10);", Map.of("v", updated, "w", updated));
    }

    /** Check that an aggregate grouped by FLOOR(ts TO DECADE) works when the waterline of ts is in
     * a decade that starts before year 1 and every row of ts is in a later decade. */
    @Test
    public void floorDecadeOfEarlyWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts TIMESTAMP NOT NULL LATENESS INTERVAL 1000 DAYS, x INT NOT NULL);
                CREATE VIEW V AS SELECT CAST(FLOOR(ts TO DECADE) AS VARCHAR) AS g, SUM(x) AS s
                FROM T GROUP BY FLOOR(ts TO DECADE);""")
                .compactAfterEachStep().withStringTrim();
        // The waterline moves to year 9, whose decade starts in year 0
        ccs.step("INSERT INTO T VALUES ('0012-01-01 00:00:00', 1);", """
                 g                   | s | weight
                ----------------------------------
                 0010-01-01 00:00:00 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES ('0012-01-01 00:00:00', 10);", """
                 g                   | s  | weight
                -----------------------------------
                 0010-01-01 00:00:00 | 1  | -1
                 0010-01-01 00:00:00 | 11 | 1""");
    }

    /** Check that an aggregate grouped by the start of a weekly TUMBLE window works when the
     * waterline of ts is in a window that starts before year 1 and every row of ts is in a later
     * window.  Weekly windows start on Thursdays, like 1970-01-01; 0001-01-04 is a Thursday. */
    @Test
    public void tumbleOfEarlyWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts TIMESTAMP NOT NULL LATENESS INTERVAL 2 HOURS, x INT NOT NULL);
                CREATE VIEW V AS SELECT CAST(window_start AS VARCHAR) AS w, SUM(x) AS s
                FROM TABLE(TUMBLE(TABLE T, DESCRIPTOR(ts), INTERVAL 7 DAYS)) GROUP BY window_start;""")
                .compactAfterEachStep().withStringTrim();
        // The waterline moves to 0001-01-03 23:00:00, in the window that starts on 0000-12-28
        ccs.step("INSERT INTO T VALUES ('0001-01-04 01:00:00', 1);", """
                 w                   | s | weight
                ----------------------------------
                 0001-01-04 00:00:00 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES ('0001-01-04 01:00:00', 10);", """
                 w                   | s  | weight
                -----------------------------------
                 0001-01-04 00:00:00 | 1  | -1
                 0001-01-04 00:00:00 | 11 | 1""");
    }

    /** Check that an aggregate grouped by ROUND(ts, -1) works when rounding the waterline of ts
     * needs more digits than its type has and rounding every row of ts does not. */
    @Test
    public void roundOfEarlyWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts DECIMAL(4, 2) NOT NULL LATENESS 1, x INT NOT NULL);
                CREATE VIEW V AS SELECT CAST(ROUND(ts, -1) AS VARCHAR) AS g, SUM(x) AS s
                FROM T GROUP BY ROUND(ts, -1);""")
                .compactAfterEachStep().withStringTrim();
        // The waterline moves to -95.00, which rounds to -100.00, outside DECIMAL(4, 2)
        ccs.step("INSERT INTO T VALUES (-94.00, 1);", """
                 g   | s | weight
                ------------------
                 -90 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES (-94.00, 10);", """
                 g   | s  | weight
                -------------------
                 -90 | 1  | -1
                 -90 | 11 | 1""");
    }

    /** Check that an aggregate grouped by ts - 20 works when the waterline of ts is below 20 and
     * ts has type INT UNSIGNED. */
    @Test
    public void unsignedSubtractionOfEarlyWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT UNSIGNED NOT NULL LATENESS 10, x INT NOT NULL);
                CREATE VIEW V AS SELECT ts - CAST(20 AS INT UNSIGNED) AS g, SUM(x) AS s
                FROM T GROUP BY ts - CAST(20 AS INT UNSIGNED);""")
                .compactAfterEachStep();
        // The waterline moves to 15, and 15 - 20 is below the minimum INT UNSIGNED
        ccs.step("INSERT INTO T VALUES (25, 1);", """
                 g | s | weight
                ----------------
                 5 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES (25, 10);", """
                 g | s  | weight
                -----------------
                 5 | 1  | -1
                 5 | 11 | 1""");
    }

    /** Check that an aggregate grouped by d - 60 DAYS works when the waterline of d is less than
     * 60 days after the first legal date. */
    @Test
    public void dateSubtractionOfEarlyWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(d DATE NOT NULL LATENESS INTERVAL 10 DAYS, x INT NOT NULL);
                CREATE VIEW V AS SELECT d - INTERVAL 60 DAYS AS g, SUM(x) AS s
                FROM T GROUP BY d - INTERVAL 60 DAYS;""")
                .compactAfterEachStep();
        // The waterline moves to 0001-02-23, and 60 days before it is before 0001-01-01
        ccs.step("INSERT INTO T VALUES ('0001-03-05', 1);", """
                 g          | s | weight
                -------------------------
                 0001-01-04 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES ('0001-03-05', 10);", """
                 g          | s  | weight
                --------------------------
                 0001-01-04 | 1  | -1
                 0001-01-04 | 11 | 1""");
    }

    /** Check that an aggregate grouped by a cast of ts to INT UNSIGNED works when the waterline
     * of ts is negative and every row of ts is positive. */
    @Test
    public void unsignedCastOfNegativeWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT NOT NULL LATENESS 10, x INT NOT NULL);
                CREATE VIEW V AS SELECT CAST(ts AS INT UNSIGNED) AS g, SUM(x) AS s
                FROM T GROUP BY CAST(ts AS INT UNSIGNED);""")
                .compactAfterEachStep();
        // The waterline moves to -5, which is not an INT UNSIGNED
        ccs.step("INSERT INTO T VALUES (5, 1);", """
                 g | s | weight
                ----------------
                 5 | 1 | 1""");
        ccs.step("INSERT INTO T VALUES (5, 10);", """
                 g | s  | weight
                -----------------
                 5 | 1  | -1
                 5 | 11 | 1""");
    }

    /** Check that aggregates grouped by casts of a number to TIMESTAMP and to an INTERVAL, and of
     * a TIMESTAMP to INT, work when the waterline is outside the range of the cast and every row
     * is inside it. */
    @Test
    public void castsOfEarlyWaterline() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts BIGINT NOT NULL LATENESS 9000000000000000000, x INT NOT NULL);
                CREATE TABLE U(t TIMESTAMP NOT NULL LATENESS INTERVAL 30 DAYS, x INT NOT NULL);
                CREATE VIEW ts AS SELECT CAST(ts AS TIMESTAMP) AS g, SUM(x) AS s
                FROM T GROUP BY CAST(ts AS TIMESTAMP);
                CREATE VIEW i AS SELECT SUM(x) AS s FROM T GROUP BY CAST(ts AS INTERVAL HOUR);
                CREATE VIEW n AS SELECT CAST(t AS INT) AS g, SUM(x) AS s
                FROM U GROUP BY CAST(t AS INT);""")
                .compactAfterEachStep();
        // The waterline of ts moves to -9e18 milliseconds, before the first legal TIMESTAMP and
        // below the smallest INTERVAL in hours; the waterline of t moves to 30 days before 1970,
        // which is below the smallest INT in milliseconds.
        ccs.stepMultiView("""
                INSERT INTO T VALUES (0, 1);
                INSERT INTO U VALUES ('1970-01-01 00:00:00', 1);""", Map.of(
                "ts", """
                 g                   | s | weight
                ----------------------------------
                 1970-01-01 00:00:00 | 1 | 1""",
                "i", """
                 s | weight
                ------------
                 1 | 1""",
                "n", """
                 g | s | weight
                ----------------
                 0 | 1 | 1"""));
        ccs.stepMultiView("""
                INSERT INTO T VALUES (0, 10);
                INSERT INTO U VALUES ('1970-01-01 00:00:00', 10);""", Map.of(
                "ts", """
                 g                   | s  | weight
                -----------------------------------
                 1970-01-01 00:00:00 | 1  | -1
                 1970-01-01 00:00:00 | 11 | 1""",
                "i", """
                 s  | weight
                -------------
                 1  | -1
                 11 | 1""",
                "n", """
                 g | s  | weight
                -----------------
                 0 | 1  | -1
                 0 | 11 | 1"""));
    }
}
