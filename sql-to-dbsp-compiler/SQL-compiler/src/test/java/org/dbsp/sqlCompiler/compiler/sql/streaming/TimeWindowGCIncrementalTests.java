package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.ColumnWithLateness;
import org.dbsp.sqlCompiler.compiler.sql.tools.CountGCOperators;
import org.dbsp.sqlCompiler.compiler.sql.tools.DifferentialTester;
import org.dbsp.sqlCompiler.compiler.sql.tools.TablePair;
import org.dbsp.sqlCompiler.compiler.sql.tools.TimestampColumn;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** GC of time windows: temporal filters on NOW(), and TUMBLE, HOP and SESSION windows over a
 * column with LATENESS. */
public class TimeWindowGCIncrementalTests extends StreamingTestBase {
    /** A row of the inputs: time a is 5 minutes before time ts, and time b 5 minutes after it. */
    record Row(@Nullable LocalDateTime a, int k, @Nullable LocalDateTime ts, int x, @Nullable LocalDateTime b) {}

    /** A pair of tables of rows; {@code lateness} gives column ts a LATENESS of 1 hour. */
    static TablePair<Row> pair(String name, boolean nullable, boolean lateness) {
        List<ColumnWithLateness<Row, ?>> lateColumns = lateness ?
                List.of(new TimestampColumn<>("ts", Row::ts, Duration.ofHours(1))) : List.of();
        return new TablePair<>(name, nullable, Row.class, lateColumns);
    }

    /** The pairs of tables without LATENESS, for the temporal filters. */
    static final TablePair<Row> N = pair("N", true, false);
    static final TablePair<Row> N_NOT_NULL = pair("N_NOT_NULL", false, false);
    /** The pairs of tables with LATENESS on ts, for the windows. */
    static final TablePair<Row> L = pair("L", true, true);
    static final TablePair<Row> L_NOT_NULL = pair("L_NOT_NULL", false, true);

    /** A query over one pair of tables.
     * @param label    Name of the query in the tables of results.
     * @param pair     The pair of tables that the query reads.
     * @param columns  The number of columns of the query.
     * @param query    The query; {T} is the table, and {NOW} is the current time. */
    record Shape(String label, TablePair<Row> pair, int columns, String query) {
        /** The query over the table with prefix {@code prefix}.  The PLAIN_ query reads the time
         * from table CLOCK instead of NOW(). */
        String query(String prefix) {
            boolean late = prefix.equals("LATE_");
            boolean now = this.query.contains("{NOW}");
            String table = prefix + this.pair.name + (now && !late ? ", CLOCK" : "");
            return this.query.replace("{T}", table).replace("{NOW}", late ? "NOW()" : "CLOCK.t");
        }

        /** The same query over the pair whose columns are NOT NULL. */
        Shape notNull() {
            return new Shape(this.label + " NOT NULL", this.pair == N ? N_NOT_NULL : L_NOT_NULL,
                    this.columns, this.query);
        }
    }

    static final String ALL = "SELECT a, k, ts, x, b FROM {T} WHERE ";
    static final String BY_HOUR = "SELECT FLOOR(ts TO HOUR) AS h, COUNT(*) AS c FROM {T} WHERE ";
    static final String HOUR = " GROUP BY FLOOR(ts TO HOUR)";
    static final String TUMBLE = "TABLE(TUMBLE(TABLE {T}, DESCRIPTOR(ts), INTERVAL 1 HOUR))";
    static final String HOP = "TABLE(HOP(TABLE {T}, DESCRIPTOR(ts), INTERVAL 30 MINUTES, INTERVAL 1 HOUR))";
    static final String SESSION = "TABLE(SESSION(TABLE {T}, DESCRIPTOR(ts), DESCRIPTOR(k), INTERVAL 10 MINUTES))";
    static final String WINDOW_ROWS = "SELECT a, k, ts, x, b, window_start, window_end FROM ";
    static final String WINDOW_SUMS = "SELECT window_start, window_end, COUNT(*) AS c, SUM(x) AS s FROM ";

    static final List<Shape> SHAPES = List.of(
            new Shape("ts >= NOW() - 1 hour", N, 5, ALL + "ts >= {NOW} - INTERVAL 1 HOUR"),
            new Shape("ts > NOW() - 1 hour", N, 5, ALL + "ts > {NOW} - INTERVAL 1 HOUR"),
            new Shape("ts <= NOW() + 30 minutes", N, 5, ALL + "ts <= {NOW} + INTERVAL 30 MINUTES"),
            new Shape("ts < NOW() + 30 minutes", N, 5, ALL + "ts < {NOW} + INTERVAL 30 MINUTES"),
            new Shape("ts BETWEEN", N, 5,
                    ALL + "ts BETWEEN {NOW} - INTERVAL 1 HOUR AND {NOW} + INTERVAL 30 MINUTES"),
            new Shape("NOW() - 1 hour <= ts", N, 5, ALL + "{NOW} - INTERVAL 1 HOUR <= ts"),
            new Shape("first column a", N, 5, ALL + "a >= {NOW} - INTERVAL 1 HOUR"),
            new Shape("last column b", N, 5, ALL + "b >= {NOW} - INTERVAL 1 HOUR"),
            new Shape("date >= today", N, 5, ALL + "CAST(ts AS DATE) >= CAST({NOW} AS DATE)"),
            new Shape("date = today", N, 5, ALL + "CAST(ts AS DATE) = CAST({NOW} AS DATE)"),
            new Shape("ts >= NOW() - 1 hour AND x > 3", N, 5,
                    ALL + "ts >= {NOW} - INTERVAL 1 HOUR AND x > 3"),
            new Shape("NOW() filter, GROUP BY k", N, 3,
                    "SELECT k, COUNT(*) AS c, SUM(x) AS s FROM {T} WHERE ts >= {NOW} - INTERVAL 1 HOUR GROUP BY k"),
            new Shape("NOW() filter, GROUP BY hour", N, 2, BY_HOUR + "ts >= {NOW} - INTERVAL 1 HOUR" + HOUR),
            new Shape("NOW() filter, LATENESS, GROUP BY hour", L, 2,
                    BY_HOUR + "ts >= {NOW} - INTERVAL 2 HOURS" + HOUR),
            new Shape("upper bound, LATENESS, GROUP BY hour", L, 2,
                    BY_HOUR + "ts <= {NOW} + INTERVAL 30 MINUTES" + HOUR),
            new Shape("BETWEEN, LATENESS, GROUP BY hour", L, 2,
                    BY_HOUR + "ts BETWEEN {NOW} - INTERVAL 2 HOURS AND {NOW} + INTERVAL 30 MINUTES" + HOUR),
            new Shape("filter on b, LATENESS on ts, GROUP BY hour", L, 2,
                    BY_HOUR + "b >= {NOW} - INTERVAL 2 HOURS" + HOUR),
            new Shape("BETWEEN, LATENESS, rows", L, 5,
                    ALL + "ts BETWEEN {NOW} - INTERVAL 2 HOURS AND {NOW} + INTERVAL 30 MINUTES"),
            new Shape("TUMBLE rows", L, 7, WINDOW_ROWS + TUMBLE),
            new Shape("TUMBLE, GROUP BY start, end", L, 4, WINDOW_SUMS + TUMBLE + " GROUP BY window_start, window_end"),
            new Shape("TUMBLE, GROUP BY end", L, 2,
                    "SELECT window_end, SUM(x) AS s FROM " + TUMBLE + " GROUP BY window_end"),
            new Shape("TUMBLE, GROUP BY k, start", L, 3,
                    "SELECT k, window_start, SUM(x) AS s FROM " + TUMBLE + " GROUP BY k, window_start"),
            new Shape("TUMBLE with offset", L, 4,
                    WINDOW_SUMS + "TABLE(TUMBLE(TABLE {T}, DESCRIPTOR(ts), INTERVAL 1 HOUR, INTERVAL 15 MINUTES)) " +
                    "GROUP BY window_start, window_end"),
            new Shape("HOP rows", L, 7, WINDOW_ROWS + HOP),
            new Shape("HOP, GROUP BY start, end", L, 4, WINDOW_SUMS + HOP + " GROUP BY window_start, window_end"),
            new Shape("HOP with offset", L, 4,
                    WINDOW_SUMS + "TABLE(HOP(TABLE {T}, DESCRIPTOR(ts), INTERVAL 30 MINUTES, INTERVAL 1 HOUR, " +
                    "INTERVAL 10 MINUTES)) GROUP BY window_start, window_end"),
            new Shape("SESSION rows", L, 7, WINDOW_ROWS + SESSION),
            new Shape("SESSION, GROUP BY k, start, end", L, 4,
                    "SELECT k, window_start, window_end, COUNT(*) AS c FROM " + SESSION +
                    " GROUP BY k, window_start, window_end"));

    /** Each shape over its pair with nullable columns, then over the pair with NOT NULL columns. */
    static final List<Shape> ALL_SHAPES = SHAPES.stream()
            .flatMap(shape -> Stream.of(shape, shape.notNull())).toList();

    /** The GC operators of each shape; see {@link CountGCOperators#kinds()}.  The operators of
     * SESSION do not bound its state: the LAG that implements it keeps every row. */
    static final String EXPECTED_GC = """
            ts >= NOW() - 1 hour                                | W
            ts >= NOW() - 1 hour NOT NULL                       | W
            ts > NOW() - 1 hour                                 | W
            ts > NOW() - 1 hour NOT NULL                        | W
            ts <= NOW() + 30 minutes                            | -
            ts <= NOW() + 30 minutes NOT NULL                   | -
            ts < NOW() + 30 minutes                             | -
            ts < NOW() + 30 minutes NOT NULL                    | -
            ts BETWEEN                                          | W
            ts BETWEEN NOT NULL                                 | W
            NOW() - 1 hour <= ts                                | W
            NOW() - 1 hour <= ts NOT NULL                       | W
            first column a                                      | W
            first column a NOT NULL                             | W
            last column b                                       | W
            last column b NOT NULL                              | W
            date >= today                                       | W
            date >= today NOT NULL                              | W
            date = today                                        | W
            date = today NOT NULL                               | W
            ts >= NOW() - 1 hour AND x > 3                      | W
            ts >= NOW() - 1 hour AND x > 3 NOT NULL             | W
            NOW() filter, GROUP BY k                            | W
            NOW() filter, GROUP BY k NOT NULL                   | W
            NOW() filter, GROUP BY hour                         | KW
            NOW() filter, GROUP BY hour NOT NULL                | KW
            NOW() filter, LATENESS, GROUP BY hour               | KW
            NOW() filter, LATENESS, GROUP BY hour NOT NULL      | KW
            upper bound, LATENESS, GROUP BY hour                | K
            upper bound, LATENESS, GROUP BY hour NOT NULL       | K
            BETWEEN, LATENESS, GROUP BY hour                    | KW
            BETWEEN, LATENESS, GROUP BY hour NOT NULL           | KW
            filter on b, LATENESS on ts, GROUP BY hour          | W
            filter on b, LATENESS on ts, GROUP BY hour NOT NULL | W
            BETWEEN, LATENESS, rows                             | W
            BETWEEN, LATENESS, rows NOT NULL                    | W
            TUMBLE rows                                         | -
            TUMBLE rows NOT NULL                                | -
            TUMBLE, GROUP BY start, end                         | K
            TUMBLE, GROUP BY start, end NOT NULL                | K
            TUMBLE, GROUP BY end                                | K
            TUMBLE, GROUP BY end NOT NULL                       | K
            TUMBLE, GROUP BY k, start                           | K
            TUMBLE, GROUP BY k, start NOT NULL                  | K
            TUMBLE with offset                                  | K
            TUMBLE with offset NOT NULL                         | K
            HOP rows                                            | -
            HOP rows NOT NULL                                   | -
            HOP, GROUP BY start, end                            | K
            HOP, GROUP BY start, end NOT NULL                   | K
            HOP with offset                                     | K
            HOP with offset NOT NULL                            | K
            SESSION rows                                        | KKKNNRR
            SESSION rows NOT NULL                               | KKKNNRR
            SESSION, GROUP BY k, start, end                     | KKKNNRR
            SESSION, GROUP BY k, start, end NOT NULL            | KKKNNRR
            """;

    /** The GC operators of each shape compiled alone, one line per shape. */
    String gcTable() {
        int width = 0;
        for (Shape shape : ALL_SHAPES)
            width = Math.max(width, shape.label.length());
        StringBuilder result = new StringBuilder();
        for (Shape shape : ALL_SHAPES) {
            String sql = shape.pair.create("LATE_", true) + "CREATE VIEW V AS " + shape.query("LATE_") + ";";
            var cc = this.getCC(sql);
            CountGCOperators operators = new CountGCOperators(cc.compiler);
            cc.visit(operators);
            result.append(String.format("%-" + width + "s | %s", shape.label, operators.kinds())).append("\n");
        }
        return result.toString();
    }

    @Test
    public void gcOperators() {
        Assert.assertEquals(EXPECTED_GC, this.gcTable());
    }

    /** The columns of views LATE_ALL and PLAIN_ALL: the label of the shape, and its columns. */
    static final List<String> COLUMNS = List.of("e", "c1", "c2", "c3", "c4", "c5", "c6", "c7");

    /** The branches of view ALL over the tables with prefix {@code prefix}: each shape, labeled,
     * with its columns cast to VARCHAR and padded with NULL to {@link #COLUMNS}. */
    static List<String> branches(String prefix) {
        List<String> result = new ArrayList<>();
        for (Shape shape : ALL_SHAPES) {
            List<String> names = new ArrayList<>();
            List<String> values = new ArrayList<>();
            for (int i = 1; i < COLUMNS.size(); i++) {
                String column = COLUMNS.get(i);
                if (i <= shape.columns) {
                    names.add(column);
                    values.add("CAST(q." + column + " AS VARCHAR) AS " + column);
                } else {
                    values.add("CAST(NULL AS VARCHAR) AS " + column);
                }
            }
            result.add("SELECT '" + shape.label + "' AS e, " + String.join(", ", values) +
                    " FROM (" + shape.query(prefix) + ") AS q(" + String.join(", ", names) + ")");
        }
        return result;
    }

    static final LocalDateTime BASE = LocalDateTime.of(2024, 3, 10, 22, 0, 0);

    /** The time {@code minutes} after BASE. */
    static LocalDateTime at(int minutes) {
        return BASE.plusMinutes(minutes);
    }

    /** A row with key k, value x, and time ts {@code minutes} after BASE. */
    static Row r(int k, int minutes, int x) {
        LocalDateTime ts = at(minutes);
        return new Row(ts.minusMinutes(5), k, ts, x, ts.plusMinutes(5));
    }

    /** A row with key k, value x, and no times. */
    static Row noTime(int k, int x) {
        return new Row(null, k, null, x, null);
    }

    /** Check that each shape gives the same output with NOW() and LATENESS as with table CLOCK
     * and without LATENESS. */
    @Test
    public void sameOutputWithoutGC() {
        List<TablePair<?>> pairs = List.of(N, N_NOT_NULL, L, L_NOT_NULL);
        StringBuilder sql = new StringBuilder(DifferentialTester.CLOCK);
        for (TablePair<?> pair : pairs) {
            sql.append(pair.create("LATE_", true));
            sql.append(pair.create("PLAIN_", false));
        }
        for (String prefix : TablePair.PREFIXES)
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", branches(prefix))).append(";\n");
        sql.append(DifferentialTester.differenceView(COLUMNS));
        var tester = new DifferentialTester(this.getCCS(sql.toString()), pairs, COLUMNS);
        Set<String> both = Set.of("N", "L");

        // 22:00.  The waterline of LATE_L moves to 22:00, one hour before its latest time
        tester.setNow(at(0));
        tester.insert(both, r(1, -90, 1), r(1, -60, 2), r(2, -59, 3), r(1, -30, 4), r(2, 0, 5),
                r(1, 10, 6), r(2, 29, 7), r(1, 30, 8), r(2, 31, 9), r(1, 60, 10), noTime(1, 11));
        // 22:30.  A row on the waterline of LATE_L, a row late for LATE_L, and a row in the future;
        // the waterline moves to 23:00
        tester.setNow(at(30));
        tester.insert(both, r(2, 0, 12), r(1, -1, 13), r(1, 120, 14));
        // 23:01.  The rows of 22:00 are more than one hour old
        tester.advance(at(61));
        // 23:30.  Rows on the bounds NOW() - 1 hour and NOW() + 30 minutes
        tester.setNow(at(90));
        tester.insert(both, r(2, 30, 15), r(1, 120, 16));
        tester.delete(Set.of("N"), r(2, 31, 9));
        // 00:05.  The date changes
        tester.advance(at(125));
        // A row inserted twice, and a delete in the future
        tester.setNow(at(126));
        tester.insert(both, r(1, 125, 17), r(1, 125, 17), r(2, 126, 18));
        tester.delete(both, r(1, 120, 14));
        // Two sessions of key 2, which a third row merges, and its delete splits again
        tester.insert(both, r(2, 140, 21), r(2, 155, 22));
        tester.insert(both, r(2, 147, 23));
        tester.delete(both, r(2, 147, 23));
        // The session of key 2 at 00:35 gets a second row, at 00:32
        tester.insert(both, r(2, 152, 27));
        // A row of key 3 moves the waterline of LATE_L to 00:38.  The session of key 2 from 00:32
        // to 00:35 is below the waterline, and an on-time row at 00:40 extends it
        tester.insert(both, r(3, 218, 25));
        tester.insert(both, r(2, 160, 26));
        // 01:20.  Rows far in the future, and a row without times; the waterline of LATE_L
        // moves to 07:00, after the row of 03:00
        tester.setNow(at(200));
        tester.insert(both, r(2, 300, 19), noTime(2, 20), r(1, 600, 24));
        tester.delete(both, noTime(1, 11));
        // 04:40.  The row of 03:00 enters the windows with an upper bound, below the waterline
        tester.advance(at(400));
    }
}
