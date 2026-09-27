package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** RANGE window frames: every ordering and every shape of frame, over a nullable and a NOT NULL
 * ORDER BY column.  Some programs are rejected at compile time; the others must produce the results
 * of Postgres. */
public class RangeFrameTests extends SqlIoTest {
    static final String TABLE = "CREATE TABLE T (ts SMALLINT, x INT NOT NULL);\n";
    static final String NOT_NULL_TABLE = "CREATE TABLE T (ts SMALLINT NOT NULL, x INT NOT NULL);\n";
    /** Each x is a distinct power of 2, so a sum names the rows of its frame.  Two NULL peers,
     * the ends of SMALLINT, and two peers at 0. */
    static final String DATA = "INSERT INTO T VALUES (NULL, 1), (NULL, 2), (-32768, 4), (-32767, 8), " +
            "(0, 16), (0, 32), (32766, 64), (32767, 128);";
    /** The rows of DATA without a NULL ts. */
    static final String NOT_NULL_DATA = "INSERT INTO T VALUES (-32768, 4), (-32767, 8), " +
            "(0, 16), (0, 32), (32766, 64), (32767, 128);";
    /** The ts and x of each row, in the order of x. */
    static final String[] TS = { "NULL", "NULL", "-32768", "-32767", "0", "0", "32766", "32767" };
    static final int[] X = { 1, 2, 4, 8, 16, 32, 64, 128 };
    static final String[] ORDERINGS = {
            "ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST" };
    /** Every valid pair of bounds, as (lower, upper). */
    static final String[][] FRAMES = {
            { "UNBOUNDED PRECEDING", "1 PRECEDING" },
            { "UNBOUNDED PRECEDING", "CURRENT ROW" },
            { "UNBOUNDED PRECEDING", "2 FOLLOWING" },
            { "UNBOUNDED PRECEDING", "UNBOUNDED FOLLOWING" },
            { "2 PRECEDING", "1 PRECEDING" },
            { "2 PRECEDING", "CURRENT ROW" },
            { "2 PRECEDING", "2 FOLLOWING" },
            { "2 PRECEDING", "UNBOUNDED FOLLOWING" },
            { "CURRENT ROW", "CURRENT ROW" },
            { "CURRENT ROW", "2 FOLLOWING" },
            { "CURRENT ROW", "UNBOUNDED FOLLOWING" },
            { "1 FOLLOWING", "2 FOLLOWING" },
            { "1 FOLLOWING", "UNBOUNDED FOLLOWING" },
    };

    /** For each shape, the sum over the frame of each row of T, in the order of x.
     * Computed by Postgres. */
    static final String NULLABLE_SUMS = """
            ASC NULLS FIRST, UNBOUNDED PRECEDING, 1 PRECEDING: 3 3 3 7 15 15 63 127
            ASC NULLS FIRST, UNBOUNDED PRECEDING, CURRENT ROW: 3 3 7 15 63 63 127 255
            ASC NULLS FIRST, UNBOUNDED PRECEDING, 2 FOLLOWING: 3 3 15 15 63 63 255 255
            ASC NULLS FIRST, UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING: 255 255 255 255 255 255 255 255
            ASC NULLS FIRST, 2 PRECEDING, 1 PRECEDING: 3 3 NULL 4 NULL NULL NULL 64
            ASC NULLS FIRST, 2 PRECEDING, CURRENT ROW: 3 3 4 12 48 48 64 192
            ASC NULLS FIRST, 2 PRECEDING, 2 FOLLOWING: 3 3 12 12 48 48 192 192
            ASC NULLS FIRST, 2 PRECEDING, UNBOUNDED FOLLOWING: 255 255 252 252 240 240 192 192
            ASC NULLS FIRST, CURRENT ROW, CURRENT ROW: 3 3 4 8 48 48 64 128
            ASC NULLS FIRST, CURRENT ROW, 2 FOLLOWING: 3 3 12 8 48 48 192 128
            ASC NULLS FIRST, CURRENT ROW, UNBOUNDED FOLLOWING: 255 255 252 248 240 240 192 128
            ASC NULLS FIRST, 1 FOLLOWING, 2 FOLLOWING: 3 3 8 NULL NULL NULL 128 NULL
            ASC NULLS FIRST, 1 FOLLOWING, UNBOUNDED FOLLOWING: 255 255 248 240 192 192 128 NULL
            ASC NULLS LAST, UNBOUNDED PRECEDING, 1 PRECEDING: 255 255 NULL 4 12 12 60 124
            ASC NULLS LAST, UNBOUNDED PRECEDING, CURRENT ROW: 255 255 4 12 60 60 124 252
            ASC NULLS LAST, UNBOUNDED PRECEDING, 2 FOLLOWING: 255 255 12 12 60 60 252 252
            ASC NULLS LAST, UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING: 255 255 255 255 255 255 255 255
            ASC NULLS LAST, 2 PRECEDING, 1 PRECEDING: 3 3 NULL 4 NULL NULL NULL 64
            ASC NULLS LAST, 2 PRECEDING, CURRENT ROW: 3 3 4 12 48 48 64 192
            ASC NULLS LAST, 2 PRECEDING, 2 FOLLOWING: 3 3 12 12 48 48 192 192
            ASC NULLS LAST, 2 PRECEDING, UNBOUNDED FOLLOWING: 3 3 255 255 243 243 195 195
            ASC NULLS LAST, CURRENT ROW, CURRENT ROW: 3 3 4 8 48 48 64 128
            ASC NULLS LAST, CURRENT ROW, 2 FOLLOWING: 3 3 12 8 48 48 192 128
            ASC NULLS LAST, CURRENT ROW, UNBOUNDED FOLLOWING: 3 3 255 251 243 243 195 131
            ASC NULLS LAST, 1 FOLLOWING, 2 FOLLOWING: 3 3 8 NULL NULL NULL 128 NULL
            ASC NULLS LAST, 1 FOLLOWING, UNBOUNDED FOLLOWING: 3 3 251 243 195 195 131 3
            DESC NULLS FIRST, UNBOUNDED PRECEDING, 1 PRECEDING: 3 3 251 243 195 195 131 3
            DESC NULLS FIRST, UNBOUNDED PRECEDING, CURRENT ROW: 3 3 255 251 243 243 195 131
            DESC NULLS FIRST, UNBOUNDED PRECEDING, 2 FOLLOWING: 3 3 255 255 243 243 195 195
            DESC NULLS FIRST, UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING: 255 255 255 255 255 255 255 255
            DESC NULLS FIRST, 2 PRECEDING, 1 PRECEDING: 3 3 8 NULL NULL NULL 128 NULL
            DESC NULLS FIRST, 2 PRECEDING, CURRENT ROW: 3 3 12 8 48 48 192 128
            DESC NULLS FIRST, 2 PRECEDING, 2 FOLLOWING: 3 3 12 12 48 48 192 192
            DESC NULLS FIRST, 2 PRECEDING, UNBOUNDED FOLLOWING: 255 255 12 12 60 60 252 252
            DESC NULLS FIRST, CURRENT ROW, CURRENT ROW: 3 3 4 8 48 48 64 128
            DESC NULLS FIRST, CURRENT ROW, 2 FOLLOWING: 3 3 4 12 48 48 64 192
            DESC NULLS FIRST, CURRENT ROW, UNBOUNDED FOLLOWING: 255 255 4 12 60 60 124 252
            DESC NULLS FIRST, 1 FOLLOWING, 2 FOLLOWING: 3 3 NULL 4 NULL NULL NULL 64
            DESC NULLS FIRST, 1 FOLLOWING, UNBOUNDED FOLLOWING: 255 255 NULL 4 12 12 60 124
            DESC NULLS LAST, UNBOUNDED PRECEDING, 1 PRECEDING: 255 255 248 240 192 192 128 NULL
            DESC NULLS LAST, UNBOUNDED PRECEDING, CURRENT ROW: 255 255 252 248 240 240 192 128
            DESC NULLS LAST, UNBOUNDED PRECEDING, 2 FOLLOWING: 255 255 252 252 240 240 192 192
            DESC NULLS LAST, UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING: 255 255 255 255 255 255 255 255
            DESC NULLS LAST, 2 PRECEDING, 1 PRECEDING: 3 3 8 NULL NULL NULL 128 NULL
            DESC NULLS LAST, 2 PRECEDING, CURRENT ROW: 3 3 12 8 48 48 192 128
            DESC NULLS LAST, 2 PRECEDING, 2 FOLLOWING: 3 3 12 12 48 48 192 192
            DESC NULLS LAST, 2 PRECEDING, UNBOUNDED FOLLOWING: 3 3 15 15 63 63 255 255
            DESC NULLS LAST, CURRENT ROW, CURRENT ROW: 3 3 4 8 48 48 64 128
            DESC NULLS LAST, CURRENT ROW, 2 FOLLOWING: 3 3 4 12 48 48 64 192
            DESC NULLS LAST, CURRENT ROW, UNBOUNDED FOLLOWING: 3 3 7 15 63 63 127 255
            DESC NULLS LAST, 1 FOLLOWING, 2 FOLLOWING: 3 3 NULL 4 NULL NULL NULL 64
            DESC NULLS LAST, 1 FOLLOWING, UNBOUNDED FOLLOWING: 3 3 3 7 15 15 63 127""";

    /** For each shape, the sum over the frame of each row of DATA with a non-NULL ts, in the
     * order of x.  Computed by Postgres. */
    static final String NOT_NULL_SUMS = """
            ASC NULLS FIRST, UNBOUNDED PRECEDING, 1 PRECEDING: NULL 4 12 12 60 124
            ASC NULLS FIRST, UNBOUNDED PRECEDING, CURRENT ROW: 4 12 60 60 124 252
            ASC NULLS FIRST, UNBOUNDED PRECEDING, 2 FOLLOWING: 12 12 60 60 252 252
            ASC NULLS FIRST, UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING: 252 252 252 252 252 252
            ASC NULLS FIRST, 2 PRECEDING, 1 PRECEDING: NULL 4 NULL NULL NULL 64
            ASC NULLS FIRST, 2 PRECEDING, CURRENT ROW: 4 12 48 48 64 192
            ASC NULLS FIRST, 2 PRECEDING, 2 FOLLOWING: 12 12 48 48 192 192
            ASC NULLS FIRST, 2 PRECEDING, UNBOUNDED FOLLOWING: 252 252 240 240 192 192
            ASC NULLS FIRST, CURRENT ROW, CURRENT ROW: 4 8 48 48 64 128
            ASC NULLS FIRST, CURRENT ROW, 2 FOLLOWING: 12 8 48 48 192 128
            ASC NULLS FIRST, CURRENT ROW, UNBOUNDED FOLLOWING: 252 248 240 240 192 128
            ASC NULLS FIRST, 1 FOLLOWING, 2 FOLLOWING: 8 NULL NULL NULL 128 NULL
            ASC NULLS FIRST, 1 FOLLOWING, UNBOUNDED FOLLOWING: 248 240 192 192 128 NULL
            ASC NULLS LAST, UNBOUNDED PRECEDING, 1 PRECEDING: NULL 4 12 12 60 124
            ASC NULLS LAST, UNBOUNDED PRECEDING, CURRENT ROW: 4 12 60 60 124 252
            ASC NULLS LAST, UNBOUNDED PRECEDING, 2 FOLLOWING: 12 12 60 60 252 252
            ASC NULLS LAST, UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING: 252 252 252 252 252 252
            ASC NULLS LAST, 2 PRECEDING, 1 PRECEDING: NULL 4 NULL NULL NULL 64
            ASC NULLS LAST, 2 PRECEDING, CURRENT ROW: 4 12 48 48 64 192
            ASC NULLS LAST, 2 PRECEDING, 2 FOLLOWING: 12 12 48 48 192 192
            ASC NULLS LAST, 2 PRECEDING, UNBOUNDED FOLLOWING: 252 252 240 240 192 192
            ASC NULLS LAST, CURRENT ROW, CURRENT ROW: 4 8 48 48 64 128
            ASC NULLS LAST, CURRENT ROW, 2 FOLLOWING: 12 8 48 48 192 128
            ASC NULLS LAST, CURRENT ROW, UNBOUNDED FOLLOWING: 252 248 240 240 192 128
            ASC NULLS LAST, 1 FOLLOWING, 2 FOLLOWING: 8 NULL NULL NULL 128 NULL
            ASC NULLS LAST, 1 FOLLOWING, UNBOUNDED FOLLOWING: 248 240 192 192 128 NULL
            DESC NULLS FIRST, UNBOUNDED PRECEDING, 1 PRECEDING: 248 240 192 192 128 NULL
            DESC NULLS FIRST, UNBOUNDED PRECEDING, CURRENT ROW: 252 248 240 240 192 128
            DESC NULLS FIRST, UNBOUNDED PRECEDING, 2 FOLLOWING: 252 252 240 240 192 192
            DESC NULLS FIRST, UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING: 252 252 252 252 252 252
            DESC NULLS FIRST, 2 PRECEDING, 1 PRECEDING: 8 NULL NULL NULL 128 NULL
            DESC NULLS FIRST, 2 PRECEDING, CURRENT ROW: 12 8 48 48 192 128
            DESC NULLS FIRST, 2 PRECEDING, 2 FOLLOWING: 12 12 48 48 192 192
            DESC NULLS FIRST, 2 PRECEDING, UNBOUNDED FOLLOWING: 12 12 60 60 252 252
            DESC NULLS FIRST, CURRENT ROW, CURRENT ROW: 4 8 48 48 64 128
            DESC NULLS FIRST, CURRENT ROW, 2 FOLLOWING: 4 12 48 48 64 192
            DESC NULLS FIRST, CURRENT ROW, UNBOUNDED FOLLOWING: 4 12 60 60 124 252
            DESC NULLS FIRST, 1 FOLLOWING, 2 FOLLOWING: NULL 4 NULL NULL NULL 64
            DESC NULLS FIRST, 1 FOLLOWING, UNBOUNDED FOLLOWING: NULL 4 12 12 60 124
            DESC NULLS LAST, UNBOUNDED PRECEDING, 1 PRECEDING: 248 240 192 192 128 NULL
            DESC NULLS LAST, UNBOUNDED PRECEDING, CURRENT ROW: 252 248 240 240 192 128
            DESC NULLS LAST, UNBOUNDED PRECEDING, 2 FOLLOWING: 252 252 240 240 192 192
            DESC NULLS LAST, UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING: 252 252 252 252 252 252
            DESC NULLS LAST, 2 PRECEDING, 1 PRECEDING: 8 NULL NULL NULL 128 NULL
            DESC NULLS LAST, 2 PRECEDING, CURRENT ROW: 12 8 48 48 192 128
            DESC NULLS LAST, 2 PRECEDING, 2 FOLLOWING: 12 12 48 48 192 192
            DESC NULLS LAST, 2 PRECEDING, UNBOUNDED FOLLOWING: 12 12 60 60 252 252
            DESC NULLS LAST, CURRENT ROW, CURRENT ROW: 4 8 48 48 64 128
            DESC NULLS LAST, CURRENT ROW, 2 FOLLOWING: 4 12 48 48 64 192
            DESC NULLS LAST, CURRENT ROW, UNBOUNDED FOLLOWING: 4 12 60 60 124 252
            DESC NULLS LAST, 1 FOLLOWING, 2 FOLLOWING: NULL 4 NULL NULL NULL 64
            DESC NULLS LAST, 1 FOLLOWING, UNBOUNDED FOLLOWING: NULL 4 12 12 60 124""";

    /** Every shape over the nullable column; the 9 frames with an offset are rejected for each
     * of the 4 orderings. */
    @Test
    public void everyFrameOverNullableColumn() {
        int rejected = this.checkFrames(TABLE, DATA, "T", NULLABLE_SUMS, 0);
        Assert.assertEquals(36, rejected);
    }

    /** Every shape over the nullable column after the NULL values are filtered out. */
    @Test
    public void everyFrameAfterFilteringNulls() {
        int rejected = this.checkFrames(TABLE, DATA, "(SELECT * FROM T WHERE ts IS NOT NULL) nn", NOT_NULL_SUMS, 2);
        Assert.assertEquals(0, rejected);
    }

    /** Every shape over a NOT NULL column. */
    @Test
    public void everyFrameOverNotNullColumn() {
        int rejected = this.checkFrames(NOT_NULL_TABLE, NOT_NULL_DATA, "T", NOT_NULL_SUMS, 2);
        Assert.assertEquals(0, rejected);
    }

    /** The rejection covers every type of ORDER BY column and every source of NULL values, and a
     * filter that is not true for NULL makes a column acceptable. */
    @Test
    public void offsetFramesNeedNonNullColumn() {
        String frame = "SUM(x) OVER (ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)";
        // The nullable side of a LEFT JOIN
        this.statementsFailingInCompilation(NOT_NULL_TABLE + """
                CREATE TABLE U (k INT NOT NULL, y INT NOT NULL);
                CREATE VIEW V AS SELECT SUM(x) OVER
                  (ORDER BY U.y RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM T LEFT JOIN U ON T.x = U.k;""",
                "A RANGE window frame with a PRECEDING or FOLLOWING offset");
        // QUALIFY filters the rows after the window computes them
        this.statementsFailingInCompilation(TABLE +
                "CREATE VIEW V AS SELECT x FROM T QUALIFY " + frame + " > 0;",
                "A RANGE window frame with a PRECEDING or FOLLOWING offset");
        // NULL IN (...) is never true
        this.getCC(TABLE + "CREATE TABLE W (w SMALLINT);\n" +
                "CREATE VIEW V AS SELECT " + frame + " FROM T WHERE ts IN (SELECT w FROM W);");
        // An expression that is NULL exactly when ts is NULL
        this.getCC(TABLE + "CREATE VIEW V AS SELECT SUM(x) OVER " +
                "(ORDER BY ts + 1 RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM T WHERE ts IS NOT NULL;");
        this.statementsFailingInCompilation("""
                CREATE TABLE D (d DATE, x INT NOT NULL);
                CREATE VIEW V AS SELECT SUM(x) OVER
                  (ORDER BY d RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM D;""",
                "A RANGE window frame with a PRECEDING or FOLLOWING offset");
        this.getCC(TABLE + """
                CREATE VIEW V AS SELECT SUM(x) OVER
                  (ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM T WHERE ts > 0;""");
    }

    /** Run one view with every shape that compiles over {@code source}, and compare with the
     * sums computed by Postgres.  The program for the nullable column starts with
     * <pre>
     * CREATE TABLE T (ts SMALLINT, x INT NOT NULL);
     * CREATE VIEW V AS SELECT 'ASC NULLS FIRST, UNBOUNDED PRECEDING, CURRENT ROW' AS frame, ts, x,
     *   SUM(x) OVER (ORDER BY ts ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM T
     * UNION ALL SELECT 'ASC NULLS FIRST, UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING' AS frame, ts, x,
     *   SUM(x) OVER (ORDER BY ts ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM T
     * UNION ALL ...;
     * </pre>
     * @param table     Declaration of table T.
     * @param data      Contents of table T.
     * @param source    The relation that the view reads.
     * @param sums      The sums computed by Postgres for every shape.
     * @param firstRow  Index in TS of the first row of {@code source}; the rows before it have a NULL ts.
     * @return The number of shapes rejected at compile time. */
    int checkFrames(String table, String data, String source, String sums, int firstRow) {
        Map<String, String[]> expectedSums = new HashMap<>();
        for (String line : sums.split("\n")) {
            String[] labelAndSums = line.split(": ");
            expectedSums.put(labelAndSums[0], labelAndSums[1].split(" "));
        }
        List<String> branches = new ArrayList<>();
        int rejected = 0;
        StringBuilder expected = new StringBuilder(" frame | ts | x | s\n------------------");
        for (String ordering : ORDERINGS) {
            for (String[] frame : FRAMES) {
                String label = ordering + ", " + frame[0] + ", " + frame[1];
                String branch = "SELECT '" + label + "' AS frame, ts, x, SUM(x) OVER (ORDER BY ts " +
                        ordering + " RANGE BETWEEN " + frame[0] + " AND " + frame[1] + ") AS s FROM " + source;
                if (this.isRejected(table + "CREATE VIEW V AS " + branch + ";")) {
                    rejected++;
                    continue;
                }
                branches.add(branch);
                String[] rowSums = expectedSums.get(label);
                for (int row = firstRow; row < TS.length; row++)
                    expected.append("\n ").append(label).append(" ").append(cell(TS[row]))
                            .append(cell(Integer.toString(X[row]))).append(cell(rowSums[row - firstRow]));
            }
        }
        var ccs = this.getCCS(table + "CREATE VIEW V AS " + String.join("\nUNION ALL ", branches) + ";")
                .withStringTrim();
        ccs.stepWeightOne(data, expected.toString());
        return rejected;
    }

    /** True if {@code program} fails to compile because of a RANGE frame over a nullable column. */
    boolean isRejected(String program) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.submitStatementsForCompilation(program);
        compiler.getFinalCircuit(true);
        if (compiler.messages.exitCode == 0)
            return false;
        String messages = compiler.messages.toString();
        Assert.assertTrue(messages, messages.contains("A RANGE window frame with a PRECEDING or FOLLOWING offset"));
        return true;
    }

    /** A table cell; a NULL cell has no space after the separator. */
    static String cell(String value) {
        return value.equals("NULL") ? "|NULL" : "| " + value + " ";
    }
}
