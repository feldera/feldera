package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.ShareWindowIntegrals;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/** Tests for {@link ShareWindowIntegrals}. */
public class ShareWindowIntegralsTests extends SqlIoTest {
    static final String PREAMBLE = """
            SET feldera_window_sharing_threshold = 1;
            CREATE TABLE T(a INT, b INT, c INT, ts TIMESTAMP, ts2 TIMESTAMP);
            """;

    @Test
    public void differentProjectionsShareOneInput() {
        // V1 uses field 'b', V2 uses field 'c', and both use 'a' (for the filter).
        WindowInputStats windows = WindowInputStats.windows(this.getCC(PREAMBLE + """
                CREATE VIEW V1 AS SELECT b FROM T WHERE ts >= NOW() - INTERVAL 1 HOURS AND a > 2;
                CREATE VIEW V2 AS SELECT c FROM T WHERE ts >= NOW() - INTERVAL 2 HOURS AND a < 9;"""));
        Assert.assertEquals(2, windows.leftInputIds.size());
        Assert.assertEquals(1, windows.distinctInputCount());
        // The shared index carries all three fields.
        Assert.assertEquals(List.of(3, 3), windows.leftInputValueWidth);
    }

    @Test
    public void differentKeysDoNotShare() {
        // Different timestamps: windows do not share inputs
        WindowInputStats windows = WindowInputStats.windows(this.getCC(PREAMBLE + """
                CREATE VIEW V1 AS SELECT b FROM T WHERE ts >= NOW() - INTERVAL 1 HOURS AND a > 2;
                CREATE VIEW V2 AS SELECT c FROM T WHERE ts2 >= NOW() - INTERVAL 2 HOURS AND a < 9;"""));
        Assert.assertEquals(2, windows.leftInputIds.size());
        Assert.assertEquals(2, windows.distinctInputCount());
    }

    @Test
    public void manyViewsShareOneInput() {
        // 6 views that share the same left input
        StringBuilder sql = new StringBuilder(PREAMBLE);
        for (int i = 1; i <= 6; i++)
            sql.append("CREATE VIEW V").append(i).append(" AS SELECT b, c FROM T WHERE ts >= NOW() - INTERVAL ")
                    .append(i).append(" HOURS AND a > ").append(i).append(";\n");
        WindowInputStats windows = WindowInputStats.windows(this.getCC(sql.toString()));
        Assert.assertEquals(6, windows.leftInputIds.size());
        Assert.assertEquals(1, windows.distinctInputCount());
    }

    @Test
    public void twoBoundsStillMakeOneWindow() {
        // A BETWEEN becomes one window with both bounds, which shares with a one-sided window.
        WindowInputStats windows = WindowInputStats.windows(this.getCC(PREAMBLE + """
                CREATE VIEW V1 AS SELECT b FROM T
                WHERE ts BETWEEN NOW() - INTERVAL 4 HOURS AND NOW() AND a > 2;
                CREATE VIEW V2 AS SELECT c FROM T
                WHERE ts >= NOW() - INTERVAL 2 HOURS AND a < 9;"""));
        Assert.assertEquals(2, windows.leftInputIds.size());
        Assert.assertEquals(1, windows.distinctInputCount());
    }

    @Test
    public void resultsSurviveSharing() {
        // Checks that non-temporal filters are not lost
        String sql = PREAMBLE + """
                CREATE LOCAL VIEW V1 AS SELECT b FROM T
                WHERE ts >= NOW() - INTERVAL 1 MONTHS AND a > 2;
                CREATE LOCAL VIEW V2 AS SELECT c FROM T
                WHERE ts >= NOW() - INTERVAL 2 MONTHS AND a < 9;
                CREATE VIEW V AS
                SELECT b AS v, 1 AS q FROM V1
                UNION ALL
                SELECT c AS v, 2 AS q FROM V2;""";
        var ccs = this.getCCS(sql);
        // Every row is inside both windows, so each view is decided by its filter on 'a' alone.
        // Validated on Postgres (by carefully simulating NOW).
        ccs.stepWeightOne("""
                INSERT INTO T VALUES (1, 10, 100, '2024-12-01 00:00:00', NULL),
                                     (5, 20, 200, '2024-12-02 00:00:00', NULL),
                                     (20, 30, 300, '2024-12-03 00:00:00', NULL);
                INSERT INTO now VALUES ('2024-12-12 00:00:00');""", """
                  v  | q
                ---------
                  20 | 1
                  30 | 1
                 100 | 2
                 200 | 2""");
    }

    /** A table whose rows carry a ROW-typed column. */
    static final String ROW_PREAMBLE = """
            SET feldera_window_sharing_threshold = 1;
            CREATE TABLE T(a INT, r ROW(x INT, y VARCHAR), ts TIMESTAMP);
            """;

    @Test
    public void rowValueFieldIsShared() {
        WindowInputStats windows = WindowInputStats.windows(this.getCC(ROW_PREAMBLE + """
                CREATE VIEW V1 AS SELECT r FROM T WHERE ts >= NOW() - INTERVAL 1 HOURS AND a > 2;
                CREATE VIEW V2 AS SELECT a, r FROM T WHERE ts >= NOW() - INTERVAL 2 HOURS AND a < 9;"""));
        Assert.assertEquals(2, windows.leftInputIds.size());
        Assert.assertEquals(1, windows.distinctInputCount());
        // Both views need 'a' and 'r', so the shared value has two fields
        Assert.assertEquals(List.of(2, 2), windows.leftInputValueWidth);
    }

    @Test
    public void rowValuesSurviveSharing() {
        // ROW-typed fields
        String sql = ROW_PREAMBLE + """
                CREATE LOCAL VIEW V1 AS SELECT r FROM T
                WHERE ts >= NOW() - INTERVAL 1 MONTHS AND a > 2;
                CREATE LOCAL VIEW V2 AS SELECT r FROM T
                WHERE ts >= NOW() - INTERVAL 2 MONTHS AND a < 9;
                CREATE VIEW V AS
                SELECT r.x AS x, r.y AS y, 1 AS q FROM V1
                UNION ALL
                SELECT r.x AS x, r.y AS y, 2 AS q FROM V2;""";
        var ccs = this.getCCS(sql).withStringTrim();
        ccs.stepWeightOne("""
                INSERT INTO T VALUES (1, ROW(10, 'ten'), '2024-12-01 00:00:00'),
                                     (5, ROW(20, 'twenty'), '2024-12-02 00:00:00'),
                                     (20, ROW(30, 'thirty'), '2024-12-03 00:00:00');
                INSERT INTO now VALUES ('2024-12-12 00:00:00');""", """
                 x  | y      | q
                ----------------
                 20 | twenty | 1
                 30 | thirty | 1
                 10 | ten    | 2
                 20 | twenty | 2""");
    }

    @Test
    public void windowsOutsideTheGroupAreUntouched() {
        showFinal();
        WindowInputStats windows = WindowInputStats.windows(this.getCC(PREAMBLE + """
                CREATE VIEW V1 AS SELECT b FROM T WHERE ts >= NOW() - INTERVAL 1 HOURS AND a > 2;"""));
        Assert.assertEquals(1, windows.leftInputIds.size());
        // The filter 'a > 2' is applied before the window, so the input has a single field
        Assert.assertEquals(1, windows.distinctInputCount());
    }
}
