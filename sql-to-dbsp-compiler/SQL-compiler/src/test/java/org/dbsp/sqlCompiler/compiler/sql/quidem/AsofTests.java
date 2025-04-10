package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.junit.Ignore;
import org.junit.Test;

public class AsofTests extends StreamingTestBase {
    @Test @Ignore("Only left implemented")
    public void testAsof() {
        this.qs("""
                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t < t1.t
                ON t1.k = t2.k;
                +---+---+----+----+
                | K | T | K0 | T0 |
                +---+---+----+----+
                | 1 | 3 |  1 |  2 |
                | 1 | 4 |  1 |  3 |
                | 2 | 3 |  2 |  0 |
                +---+---+----+----+
                (3 rows)

                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k1, ts1)
                ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k2, ts2)
                MATCH_CONDITION ts2 < ts1
                ON k1 = k2;
                +----+-----+----+-----+
                | K1 | TS1 | K2 | TS2 |
                +----+-----+----+-----+
                |  1 |   3 |  1 |   2 |
                |  1 |   4 |  1 |   3 |
                |  2 |   3 |  2 |   0 |
                +----+-----+----+-----+
                (3 rows)

                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t > t1.t
                ON t1.k = t2.k;
                +---+---+----+----+
                | K | T | K0 | T0 |
                +---+---+----+----+
                | 1 | 0 |  1 |  2 |
                | 1 | 1 |  1 |  2 |
                | 1 | 2 |  1 |  3 |
                | 2 | 3 |  2 | 10 |
                +---+---+----+----+
                (4 rows)

                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t >= t1.t
                ON t1.k = t2.k;
                +---+---+----+----+
                | K | T | K0 | T0 |
                +---+---+----+----+
                | 1 | 0 |  1 |  2 |
                | 1 | 1 |  1 |  2 |
                | 1 | 2 |  1 |  2 |
                | 1 | 3 |  1 |  3 |
                | 2 | 3 |  2 | 10 |
                +---+---+----+----+
                (5 rows)

                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t <= t1.t
                ON t1.k = t2.k;
                +---+---+----+----+
                | K | T | K0 | T0 |
                +---+---+----+----+
                | 1 | 2 |  1 |  2 |
                | 1 | 3 |  1 |  3 |
                | 1 | 4 |  1 |  3 |
                | 2 | 3 |  2 |  0 |
                +---+---+----+----+
                (4 rows)""");
    }

    @Test
    public void testLeftAsofGE() {
        this.qs("""
                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                LEFT ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t <= t1.t
                ON t1.k = t2.k;
                +---+---+----+----+
                | K | T | K0 | T0 |
                +---+---+----+----+
                |   | 0 |    |    |
                | 1 |   |    |    |
                | 1 | 0 |    |    |
                | 1 | 1 |    |    |
                | 1 | 2 |  1 |  2 |
                | 1 | 3 |  1 |  3 |
                | 1 | 4 |  1 |  3 |
                | 2 | 3 |  2 |  0 |
                | 3 | 4 |    |    |
                +---+---+----+----+
                (9 rows)""");
    }

    @Test @Ignore("Not all comparisons supported")
    public void testLeftAsof() {
        this.qs("""
                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                LEFT ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t < t1.t
                ON t1.k = t2.k;
                +---+---+----+----+
                | K | T | K0 | T0 |
                +---+---+----+----+
                |   | 0 |    |    |
                | 1 |   |    |    |
                | 1 | 0 |    |    |
                | 1 | 1 |    |    |
                | 1 | 2 |    |    |
                | 1 | 3 |  1 |  2 |
                | 1 | 4 |  1 |  3 |
                | 2 | 3 |  2 |  0 |
                | 3 | 4 |    |    |
                +---+---+----+----+
                (9 rows)

                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                LEFT ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t > t1.t
                ON t1.k = t2.k;
                +---+---+----+----+
                | K | T | K0 | T0 |
                +---+---+----+----+
                |   | 0 |    |    |
                | 1 |   |    |    |
                | 1 | 0 |  1 |  2 |
                | 1 | 1 |  1 |  2 |
                | 1 | 2 |  1 |  3 |
                | 1 | 3 |    |    |
                | 1 | 4 |    |    |
                | 2 | 3 |  2 | 10 |
                | 3 | 4 |    |    |
                +---+---+----+----+
                (9 rows)

                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                LEFT ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t >= t1.t
                ON t1.k = t2.k;
                +---+---+----+----+
                | K | T | K0 | T0 |
                +---+---+----+----+
                |   | 0 |    |    |
                | 1 |   |    |    |
                | 1 | 0 |  1 |  2 |
                | 1 | 1 |  1 |  2 |
                | 1 | 2 |  1 |  2 |
                | 1 | 3 |  1 |  3 |
                | 1 | 4 |    |    |
                | 2 | 3 |  2 | 10 |
                | 3 | 4 |    |    |
                +---+---+----+----+
                (9 rows)

                SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                LEFT ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t <= t1.t
                ON t1.k = t2.k;
                +---+---+----+----+
                | K | T | K0 | T0 |
                +---+---+----+----+
                |   | 0 |    |    |
                | 1 |   |    |    |
                | 1 | 0 |    |    |
                | 1 | 1 |    |    |
                | 1 | 2 |  1 |  2 |
                | 1 | 3 |  1 |  3 |
                | 1 | 4 |  1 |  3 |
                | 2 | 3 |  2 |  0 |
                | 3 | 4 |    |    |
                +---+---+----+----+
                (9 rows)""");
    }

    // This test should eventually be deleted once ASOF join supports more variants.
    @Test
    public void issue2549() {
        this.statementsFailingInCompilation("""
                CREATE VIEW V AS SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t < t1.t
                ON t1.k = t2.k;""",
                "Not yet implemented: Currently only LEFT ASOF joins are supported");
        this.statementsFailingInCompilation("""
                CREATE VIEW V AS SELECT *
                FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), (1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)
                LEFT ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)
                MATCH_CONDITION t2.t < t1.t
                ON t1.k = t2.k;""",
                "Not yet implemented: Currently the only MATCH_CONDITION comparison supported by ASOF joins is '>='");
    }
}
