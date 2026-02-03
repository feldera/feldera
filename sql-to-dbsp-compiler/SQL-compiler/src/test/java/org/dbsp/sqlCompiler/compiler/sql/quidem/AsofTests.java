package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.junit.Assert;
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

    @Test
    public void issue3435() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT, y INT, z INT);
                CREATE TABLE S(x INT, y INT, v INT);
                
                CREATE VIEW V AS
                SELECT
                    T.y, T.z
                FROM
                    T LEFT ASOF JOIN S
                    MATCH_CONDITION ( T.x >= S.x )
                    ON T.y = S.y;""");
        Assert.assertTrue(ccs.compiler.hasWarnings());
        Assert.assertEquals("Unused column",
                ccs.compiler.messages.getMessage(0).errorType);
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
                "Not yet implemented: Currently the only MATCH_CONDITION comparison supported by ASOF joins is 'leftCol >= rightCol'");
    }

    @Test
    public void cloudIssue994() {
        this.getCCS("""
                CREATE TABLE T(x BIGINT, y INT, z INT);
                CREATE TABLE S(x TIMESTAMP, y INT, v INT);
                
                CREATE VIEW V AS
                SELECT
                    T.y, T.z
                FROM
                    T LEFT ASOF JOIN S
                    MATCH_CONDITION ( CAST(T.x AS TIMESTAMP) >= S.x )
                    ON T.y = S.y;""");
    }

    @Test
    public void issue5180() {
        this.statementsFailingInCompilation("""
                CREATE TABLE asof_tbl1(
                id INT, intt INT NULL);
                
                CREATE TABLE asof_tbl2(
                id INT, intt INT);
                
                CREATE MATERIALIZED VIEW v AS
                SELECT
                    t1.id,
                    t2.intt AS t2_int
                FROM asof_tbl1 AS t1
                LEFT ASOF JOIN asof_tbl2 AS t2
                    MATCH_CONDITION (t2.intt = (SELECT t2.intt))
                    ON t1.id = t2.id;""",
                "ASOF JOIN MATCH_CONDITION must be a comparison between columns from the two inputs");
    }

    @Test
    public void asofWithTest() {
        this.statementsFailingInCompilation("""
                CREATE VIEW V AS WITH
                   T2(id, intt) AS (VALUES(1, 0)),
                   T1(id, intt) as (VALUES(1, 0)),
                   T3(id) AS (VALUES(1))
                SELECT t1.id, t2.intt
                FROM T1 LEFT ASOF JOIN T2
                    MATCH_CONDITION t2.intt < t1.intt
                    ON t1.id = t2.id""", "7|    MATCH_CONDITION t2.intt < t1.intt");
    }

    @Test
    public void issue5259() {
        this.statementsFailingInCompilation("""
                CREATE TABLE asof_tbl1(id INT, intt INT NULL);
                CREATE TABLE asof_tbl2(id INT, intt INT);
                
                CREATE MATERIALIZED VIEW v AS
                WITH combined AS (SELECT
                t1.id AS t1_id, t1.intt AS t1_intt
                FROM asof_tbl1 t1)
                SELECT
                    c.t1_id, c.t1_intt
                FROM combined c
                LEFT ASOF JOIN asof_tbl2 t2
                    MATCH_CONDITION (t2.intt >= c.t1_intt)
                    ON c.t1_id = t2.id;""",
                "Not yet implemented: Currently the only MATCH_CONDITION comparison supported by ASOF joins is 'leftCol >= rightCol'");
    }
}
