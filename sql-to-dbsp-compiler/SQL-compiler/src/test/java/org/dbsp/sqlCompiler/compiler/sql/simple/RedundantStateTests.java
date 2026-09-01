package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Programs that should be optimized using KeyAnalysis.
 * The comment on each test names the redundancy and
 * the optimization expected to remove it. */
public class RedundantStateTests extends SqlIoTest {
    /** Every LEFT JOIN integrates its left input, so the payload columns
     * p0..p7 of the fact table are stored once per join; the intermediate rows
     * widen along the chain.  Expected: run the chain over the primary key and
     * the lookup keys only, then join the payload back once on the primary
     * key (late materialization). */
    @Test
    public void leftJoinChainCarriesPayload() {
        String sql = """
                CREATE TABLE fact(
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    k3 DATE NOT NULL,
                    ref1 VARCHAR,
                    ref2 VARCHAR,
                    ref3 VARCHAR,
                    ref4 VARCHAR,
                    p0 VARCHAR, p1 VARCHAR, p2 VARCHAR, p3 VARCHAR,
                    p4 VARCHAR, p5 VARCHAR, p6 VARCHAR, p7 VARCHAR,
                    PRIMARY KEY (k1, k2, k3));
                CREATE TABLE lookup(
                    k1 VARCHAR NOT NULL,
                    oid VARCHAR NOT NULL,
                    code VARCHAR,
                    descr VARCHAR,
                    PRIMARY KEY (k1, oid));
                CREATE VIEW result AS
                SELECT p.k1, p.k2, p.k3,
                       p.p0, p.p1, p.p2, p.p3, p.p4, p.p5, p.p6, p.p7,
                       s.code AS cd1,
                       d.code AS cd2, d.descr AS dsc2,
                       j.code AS cd3,
                       f.code AS cd4, f.descr AS dsc4
                FROM fact p
                LEFT JOIN lookup s ON s.k1 = p.k1 AND s.oid = p.ref1
                LEFT JOIN lookup d ON d.k1 = p.k1 AND d.oid = p.ref2
                LEFT JOIN lookup j ON j.k1 = p.k1 AND j.oid = p.ref3
                LEFT JOIN lookup f ON f.k1 = p.k1 AND f.oid = p.ref4;""";
        this.getCCS(sql);
    }

    /** Same chain, but the fact table has no PRIMARY KEY: its key
     * (k1, k2, k3) is only implied by the TOP-1 partition columns.
     * Each lookup is a pair, client-specific first and the 'DEFAULT' row as
     * fallback.  Expected: infer the key from the TOP-1, then late
     * materialization as above. */
    @Test
    public void leftJoinChainKeyFromTopK() {
        String sql = """
                CREATE TABLE raw_fact(
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    k3 DATE NOT NULL,
                    ref1 VARCHAR,
                    ref2 VARCHAR,
                    deleted INT,
                    ts TIMESTAMP,
                    p0 VARCHAR, p1 VARCHAR, p2 VARCHAR, p3 VARCHAR);
                CREATE TABLE raw_lookup(
                    k1 VARCHAR NOT NULL,
                    oid VARCHAR NOT NULL,
                    code VARCHAR,
                    descr VARCHAR,
                    version INT,
                    ts TIMESTAMP);
                CREATE LOCAL VIEW lookup AS
                SELECT k1, oid, code, descr FROM raw_lookup
                QUALIFY row_number() OVER (PARTITION BY k1, oid ORDER BY version DESC, ts DESC) = 1;
                CREATE VIEW result AS
                WITH fact AS (
                    SELECT k1, k2, k3, ref1, ref2, p0, p1, p2, p3
                    FROM raw_fact
                    WHERE deleted = 0
                    QUALIFY row_number() OVER (PARTITION BY k1, k2, k3 ORDER BY ts DESC) = 1)
                SELECT p.k1, p.k2, p.k3, p.p0, p.p1, p.p2, p.p3,
                       IFNULL(s1.code, s2.code) AS cd1,
                       IFNULL(d1.code, d2.code) AS cd2,
                       IFNULL(d1.descr, d2.descr) AS dsc2
                FROM fact p
                LEFT JOIN lookup s1 ON s1.oid = p.ref1 AND s1.k1 = p.k1
                LEFT JOIN lookup s2 ON s2.oid = p.ref1 AND s2.k1 = 'DEFAULT'
                LEFT JOIN lookup d1 ON d1.oid = p.ref2 AND d1.k1 = p.k1
                LEFT JOIN lookup d2 ON d2.oid = p.ref2 AND d2.k1 = 'DEFAULT';""";
        this.getCCS(sql);
    }

    /** The LEFT JOIN with dim_text only multiplies rows (one per locale) and
     * its two columns are dropped by the TOP-1 in the next view, so the join
     * and everything feeding it are dead.
     * Expected: prune the DISTINCT to the columns its duplicate-insensitive
     * consumer reads, then remove the join.  Results validated using postgres. */
    @Test
    public void deadLeftJoinUnderDistinctAndTopK() {
        String sql = """
                CREATE TABLE dimension(
                    k1 VARCHAR NOT NULL,
                    oid VARCHAR NOT NULL,
                    code VARCHAR,
                    active INT);
                CREATE TABLE dim_text(
                    k1 VARCHAR NOT NULL,
                    oid VARCHAR NOT NULL,
                    locale VARCHAR NOT NULL,
                    descr VARCHAR);
                CREATE LOCAL VIEW transform_dim AS
                SELECT DISTINCT bu.k1, bu.oid, bu.code, bu.active, vd.locale, vd.descr
                FROM dimension bu
                LEFT JOIN dim_text vd ON bu.k1 = vd.k1 AND bu.oid = vd.oid;
                CREATE VIEW dedupe_dim AS
                SELECT k1, oid, code
                FROM transform_dim
                WHERE code <> ' '
                QUALIFY row_number() OVER (PARTITION BY k1, oid ORDER BY code NULLS LAST) = 1;""";
        CompilerCircuitStream ccs = this.getCCS(sql).withStringTrim();
        // c1/o1: two locales duplicate each row; two codes, TOP-1 keeps the smaller; one exact duplicate.
        // c1/o2: blank and NULL codes are both dropped by the WHERE.
        // c2/o1: no dim_text row, kept by the LEFT JOIN.
        ccs.stepWeightOne("""
                        INSERT INTO dimension VALUES
                            ('c1', 'o1', 'B', 1),
                            ('c1', 'o1', 'A', 1),
                            ('c1', 'o1', 'A', 1),
                            ('c1', 'o2', ' ', 1),
                            ('c1', 'o2', NULL, 1),
                            ('c2', 'o1', 'C', 0);
                        INSERT INTO dim_text VALUES
                            ('c1', 'o1', 'l1', 'x1'),
                            ('c1', 'o1', 'l2', 'x2'),
                            ('c1', 'o2', 'l1', 'x3');""", """
                         k1 | oid | code
                        ------------------
                         c1 | o1  | A
                         c2      | o1  | C""");
    }

    /** Three-way inner join written large table first.  The plan joins the
     * large table with the first small table and stores that intermediate,
     * indexed for the second join, next to the large table's own index.
     * Joining the two small tables first stores no large intermediate.
     * Expected: state-aware join ordering, using expected_size. */
    @Test
    public void threeWayJoinStoresLargeIntermediate() {
        String sql = """
                CREATE TABLE raw_detail(
                    schema_id VARCHAR NOT NULL,
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    amount DECIMAL(22, 4)
                ) WITH ('expected_size' = '400000000');
                CREATE TABLE medium(
                    cid VARCHAR,
                    db VARCHAR,
                    sch VARCHAR,
                    app VARCHAR
                ) WITH ('expected_size' = '200000');
                CREATE TABLE small(
                    id VARCHAR NOT NULL,
                    sdb VARCHAR,
                    ssch VARCHAR,
                    app VARCHAR
                ) WITH ('expected_size' = '600');
                CREATE VIEW joined AS
                SELECT a.k1, a.k2, a.amount
                FROM raw_detail a,
                     (SELECT cid, db, sch FROM medium WHERE app = 'app1') b,
                     small c
                WHERE a.k1 = b.cid
                  AND b.db = c.sdb
                  AND b.sch = c.ssch
                  AND a.schema_id = c.id
                  AND c.app = 'app1';""";
        this.getCCS(sql);
    }

    /** The TOP-1 output is already indexed by its partition columns
     * (k1, k2); the LEFT JOIN on the same columns re-indexes it, storing
     * a second copy.  Expected: the join reads the TOP-1 output directly. */
    @Test
    public void topKOutputReindexedOnPartitionKey() {
        String sql = """
                CREATE TABLE fact(
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    k3 DATE NOT NULL,
                    paid VARCHAR,
                    PRIMARY KEY (k1, k2, k3));
                CREATE TABLE extra(
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    extra_code VARCHAR,
                    ts TIMESTAMP);
                CREATE VIEW result AS
                WITH additional1 AS (
                    SELECT k1, k2, extra_code,
                           row_number() OVER (PARTITION BY k1, k2
                                              ORDER BY extra_code NULLS LAST, ts DESC) AS rn
                    FROM extra)
                SELECT p.k1, p.k2, p.k3, p.paid, a.extra_code
                FROM fact p
                LEFT JOIN additional1 a
                       ON a.k1 = p.k1 AND a.k2 = p.k2 AND a.rn = 1;""";
        this.getCCS(sql);
    }

    /** lookup is indexed twice on (k1, oid): once with a NOT NULL key
     * for the join with fact, once with a nullable key for the join
     * with detail's columns, which are nullable after the LEFT JOIN.  The two
     * indexes hold the same rows.  Expected: cast or null-filter the probing
     * side so both joins share one index. */
    @Test
    public void sameIndexTwiceDifferentNullability() {
        String sql = """
                CREATE TABLE fact(
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    ref1 VARCHAR NOT NULL,
                    PRIMARY KEY (k1, k2));
                CREATE TABLE detail(
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    ref1 VARCHAR NOT NULL,
                    amount DECIMAL(22, 4),
                    PRIMARY KEY (k1, k2));
                CREATE TABLE lookup(
                    k1 VARCHAR NOT NULL,
                    oid VARCHAR NOT NULL,
                    code VARCHAR,
                    PRIMARY KEY (k1, oid));
                CREATE VIEW result AS
                SELECT p.k1, p.k2, c1.code AS rt1, c2.code AS rt2
                FROM fact p
                LEFT JOIN detail s ON s.k1 = p.k1 AND s.k2 = p.k2
                LEFT JOIN lookup c1 ON c1.k1 = p.k1 AND c1.oid = p.ref1
                LEFT JOIN lookup c2 ON c2.k1 = s.k1 AND c2.oid = s.ref1;""";
        this.getCCS(sql);
    }

    /** fact is indexed by (k1, k2) twice: with all its columns
     * for the LEFT JOIN with additional1, and with the two dates only for the
     * range join inside sal1.  Expected: one index, consumers project from it. */
    @Test
    public void sameInputSameKeyDifferentProjections() {
        String sql = """
                CREATE TABLE fact(
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    k3 DATE NOT NULL,
                    k3_end DATE,
                    p0 VARCHAR, p1 VARCHAR, p2 VARCHAR, p3 VARCHAR,
                    PRIMARY KEY (k1, k2, k3));
                CREATE TABLE extra(
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    extra_code VARCHAR,
                    ts TIMESTAMP);
                CREATE TABLE detail(
                    k1 VARCHAR NOT NULL,
                    k2 VARCHAR NOT NULL,
                    k3 DATE NOT NULL,
                    k3_end DATE,
                    amount DECIMAL(22, 4));
                CREATE VIEW result AS
                WITH additional1 AS (
                    SELECT k1, k2, extra_code,
                           row_number() OVER (PARTITION BY k1, k2
                                              ORDER BY extra_code NULLS LAST, ts DESC) AS rn
                    FROM extra),
                sal1 AS (
                    SELECT s.k1, s.k2, p.k3 AS emp_eff_date, s.amount,
                           row_number() OVER (PARTITION BY s.k1, s.k2, p.k3
                                              ORDER BY s.k3_end DESC, s.k3 DESC) AS rn
                    FROM detail s
                    JOIN fact p
                      ON s.k1 = p.k1 AND s.k2 = p.k2
                     AND s.k3 <= p.k3_end AND s.k3_end >= p.k3)
                SELECT p.k1, p.k2, p.k3, p.p0, p.p1, p.p2, p.p3,
                       a.extra_code, sal1.amount
                FROM fact p
                LEFT JOIN additional1 a
                       ON a.k1 = p.k1 AND a.k2 = p.k2 AND a.rn = 1
                LEFT JOIN sal1
                       ON sal1.k1 = p.k1 AND sal1.k2 = p.k2
                      AND sal1.emp_eff_date = p.k3 AND sal1.rn = 1;""";
        this.getCCS(sql);
    }
}
