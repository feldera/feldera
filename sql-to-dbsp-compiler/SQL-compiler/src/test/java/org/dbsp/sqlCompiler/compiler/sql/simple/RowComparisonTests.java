package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests for the rejection of equality comparisons between ROW values,
 * implemented by {@link org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RejectRowEquality}. */
public class RowComparisonTests extends SqlIoTest {
    static final String EQ_REJECTED = "ROW values cannot be compared using '='";
    static final String NE_REJECTED = "ROW values cannot be compared using '<>'";

    /** Two tables with a single column of the user-defined type 'point' */
    static final String UDT_TABLES = """
            CREATE TYPE point AS (x INT, y INT);
            CREATE TABLE L(p point);
            CREATE TABLE R(p point);
            """;

    /** One table with two columns of an anonymous ROW type with a nullable field */
    static final String ROW_TABLE = """
            CREATE TABLE T(r ROW(a INT, b INT NULL), s ROW(a INT, b INT NULL));
            """;

    @Test
    public void rejectEqualsOnRow() {
        this.statementsFailingInCompilation(ROW_TABLE +
                "CREATE VIEW V AS SELECT r = s FROM T;", EQ_REJECTED);
        this.statementsFailingInCompilation(ROW_TABLE +
                "CREATE VIEW V AS SELECT r <> s FROM T;", NE_REJECTED);
        // '!=' is a synonym of '<>'
        this.statementsFailingInCompilation(ROW_TABLE +
                "CREATE VIEW V AS SELECT r != s FROM T;", NE_REJECTED);
        // A user-defined structure type is a ROW type
        this.statementsFailingInCompilation(UDT_TABLES +
                "CREATE VIEW V AS SELECT L.p = R.p FROM L, R;", EQ_REJECTED);
        this.statementsFailingInCompilation("""
                CREATE TABLE T(r ROW(a INT, b ROW(c INT)), s ROW(a INT, b ROW(c INT)));
                CREATE VIEW V AS SELECT r.b = s.b FROM T;""", EQ_REJECTED);
    }

    @Test
    public void rejectEqualityInJoinCondition() {
        this.statementsFailingInCompilation(UDT_TABLES +
                "CREATE VIEW V AS SELECT * FROM L JOIN R ON L.p = R.p;", EQ_REJECTED);
        this.statementsFailingInCompilation(UDT_TABLES +
                "CREATE VIEW V AS SELECT * FROM L NATURAL JOIN R;", EQ_REJECTED);
        this.statementsFailingInCompilation(UDT_TABLES +
                "CREATE VIEW V AS SELECT * FROM L JOIN R USING (p);", EQ_REJECTED);
    }

    /** IN over a list of ROW values expands into a disjunction of equalities */
    @Test
    public void rejectEqualityInInList() {
        this.statementsFailingInCompilation(ROW_TABLE +
                "CREATE VIEW V AS SELECT r IN (ROW(1, 2), ROW(3, 4)) FROM T;", EQ_REJECTED);
    }

    /** 'CASE r WHEN v' expands into 'r = v' */
    @Test
    public void rejectEqualityInCase() {
        this.statementsFailingInCompilation("""
                CREATE TYPE point AS (x INT, y INT);
                CREATE TABLE T(p point);
                CREATE VIEW V AS SELECT CASE p WHEN point(1, 2) THEN 1 ELSE 0 END FROM T;""",
                EQ_REJECTED);
    }

    /** NULLIF returns NULL when its arguments are equal */
    @Test
    public void rejectNullifOnRow() {
        this.statementsFailingInCompilation("""
                CREATE TYPE point AS (x INT, y INT);
                CREATE TABLE T(p point);
                CREATE VIEW V AS SELECT NULLIF(p, point(1, 2)) FROM T;""",
                "'NULLIF' compares ROW values for equality");
    }

    /** An equality in a correlated subquery is rejected as well */
    @Test
    public void rejectEqualityInCorrelatedSubquery() {
        this.statementsFailingInCompilation(UDT_TABLES +
                "CREATE VIEW V AS SELECT * FROM L WHERE EXISTS (SELECT * FROM R WHERE R.p = L.p);",
                EQ_REJECTED);
    }

    /** Comparing a ROW value against a ROW constructor is a ROW comparison */
    @Test
    public void rejectEqualsAgainstRowConstructor() {
        this.statementsFailingInCompilation(ROW_TABLE +
                "CREATE VIEW V AS SELECT r = ROW(1, 2) FROM T;", EQ_REJECTED);
    }

    /** Comparing two ROW constructors is rejected too. */
    @Test
    public void rejectComparisonBetweenRowConstructors() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T(a INT, b INT);
                CREATE VIEW V AS SELECT (a, b) = (b, a) FROM T;""", EQ_REJECTED);
        this.statementsFailingInCompilation("""
                CREATE TABLE T(a INT, b INT);
                CREATE VIEW V AS SELECT (a, b) <> (b, a) FROM T;""", NE_REJECTED);
    }

    @Test
    public void acceptIsNotDistinctFromOnRow() {
        CompilerCircuitStream ccs = this.getCCS(ROW_TABLE +
                """
                CREATE VIEW V AS SELECT r IS NOT DISTINCT FROM s AS nd,
                                        r IS DISTINCT FROM s AS d FROM T;""");
        ccs.step("INSERT INTO T VALUES(ROW(ROW(1, NULL), ROW(1, NULL)))", """
                 nd   | d     | weight
                -----------------------
                 true | false | 1""");
    }

    /** IS [NOT] DISTINCT FROM and '<=>' on a user-defined type, which is a ROW type.
     * NULL fields compare as equal, and so do two NULL values. */
    @Test
    public void distinctFromOnUserDefinedType() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TYPE point AS (x INT, y INT);
                CREATE TABLE T(id INT, p point, q point);
                CREATE VIEW V AS SELECT id,
                                        p IS NOT DISTINCT FROM q AS nd,
                                        p IS DISTINCT FROM q AS d,
                                        p <=> q AS eq
                                 FROM T;""");
        ccs.step("""
                INSERT INTO T VALUES(0, point(1, NULL), point(1, NULL)),
                                    (1, point(1, 2), point(1, 3)),
                                    (2, NULL, NULL),
                                    (3, NULL, point(1, 2));""", """
                 id | nd    | d     | eq    | weight
                --------------------------------------
                 0  | true  | false | true  | 1
                 1  | false | true  | false | 1
                 2  | true  | false | true  | 1
                 3  | false | true  | false | 1""");
    }

    /** '<=>' is a synonym of IS NOT DISTINCT FROM, so it is accepted */
    @Test
    public void acceptNullSafeEqualOnRow() {
        CompilerCircuitStream ccs = this.getCCS(ROW_TABLE +
                "CREATE VIEW V AS SELECT r <=> s AS eq FROM T;");
        ccs.step("INSERT INTO T VALUES(ROW(ROW(1, NULL), ROW(1, NULL)))", """
                 eq   | weight
                ---------------
                 true | 1""");
    }

    /** Ordering comparisons on ROW values remain legal */
    @Test
    public void acceptOrderingComparisonsOnRow() {
        CompilerCircuitStream ccs = this.getCCS(ROW_TABLE +
                "CREATE VIEW V AS SELECT r < s AS lt, r >= s AS ge FROM T;");
        ccs.step("INSERT INTO T VALUES(ROW(ROW(1, 2), ROW(1, 3)))", """
                 lt   | ge    | weight
                -----------------------
                 true | false | 1""");
    }

    /** Every rewrite that the error messages and the documentation suggest must compile */
    @Test
    public void acceptSuggestedRewrites() {
        this.getCC(UDT_TABLES + """
                CREATE VIEW V1 AS SELECT p IS NOT DISTINCT FROM point(1, 2)
                                      OR p IS NOT DISTINCT FROM point(3, 4) FROM L;
                CREATE VIEW V2 AS SELECT CASE WHEN p IS NOT DISTINCT FROM point(1, 2)
                                              THEN 1 ELSE 0 END FROM L;
                CREATE VIEW V3 AS SELECT CASE WHEN p IS NOT DISTINCT FROM point(1, 2)
                                              THEN NULL ELSE p END FROM L;""");
        this.getCC("""
                CREATE TABLE T(a INT, b INT);
                CREATE VIEW V AS SELECT (a, b) IS DISTINCT FROM (b, a) FROM T;""");
    }

    /** A join key of ROW type joins on IS NOT DISTINCT FROM: two NULL fields match,
     * and so do two NULL ROW values */
    @Test
    public void joinOnRowValues() {
        CompilerCircuitStream ccs = this.getCCS(UDT_TABLES +
                "CREATE VIEW V AS SELECT L.p.x AS x FROM L JOIN R ON L.p IS NOT DISTINCT FROM R.p;");
        // point(1, NULL) matches on a NULL field, and the NULL row matches the NULL row
        ccs.step("""
                INSERT INTO L VALUES(point(1, NULL)), (point(2, 2)), (point(3, 3));
                INSERT INTO L VALUES(NULL);
                INSERT INTO R VALUES(point(1, NULL)), (point(2, 9));
                INSERT INTO R VALUES(NULL);""", """
                 x | weight
                ------------
                 1 | 1
                   | 1""");
    }

    /** The same join on an anonymous ROW column.  Calcite wraps each operand of the
     * comparison in a cast to the record type, unlike a user-defined type. */
    @Test
    public void joinOnAnonymousRowValues() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TABLE L(r ROW(a INT, b INT NULL));
                CREATE TABLE R(r ROW(a INT, b INT NULL));
                CREATE VIEW V AS SELECT L.r.a AS a FROM L JOIN R ON L.r IS NOT DISTINCT FROM R.r;""");
        ccs.step("""
                INSERT INTO L VALUES(ROW(ROW(1, NULL))), (ROW(ROW(2, 2)));
                INSERT INTO L VALUES(NULL);
                INSERT INTO R VALUES(ROW(ROW(1, NULL))), (ROW(ROW(2, 9)));
                INSERT INTO R VALUES(NULL);""", """
                 a | weight
                ------------
                 1 | 1
                   | 1""");
    }

    /** A correlated EXISTS over ROW values decorrelates into a join on a ROW key */
    @Test
    public void existsOnRowValues() {
        this.getCC(UDT_TABLES +
                """
                CREATE VIEW V AS SELECT * FROM L WHERE EXISTS (
                    SELECT 1 FROM R WHERE R.p IS NOT DISTINCT FROM L.p);""");
    }

    /** Comparing the fields one by one stays legal too */
    @Test
    public void acceptFieldWiseJoin() {
        this.getCC(UDT_TABLES +
                """
                CREATE VIEW V AS SELECT * FROM L JOIN R
                ON L.p.x IS NOT DISTINCT FROM R.p.x AND L.p.y IS NOT DISTINCT FROM R.p.y;""");
    }

    /** Grouping constructs compare ROW values for distinctness, not equality */
    @Test
    public void acceptGroupingConstructsOnRow() {
        this.getCC(UDT_TABLES + """
                CREATE VIEW V1 AS SELECT DISTINCT p FROM L;
                CREATE VIEW V2 AS SELECT p, COUNT(*) FROM L GROUP BY p;
                CREATE VIEW V3 AS SELECT p FROM L UNION SELECT p FROM R;
                CREATE VIEW V4 AS SELECT p FROM L INTERSECT SELECT p FROM R;
                CREATE VIEW V5 AS SELECT p FROM L EXCEPT SELECT p FROM R;
                CREATE VIEW V6 AS SELECT p, COUNT(*) OVER (PARTITION BY p) FROM L;""");
    }

    /** For ARRAY values '=' means IS NOT DISTINCT FROM: two NULL elements are equal */
    @Test
    public void arrayEqualityIgnoresNulls() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TABLE T(a INT ARRAY, b INT ARRAY);
                CREATE VIEW V AS SELECT a = b AS eq, a IS NOT DISTINCT FROM b AS nd FROM T;""");
        ccs.step("INSERT INTO T VALUES(ARRAY[1, NULL], ARRAY[1, NULL])", """
                 eq   | nd   | weight
                ----------------------
                 true | true | 1""");
    }

    /** For MAP values '=' means IS NOT DISTINCT FROM: two NULL values are equal */
    @Test
    public void mapEqualityIgnoresNulls() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TABLE T(a MAP<VARCHAR, INT>, b MAP<VARCHAR, INT>);
                CREATE VIEW V AS SELECT a = b AS eq, a IS NOT DISTINCT FROM b AS nd FROM T;""");
        ccs.step("INSERT INTO T VALUES(MAP['x', CAST(NULL AS INT)], MAP['x', CAST(NULL AS INT)])",
                """
                 eq   | nd   | weight
                ----------------------
                 true | true | 1""");
    }

    /** The equivalence between '=' and IS NOT DISTINCT FROM covers the elements of an
     * array, not the array itself: a NULL array still makes '=' produce NULL */
    @Test
    public void nullArrayComparison() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TABLE T(a INT ARRAY, b INT ARRAY);
                CREATE VIEW V AS SELECT a = b AS eq, a IS NOT DISTINCT FROM b AS nd FROM T;""");
        ccs.step("INSERT INTO T VALUES(NULL, ARRAY[1])", """
                 eq | nd    | weight
                ---------------------
                    | false | 1""");
    }

    /** Map entries are compared by increasing key, each key before its value.  A map
     * has no other order, so the order a literal writes the entries in does not matter. */
    @Test
    public void mapComparisonOrder() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TABLE T(id INT, m MAP<VARCHAR, INT>, n MAP<VARCHAR, INT>);
                CREATE VIEW V AS SELECT id, m = n AS eq, m < n AS lt FROM T;""");
        ccs.step("""
                INSERT INTO T VALUES
                    (0, MAP['b', 1, 'a', 2], MAP['a', 2, 'b', 1]),
                    (1, MAP['a', 1], MAP['b', 1]),
                    (2, MAP['a', 1], MAP['a', 2]),
                    (3, MAP['a', 1], MAP['a', 1, 'b', 1]);""", """
                 id | eq    | lt    | weight
                ------------------------------
                 0  | true  | false | 1
                 1  | false | true  | 1
                 2  | false | true  | 1
                 3  | false | true  | 1""");
    }

    /** NULLS FIRST always holds inside an ARRAY value */
    @Test
    public void arrayElementsSortNullsFirst() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TABLE T(a INT ARRAY, b INT ARRAY);
                CREATE VIEW V AS SELECT a < b AS lt FROM T;""");
        ccs.step("INSERT INTO T VALUES(ARRAY[NULL], ARRAY[1])", """
                 lt   | weight
                ---------------
                 true | 1""");
    }

    /** An ARRAY whose elements are ROW values can still be compared with '=' */
    @Test
    public void acceptEqualityOnArrayOfRows() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TABLE T(a ROW(x INT) ARRAY, b ROW(x INT) ARRAY);
                CREATE VIEW V AS SELECT a = b AS eq FROM T;""");
        ccs.step("INSERT INTO T VALUES(ARRAY[ROW(1)], ARRAY[ROW(1)])", """
                 eq   | weight
                ---------------
                 true | 1""");
    }
}
