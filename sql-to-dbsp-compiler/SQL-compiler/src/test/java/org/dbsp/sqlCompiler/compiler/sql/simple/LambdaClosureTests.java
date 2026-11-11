package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Lambda expressions that refer to enclosing scopes, and the rules for lambda bodies */
public class LambdaClosureTests extends SqlIoTest {
    static final String TABLES = """
            CREATE TABLE T(a INT ARRAY, x INT);
            CREATE TABLE S(y INT, b INT ARRAY);
            """;

    static final String DATA = """
            INSERT INTO T VALUES(ARRAY[1, 2, 3], 10), (ARRAY[1, 2, 3], 2);
            INSERT INTO S VALUES(2, ARRAY[1, 5]), (5, ARRAY[1, 2]);""";

    @Test
    public void testArrayExists() {
        // Validated on DuckDB
        this.qst("""
                SELECT ARRAY_EXISTS(ARRAY[1, -12, 3], x -> x > 0);
                result
                ------
                 true
                (1 row)""");
    }

    @Test
    public void testProject() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + "CREATE VIEW V AS SELECT TRANSFORM(T.a, e -> e + T.x) FROM T;");
        ccs.stepWeightOne(DATA, """
                 r
                ---
                 { 11, 12, 13 }
                 { 3, 4, 5 }""");
    }

    @Test
    public void testFilter() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + "CREATE VIEW V AS SELECT x FROM T WHERE ARRAY_EXISTS(a, e -> e = x);");
        ccs.stepWeightOne(DATA, """
                 x
                ---
                 2""");
    }

    @Test
    public void testJoinCondition() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS SELECT T.x, S.y FROM T JOIN S ON ARRAY_EXISTS(T.a, e -> e = S.y);""");
        ccs.stepWeightOne(DATA, """
                 x  | y
                --------
                 10 | 2
                 2  | 2""");
    }

    @Test
    public void testLeftJoinCondition() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS SELECT T.x, S.y FROM T LEFT JOIN S ON ARRAY_EXISTS(T.a, e -> e + T.x = S.y);""");
        ccs.stepWeightOne(DATA, """
                 x  | y
                --------
                 10 |NULL
                 2  | 5""");
    }

    @Test
    public void testCorrelatedSubquery() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS
                SELECT T.x, (SELECT COUNT(*) FROM S WHERE ARRAY_EXISTS(S.b, e -> e = T.x)) FROM T;""");
        ccs.stepWeightOne(DATA, """
                 x  | c
                --------
                 10 | 0
                 2  | 1""");
    }

    @Test
    public void testCorrelatedSubquerySameType() {
        // Validated on DuckDB
        // T.x and S.y have the same index and type
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT, a INT ARRAY);
                CREATE TABLE S(y INT, b INT ARRAY);
                CREATE VIEW V AS SELECT x, (SELECT COUNT(*) FROM S WHERE ARRAY_EXISTS(S.b, e -> e = T.x)) AS c
                FROM T;""");
        ccs.stepWeightOne("""
                INSERT INTO T VALUES(1, ARRAY()), (7, ARRAY());
                INSERT INTO S VALUES(5, ARRAY[1, 5]), (6, ARRAY[1]);""", """
                 x | c
                -------
                 1 | 2
                 7 | 0""");
    }

    @Test
    public void testParameterHidesColumn() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + "CREATE VIEW V AS SELECT TRANSFORM(a, x -> x * T.x) AS r FROM T;");
        ccs.stepWeightOne(DATA, """
                 r
                ---
                 { 10, 20, 30 }
                 { 2, 4, 6 }""");
    }

    @Test
    public void testFilterPushedIntoJoinRightInput() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS SELECT T.x, S.y FROM T JOIN S ON T.x = S.y
                WHERE ARRAY_EXISTS(S.b, e -> e > S.y);""");
        ccs.stepWeightOne(DATA, """
                 x | y
                -------
                 2 | 2""");
    }

    @Test
    public void testProjectMerge() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS SELECT TRANSFORM(a, e -> e + z) AS r FROM (SELECT x + 1 AS z, a FROM T);""");
        ccs.stepWeightOne(DATA, """
                 r
                ---
                 { 12, 13, 14 }
                 { 4, 5, 6 }""");
    }

    @Test
    public void testNested() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS SELECT TRANSFORM(a, e -> ARRAY_EXISTS(a, f -> f = e + x)) AS r FROM T;""");
        ccs.stepWeightOne(DATA, """
                 r
                ---
                 { false, false, false }
                 { true, false, false }""");
    }

    @Test
    public void testNestedReadsOuterParameter() {
        // Validated on DuckDB
        this.qst("""
                SELECT transform(array[array[1, 2], array[3]], a -> transform(a, b -> b + cardinality(a)));
                result
                ------
                 { { 3, 4}, { 4} }
                (1 row)""");
    }

    @Test
    public void testUnusedFields() {
        // Validated on DuckDB
        var ccs = this.getCCS("""
                CREATE TABLE U(a INT ARRAY, p INT, q INT, r INT);
                CREATE LOCAL VIEW W AS SELECT a, p, q FROM U WHERE p > 0;
                CREATE VIEW V AS SELECT TRANSFORM(a, e -> e * q) AS r FROM W;""");
        ccs.stepWeightOne("INSERT INTO U VALUES(ARRAY[1, 2], 1, 3, 4);", """
                 r
                ---
                 { 3, 6 }""");
    }

    @Test
    public void testProjectMergeNestsSameName() {
        // Validated on DuckDB
        // Merging the projections puts the inner lambda in the body of the outer lambda;
        // both lambdas have a parameter named e
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS SELECT TRANSFORM(a, e -> e + CARDINALITY(z)) AS r
                FROM (SELECT ARRAY_FILTER(a, e -> e > x) AS z, a FROM T);""");
        ccs.stepWeightOne(DATA, """
                 r
                ---
                 { 1, 2, 3 }
                 { 2, 3, 4 }""");
    }

    @Test
    public void testAggregate() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS SELECT SUM(CARDINALITY(ARRAY_FILTER(a, e -> e > x))) AS s FROM T;""");
        ccs.stepWeightOne(DATA, """
                 s
                ---
                 1""");
    }

    @Test
    public void testAggregateArgument() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS SELECT SORT_ARRAY(TRANSFORM(ARRAY_AGG(x), e -> e * 2)) AS r FROM T;""");
        ccs.stepWeightOne(DATA, """
                 r
                ---
                 { 4, 20 }""");
    }

    @Test
    public void testOrderByReadsColumnThroughArgument() {
        // Validated on DuckDB
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW V AS SELECT x FROM T ORDER BY ARRAY_EXISTS(T.a, e -> e = 2) LIMIT 1;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(ARRAY[2], 1), (ARRAY[3], 2);", """
                 x
                ---
                 2""");
    }

    @Test
    public void testBodyRestrictions() {
        // DuckDB rejects this query too
        this.statementsFailingInCompilation(TABLES + """
                CREATE VIEW V AS SELECT TRANSFORM(a, e -> e + (SELECT MAX(y) FROM S)) FROM T;""",
                "Sub-query is illegal in lambda expression");
        // DuckDB accepts the following queries
        this.statementsFailingInCompilation(TABLES + """
                CREATE VIEW V AS SELECT ARRAY_EXISTS(ARRAY[1, 2], e -> e = SUM(x)) FROM T;""",
                "Aggregate expression is illegal in lambda expression");
        this.statementsFailingInCompilation(TABLES + """
                CREATE VIEW V AS SELECT x, ARRAY_EXISTS(ARRAY[0], e -> e = GROUPING(x))
                FROM T GROUP BY ROLLUP(x);""",
                "Aggregate expression is illegal in lambda expression");
        this.statementsFailingInCompilation(TABLES + """
                CREATE VIEW V AS SELECT ARRAY_EXISTS(ARRAY[1], e -> e = SUM(x) OVER (ORDER BY x)) FROM T;""",
                "Windowed aggregate expression is illegal in lambda expression");
        this.statementsFailingInCompilation(TABLES + """
                CREATE VIEW V AS SELECT x FROM T ORDER BY ARRAY_EXISTS(a, e -> e = x) LIMIT 1;""",
                "Lambda expression in ORDER BY clause can read only lambda parameters");
    }
}
