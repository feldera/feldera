package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.WarnFloatingPointEquality;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for {@link WarnFloatingPointEquality} */
public class FloatingPointEqualityTests extends BaseSQLTests {
    /** Two tables; the views under test start on line 3 */
    static final String TABLES = """
            CREATE TABLE T(x DOUBLE, y DOUBLE, i INT, d DECIMAL(10, 2), r REAL, f REAL, arr DOUBLE ARRAY, rw ROW(a DOUBLE, b INT), m MAP<VARCHAR, DOUBLE>, md MAP<DOUBLE, INT>);
            CREATE TABLE S(x DOUBLE, z INT);
            """;

    static final String DOUBLE = "compares floating point values of type DOUBLE for equality.";

    List<CompilerMessages.Message> compileAndGetFPWarnings(String program) {
        var cc = this.getCC(TABLES + program);
        List<CompilerMessages.Message> result = new ArrayList<>();
        for (CompilerMessages.Message message : cc.compiler.messages.messages)
            if (message.errorType.equals(WarnFloatingPointEquality.WARNING))
                result.add(message);
        return result;
    }

    /** The floating point equality warnings for the program as the compiler prints them on stderr */
    String outputFPWarnings(String program) {
        StringBuilder result = new StringBuilder();
        for (CompilerMessages.Message message : this.compileAndGetFPWarnings(program))
            result.append(message);
        return result.toString().stripTrailing();
    }

    /** The content of each floating point equality warning */
    List<String> warnings(String program) {
        List<String> result = new ArrayList<>();
        for (CompilerMessages.Message message : this.compileAndGetFPWarnings(program))
            for (String line : message.toString().split("\n"))
                if (line.contains("warning: "))
                    result.add(line.replace("(no input file):", ""));
        return result;
    }

    void assertWarnings(String program, String... expected) {
        Assert.assertEquals(List.of(expected), this.warnings(program));
    }

    void assertNoWarnings(String program) {
        this.assertWarnings(program);
    }

    @Test
    public void equalsRendered() {
        Assert.assertEquals("""
                While compiling:
                    2|CREATE TABLE S(x DOUBLE, z INT);
                    3|CREATE VIEW V AS SELECT * FROM T WHERE x = y;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                (no input file): Floating point equality
                (no input file):3:40: warning: Floating point equality: '=' compares floating point values of type DOUBLE for equality.
                See https://docs.feldera.com/sql/comparisons#comparing-floating-point-values
                    2|CREATE TABLE S(x DOUBLE, z INT);
                    3|CREATE VIEW V AS SELECT * FROM T WHERE x = y;
                                                             ^^^^^""",
                this.outputFPWarnings("CREATE VIEW V AS SELECT * FROM T WHERE x = y;"));
    }

    @Test
    public void equalityOperators() {
        // '!=' is a synonym of '<>' and '<=>' of 'IS NOT DISTINCT FROM'
        this.assertWarnings(
                "CREATE VIEW V AS SELECT x <> y, x != y, x IS DISTINCT FROM y, x IS NOT DISTINCT FROM y, x <=> y FROM T;",
                "3:25: warning: Floating point equality: '<>' " + DOUBLE,
                "3:33: warning: Floating point equality: '<>' " + DOUBLE,
                "3:41: warning: Floating point equality: 'IS DISTINCT FROM' " + DOUBLE,
                "3:63: warning: Floating point equality: 'IS NOT DISTINCT FROM' " + DOUBLE,
                "3:89: warning: Floating point equality: '<=>' " + DOUBLE);
    }

    /** A comparison is floating point when either operand is; ordering comparisons should produce no warnings */
    @Test
    public void mixedOperands() {
        this.assertWarnings(
                "CREATE VIEW V AS SELECT x = 1.5, i = 1.5e0, d = 1.5, x < y, i = 1, r = f FROM T;",
                "3:25: warning: Floating point equality: '=' " + DOUBLE,
                "3:34: warning: Floating point equality: '=' " + DOUBLE,
                "3:68: warning: Floating point equality: '=' compares floating point values of type REAL for equality.");
    }

    @Test
    public void exactTypesAreSilent() {
        this.assertNoWarnings("""
                CREATE VIEW V AS SELECT i, d, COUNT(DISTINCT d) FROM T
                WHERE d = 1.5 AND i IN (1, 2) AND x < y GROUP BY i, d;""");
    }

    @Test
    public void groupBy() {
        this.assertWarnings("CREATE VIEW V AS SELECT x, COUNT(*) FROM T GROUP BY x;",
                "3:53: warning: Floating point equality: 'GROUP BY' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT x, i, COUNT(*) FROM T GROUP BY ROLLUP(x, i);",
                "3:63: warning: Floating point equality: 'GROUP BY' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT x, i, COUNT(*) FROM T GROUP BY GROUPING SETS ((x), (i));",
                "3:71: warning: Floating point equality: 'GROUP BY' " + DOUBLE);
        this.assertNoWarnings("CREATE VIEW V AS SELECT i, COUNT(*) FROM T GROUP BY i;");
    }

    /** The warning points at the DISTINCT keyword and names the columns */
    @Test
    public void distinctEmitted() {
        Assert.assertEquals("""
                While compiling:
                    2|CREATE TABLE S(x DOUBLE, z INT);
                    3|CREATE VIEW V AS SELECT DISTINCT x FROM T;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                (no input file): Floating point equality
                (no input file):3:25: warning: Floating point equality: 'DISTINCT' compares the floating point values in column 'x' for equality.
                See https://docs.feldera.com/sql/comparisons#comparing-floating-point-values
                    2|CREATE TABLE S(x DOUBLE, z INT);
                    3|CREATE VIEW V AS SELECT DISTINCT x FROM T;
                                              ^^^^^^^^""",
                this.outputFPWarnings("CREATE VIEW V AS SELECT DISTINCT x FROM T;"));
    }

    @Test
    public void distinct() {
        this.assertWarnings("CREATE VIEW V AS SELECT DISTINCT x, r, i FROM T;",
                "3:25: warning: Floating point equality: 'DISTINCT' compares the floating point values in columns 'x' and 'r' for equality.");
        this.assertWarnings("CREATE VIEW V AS SELECT DISTINCT * FROM T;",
                "3:25: warning: Floating point equality: 'DISTINCT' compares the floating point values in columns 'x', 'y', 'r', 'f', 'arr', 'rw', 'm', and 'md' for equality.");
        this.assertWarnings("CREATE VIEW V AS WITH W AS (SELECT DISTINCT x FROM T) SELECT * FROM W;",
                "3:36: warning: Floating point equality: 'DISTINCT' compares the floating point values in column 'x' for equality.");
        this.assertNoWarnings("CREATE VIEW V AS SELECT DISTINCT i FROM T;");
    }

    @Test
    public void aggregates() {
        this.assertWarnings("CREATE VIEW V AS SELECT COUNT(DISTINCT x) FROM T;",
                "3:25: warning: Floating point equality: 'COUNT(DISTINCT)' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT MODE(x) FROM T;",
                "3:25: warning: Floating point equality: 'MODE' " + DOUBLE);
        this.assertNoWarnings("CREATE VIEW V AS SELECT SUM(x), MAX(x), COUNT(x) FROM T;");
    }

    @Test
    public void setOperations() {
        this.assertWarnings("CREATE VIEW V AS SELECT x FROM T UNION SELECT x FROM S;",
                "3:18: warning: Floating point equality: 'UNION' compares the floating point values in column 'x' for equality.");
        this.assertWarnings("CREATE VIEW V AS SELECT x FROM T INTERSECT SELECT x FROM S;",
                "3:18: warning: Floating point equality: 'INTERSECT' compares the floating point values in column 'x' for equality.");
        this.assertWarnings("CREATE VIEW V AS SELECT x FROM T EXCEPT ALL SELECT x FROM S;",
                "3:18: warning: Floating point equality: 'EXCEPT ALL' compares the floating point values in column 'x' for equality.");
        this.assertNoWarnings("CREATE VIEW V AS SELECT x FROM T UNION ALL SELECT x FROM S;");
    }

    @Test
    public void joins() {
        this.assertWarnings("CREATE VIEW V AS SELECT * FROM T JOIN S ON T.x = S.x;",
                "3:44: warning: Floating point equality: '=' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT * FROM T JOIN S USING (x);",
                "3:41: warning: Floating point equality: 'USING' compares the floating point values in column 'x' for equality.");
        this.assertNoWarnings("CREATE VIEW V AS SELECT * FROM T JOIN S ON T.i = S.z;");
    }

    /** The shared column is coerced to DOUBLE whichever side declares it exact */
    @Test
    public void joinsCoerceSharedColumns() {
        String tables = """
                CREATE TABLE U(i INT, w DOUBLE);
                CREATE TABLE V2(i DOUBLE, q INT);
                """;
        this.assertWarnings(tables + "CREATE VIEW W AS SELECT * FROM U NATURAL JOIN V2;",
                "5:42: warning: Floating point equality: 'NATURAL JOIN' compares the floating point values in column 'i' for equality.");
        this.assertWarnings(tables + "CREATE VIEW W AS SELECT * FROM U JOIN V2 USING (i);",
                "5:42: warning: Floating point equality: 'USING' compares the floating point values in column 'i' for equality.");
        this.assertWarnings(tables + "CREATE VIEW W AS SELECT * FROM V2 NATURAL JOIN U;",
                "5:43: warning: Floating point equality: 'NATURAL JOIN' compares the floating point values in column 'i' for equality.");
    }

    @Test
    public void naturalJoinEmitted() {
        Assert.assertEquals("""
                While compiling:
                    2|CREATE TABLE S(x DOUBLE, z INT);
                    3|CREATE VIEW V AS SELECT * FROM T NATURAL JOIN S;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                (no input file): Floating point equality
                (no input file):3:42: warning: Floating point equality: 'NATURAL JOIN' compares the floating point values in column 'x' for equality.
                See https://docs.feldera.com/sql/comparisons#comparing-floating-point-values
                    2|CREATE TABLE S(x DOUBLE, z INT);
                    3|CREATE VIEW V AS SELECT * FROM T NATURAL JOIN S;
                                                               ^^^^""",
                this.outputFPWarnings("CREATE VIEW V AS SELECT * FROM T NATURAL JOIN S;"));
    }

    @Test
    public void windows() {
        this.assertWarnings("CREATE VIEW V AS SELECT x, RANK() OVER (PARTITION BY x ORDER BY i) FROM T;",
                "3:54: warning: Floating point equality: 'PARTITION BY' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT x, RANK() OVER (ORDER BY x DESC) FROM T;",
                "3:50: warning: Floating point equality: 'RANK' compares floating point values of type DOUBLE for equality to detect ties.");
        this.assertWarnings("CREATE VIEW V AS SELECT x, RANK() OVER (PARTITION BY i ORDER BY x, y) FROM T;",
                "3:65: warning: Floating point equality: 'RANK' compares floating point values of type DOUBLE for equality to detect ties.",
                "3:68: warning: Floating point equality: 'RANK' compares floating point values of type DOUBLE for equality to detect ties.");
        // ROW_NUMBER has no ties
        this.assertNoWarnings("CREATE VIEW V AS SELECT x, ROW_NUMBER() OVER (ORDER BY x) FROM T;");
    }

    /** Named windows are resolved through the WINDOW clause of the enclosing SELECT */
    @Test
    public void namedWindows() {
        this.assertWarnings("CREATE VIEW V AS SELECT x, RANK() OVER (w ORDER BY y) FROM T WINDOW w AS (PARTITION BY x);",
                "3:52: warning: Floating point equality: 'RANK' compares floating point values of type DOUBLE for equality to detect ties.",
                "3:88: warning: Floating point equality: 'PARTITION BY' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT x, DENSE_RANK() OVER w FROM T WINDOW w AS (ORDER BY x, i);",
                "3:77: warning: Floating point equality: 'DENSE_RANK' compares floating point values of type DOUBLE for equality to detect ties.");
    }

    @Test
    public void membership() {
        this.assertWarnings("CREATE VIEW V AS SELECT x IN (1.0, 2.0), i IN (1, 2) FROM T;",
                "3:25: warning: Floating point equality: 'IN' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT * FROM T WHERE x IN (SELECT x FROM S);",
                "3:40: warning: Floating point equality: 'IN' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT * FROM T WHERE x NOT IN (SELECT x FROM S);",
                "3:40: warning: Floating point equality: 'NOT IN' " + DOUBLE);
    }

    /** The parser rewrites 'CASE x WHEN v' into 'x = v' */
    @Test
    public void caseAndNullif() {
        this.assertWarnings("CREATE VIEW V AS SELECT CASE x WHEN 1.0 THEN 1 ELSE 0 END FROM T;",
                "3:25: warning: Floating point equality: '=' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT NULLIF(x, 1.0) FROM T;",
                "3:25: warning: Floating point equality: 'NULLIF' " + DOUBLE);
    }

    @Test
    public void containers() {
        this.assertWarnings("CREATE VIEW V AS SELECT arr = ARRAY[1.0e0], m = m, rw <=> rw FROM T;",
                "3:25: warning: Floating point equality: '=' compares ARRAY values containing floating point values for equality.",
                "3:45: warning: Floating point equality: '=' compares MAP values containing floating point values for equality.",
                "3:52: warning: Floating point equality: '<=>' compares ROW values containing floating point values for equality.");
        this.assertWarnings("CREATE VIEW V AS SELECT DISTINCT rw FROM T;",
                "3:25: warning: Floating point equality: 'DISTINCT' compares the floating point values in column 'rw' for equality.");
    }

    /** Functions that compare array elements or map keys are checked where they are compiled */
    @Test
    public void arrayFunctions() {
        this.assertWarnings("CREATE VIEW V AS SELECT ARRAY_CONTAINS(arr, 1.0e0), ARRAY_POSITION(arr, 1.0e0), ARRAY_REMOVE(arr, 1.0e0), ARRAY_DISTINCT(arr) FROM T;",
                "3:25: warning: Floating point equality: 'ARRAY_CONTAINS' " + DOUBLE,
                "3:53: warning: Floating point equality: 'ARRAY_POSITION' " + DOUBLE,
                "3:81: warning: Floating point equality: 'ARRAY_REMOVE' " + DOUBLE,
                "3:107: warning: Floating point equality: 'ARRAY_DISTINCT' " + DOUBLE);
        this.assertWarnings("CREATE VIEW V AS SELECT ARRAY_EXCEPT(arr, arr), ARRAY_UNION(arr, arr), ARRAY_INTERSECT(arr, arr), ARRAYS_OVERLAP(arr, arr), ARRAY_CONCAT(arr, arr) FROM T;",
                "3:25: warning: Floating point equality: 'ARRAY_EXCEPT' " + DOUBLE,
                "3:49: warning: Floating point equality: 'ARRAY_UNION' " + DOUBLE,
                "3:72: warning: Floating point equality: 'ARRAY_INTERSECT' " + DOUBLE,
                "3:99: warning: Floating point equality: 'ARRAYS_OVERLAP' " + DOUBLE);
        this.assertNoWarnings("CREATE VIEW V AS SELECT ARRAY_DISTINCT(ARRAY[i]), ARRAY_CONTAINS(ARRAY[i], 1) FROM T;");
    }

    @Test
    public void mapKeys() {
        this.assertWarnings("CREATE VIEW V AS SELECT md[1.0e0], MAP_CONTAINS_KEY(md, 1.0e0), m['a'], MAP_CONTAINS_KEY(m, 'a') FROM T;",
                "3:25: warning: Floating point equality: 'MAP[key]' " + DOUBLE,
                "3:36: warning: Floating point equality: 'MAP_CONTAINS_KEY' " + DOUBLE);
    }

    @Test
    public void pivot() {
        this.assertWarnings("CREATE VIEW V AS SELECT * FROM (SELECT x, i FROM T) PIVOT (COUNT(i) FOR x IN (1.0e0 AS one, 2.0e0 AS two));",
                "3:73: warning: Floating point equality: 'PIVOT' " + DOUBLE);
        this.assertNoWarnings("CREATE VIEW V AS SELECT * FROM (SELECT x, i FROM T) PIVOT (SUM(x) FOR i IN (1 AS one, 2 AS two));");
    }

    /** The validator replaces the ordinal by the select item */
    @Test
    public void groupByOrdinal() {
        this.assertWarnings("CREATE VIEW V AS SELECT x, COUNT(*) FROM T GROUP BY 1;",
                "3:25: warning: Floating point equality: 'GROUP BY' " + DOUBLE);
    }

    @Test
    public void recursiveView() {
        this.assertWarnings("""
                DECLARE RECURSIVE VIEW R(x DOUBLE);
                CREATE VIEW R AS SELECT x FROM T UNION SELECT x + 1 FROM R WHERE x < 10;""",
                "4:18: warning: Floating point equality: 'UNION' compares the floating point values in column 'x' for equality.");
    }

    @Test
    public void correlatedSubquery() {
        this.assertWarnings("CREATE VIEW V AS SELECT * FROM T WHERE EXISTS (SELECT * FROM S WHERE S.x = T.x);",
                "3:70: warning: Floating point equality: '=' " + DOUBLE);
    }

    /** A function body is compiled inside a generated program; the warning
     * must point into the CREATE FUNCTION statement of the user's program */
    @Test
    public void functionBody() {
        this.assertWarnings("""
                CREATE FUNCTION eq(a DOUBLE, b DOUBLE) RETURNS BOOLEAN AS a = b;
                CREATE VIEW V AS SELECT eq(x, y) FROM T;""",
                "3:59: warning: Floating point equality: '=' " + DOUBLE);
        Assert.assertEquals("""
                (no input file): Floating point equality
                (no input file):5:3: warning: Floating point equality: '=' compares floating point values of type DOUBLE for equality.
                See https://docs.feldera.com/sql/comparisons#comparing-floating-point-values
                    5|  a = b;
                        ^^^^^
                    6|CREATE VIEW V AS SELECT eq(x, y) FROM T;""",
                this.outputFPWarnings("""
                        CREATE FUNCTION eq(a DOUBLE, b DOUBLE) RETURNS BOOLEAN AS
                          a > 0 AND
                          a = b;
                        CREATE VIEW V AS SELECT eq(x, y) FROM T;"""));
    }

    @Test
    public void silenced() {
        this.assertNoWarnings("""
                SET FELDERA_IGNORE_WARNING_FLOATING_POINT_EQUALITY = ON;
                CREATE VIEW V AS SELECT * FROM T WHERE x = y;""");
    }

    @Test
    public void warningsAreErrors() {
        this.statementsFailingInCompilation(TABLES + """
                SET FELDERA_WARNINGS_ARE_ERRORS = ON;
                CREATE VIEW V AS SELECT * FROM T WHERE x = y;""",
                "'=' " + DOUBLE);
    }
}
