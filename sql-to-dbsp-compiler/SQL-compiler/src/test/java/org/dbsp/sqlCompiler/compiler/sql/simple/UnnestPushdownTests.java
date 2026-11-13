package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests for {@link org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.FilterUncollectTransposeRule} */
public class UnnestPushdownTests extends SqlIoTest {
    @Test
    public void outerUnnestKeepsRows() {
        var ccs = this.getCCS("""
                CREATE TABLE t (id INT, arr INT ARRAY);
                CREATE VIEW v AS
                SELECT t.id, x FROM t LEFT JOIN UNNEST(t.arr) AS x ON TRUE
                WHERE t.id > 1;""");
        ccs.step("INSERT INTO t VALUES (1, ARRAY(1)), (2, ARRAY()), (3, NULL), (4, ARRAY(5, 6));", """
                 id | x    | weight
                -------------------
                 2  |NULL | 1
                 3  |NULL | 1
                 4  | 5   | 1
                 4  | 6   | 1""");
    }

    @Test
    public void mixedConjuncts() {
        var ccs = this.getCCS("""
                CREATE TABLE t (id INT, d INT, arr INT ARRAY);
                CREATE VIEW v AS
                SELECT t.id, x FROM t, UNNEST(t.arr) AS x
                WHERE t.id = 1 AND 10 / t.d > 1 AND x > 2;""");
        ccs.step("INSERT INTO t VALUES (1, 2, ARRAY(1, 3, 4)), (2, 0, ARRAY(1)), (1, 0, ARRAY());", """
                 id | x | weight
                --------
                 1  | 3 | 1
                 1  | 4 | 1""");
    }

    @Test
    public void ordinalityAndPassthrough() {
        var ccs = this.getCCS("""
                CREATE TABLE t (id INT, arr INT ARRAY);
                CREATE VIEW v AS
                SELECT t.id, x, o FROM t, UNNEST(t.arr) WITH ORDINALITY AS m(x, o)
                WHERE o = 2 AND t.id IN (1, 3);""");
        ccs.step("INSERT INTO t VALUES (1, ARRAY(7, 8)), (2, ARRAY(9, 10)), (3, ARRAY(11));", """
                 id | x | o | weight
                ------------
                 1  | 8 | 2 | 1""");
    }

    @Test
    public void orAcrossPassthroughAndElement() {
        var ccs = this.getCCS("""
                CREATE TABLE t (id INT, arr INT ARRAY);
                CREATE VIEW v AS
                SELECT t.id, x FROM t, UNNEST(t.arr) AS x
                WHERE t.id = 1 OR x = 10;""");
        ccs.step("INSERT INTO t VALUES (1, ARRAY(7)), (2, ARRAY(9, 10));", """
                 id | x | weight
                --------
                 1  | 7 | 1
                 2  | 10 | 1""");
    }

    @Test
    public void wideningCastPushed() {
        var ccs = this.getCCS("""
                CREATE TABLE t (id TINYINT, s VARCHAR, arr INT ARRAY);
                CREATE VIEW v AS
                SELECT t.id, x FROM t, UNNEST(t.arr) AS x
                WHERE CAST(t.id AS BIGINT) = 2 AND t.s = 'a';""");
        ccs.step("INSERT INTO t VALUES (1, 'a', ARRAY(7)), (2, 'a', ARRAY(9)), (2, NULL, ARRAY(3)), (NULL, 'a', ARRAY(1));", """
                 id | x | weight
                --------
                 2  | 9 | 1""");
    }

    @Test
    public void unsafeCastStaysAbove() {
        var ccs = this.getCCS("""
                CREATE TABLE t (id BIGINT, arr INT ARRAY);
                CREATE VIEW v AS
                SELECT t.id, x FROM t, UNNEST(t.arr) AS x
                WHERE CAST(t.id AS TINYINT) = 2;""");
        ccs.step("INSERT INTO t VALUES (1000, ARRAY()), (2, ARRAY(4));", """
                 id | x | weight
                --------
                 2  | 4 | 1""");
    }
}
