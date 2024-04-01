package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

public class AggregateTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.compileStatements("""
                CREATE TABLE T(
                   B BIGINT,
                   I INTEGER,
                   S SMALLINT,
                   T TINYINT,
                   R REAL,
                   D DOUBLE,
                   E DECIMAL(3,2)
                );
                INSERT INTO T VALUES
                   (NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                   (0, 0, 0, 0, 0, 0, 0),
                   (1, 1, 1, 1, 1, 1, 1),
                   (2, 2, 2, 2, 2, 2, 2);
                """);
    }

    @Test
    public void testAggregates() {
        this.qs("""
                SELECT COUNT(*), COUNT(B), COUNT(I), COUNT(S), COUNT(T), COUNT(R), COUNT(D), COUNT(E) FROM T;
                 * | B | I | S | T | R | D | E
                -------------------------------
                 4 | 3 | 3 | 3 | 3 | 3 | 3 | 3
                (3 row)
                
                SELECT SUM(B), SUM(I), SUM(S), SUM(T), SUM(R), SUM(D), SUM(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 3 | 3 | 3 | 3 | 3 | 3 | 3
                (3 row)
                
                SELECT AVG(B), AVG(I), AVG(S), AVG(T), AVG(R), AVG(D), AVG(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (3 row)
                
                SELECT MIN(B), MIN(I), MIN(S), MIN(T), MIN(R), MIN(D), MIN(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 0 | 0 | 0 | 0 | 0 | 0 | 0
                (3 row)
                
                SELECT MAX(B), MAX(I), MAX(S), MAX(T), MAX(R), MAX(D), MAX(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 2 | 2 | 2 | 2 | 2 | 2 | 2
                (3 row)
                
                SELECT STDDEV(B), STDDEV(I), STDDEV(S), STDDEV(T), STDDEV(R), STDDEV(D), STDDEV(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (3 row)
                
                SELECT STDDEV_POP(B), STDDEV_POP(I), STDDEV_POP(S), STDDEV_POP(T), STDDEV_POP(R), STDDEV_POP(D), STDDEV_POP(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 0 | 0 | 0 | 0 | 0.8164966 | 0.816496580927726 | 0.82
                (3 row)
                
                SELECT STDDEV_SAMP(B), STDDEV_SAMP(I), STDDEV_SAMP(S), STDDEV_SAMP(T), STDDEV_SAMP(R), STDDEV_SAMP(D), STDDEV_SAMP(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (3 row)
                
                SELECT BIT_AND(B), BIT_AND(I), BIT_AND(S), BIT_AND(T) FROM T;
                 B | I | S | T
                ---------------
                 0 | 0 | 0 | 0
                (3 row)
                
                SELECT BIT_OR(B), BIT_OR(I), BIT_OR(S), BIT_OR(T) FROM T;
                 B | I | S | T
                ---------------
                 3 | 3 | 3 | 3
                (3 row)
                
                SELECT BIT_XOR(B), BIT_XOR(I), BIT_XOR(S), BIT_XOR(T) FROM T;
                 B | I | S | T
                ---------------
                 3 | 3 | 3 | 3
                (3 row)""");
    }

    @Test
    public void testDistinctAggregates() {
        this.qs("""
                SELECT COUNT(*), COUNT(DISTINCT B), COUNT(DISTINCT I), COUNT(DISTINCT S), COUNT(DISTINCT T), COUNT(DISTINCT R), COUNT(DISTINCT D), COUNT(DISTINCT E) FROM T;
                 * | B | I | S | T | R | D | E
                -------------------------------
                 4 | 3 | 3 | 3 | 3 | 3 | 3 | 3
                (3 row)
                
                SELECT SUM(DISTINCT B), SUM(DISTINCT I), SUM(DISTINCT S), SUM(DISTINCT T), SUM(DISTINCT R), SUM(DISTINCT D), SUM(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 3 | 3 | 3 | 3 | 3 | 3 | 3
                (3 row)
                
                SELECT AVG(DISTINCT B), AVG(DISTINCT I), AVG(DISTINCT S), AVG(DISTINCT T), AVG(DISTINCT R), AVG(DISTINCT D), AVG(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (3 row)
                
                SELECT MIN(DISTINCT B), MIN(DISTINCT I), MIN(DISTINCT S), MIN(DISTINCT T), MIN(DISTINCT R), MIN(DISTINCT D), MIN(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 0 | 0 | 0 | 0 | 0 | 0 | 0
                (3 row)
                
                SELECT MAX(DISTINCT B), MAX(DISTINCT I), MAX(DISTINCT S), MAX(DISTINCT T), MAX(DISTINCT R), MAX(DISTINCT D), MAX(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 2 | 2 | 2 | 2 | 2 | 2 | 2
                (3 row)
                
                SELECT STDDEV(DISTINCT B), STDDEV(DISTINCT I), STDDEV(DISTINCT S), STDDEV(DISTINCT T), STDDEV(DISTINCT R), STDDEV(DISTINCT D), STDDEV(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (3 row)
                
                SELECT STDDEV_POP(DISTINCT B), STDDEV_POP(DISTINCT I), STDDEV_POP(DISTINCT S), STDDEV_POP(DISTINCT T), STDDEV_POP(DISTINCT R), STDDEV_POP(DISTINCT D), STDDEV_POP(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 0 | 0 | 0 | 0 | 0.8164966 | 0.816496580927726 | 0.82
                (3 row)
                
                SELECT STDDEV_SAMP(DISTINCT B), STDDEV_SAMP(DISTINCT I), STDDEV_SAMP(DISTINCT S), STDDEV_SAMP(DISTINCT T), STDDEV_SAMP(DISTINCT R), STDDEV_SAMP(DISTINCT D), STDDEV_SAMP(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (3 row)
                
                SELECT BIT_AND(DISTINCT B), BIT_AND(DISTINCT I), BIT_AND(DISTINCT S), BIT_AND(DISTINCT T) FROM T;
                 B | I | S | T
                ---------------
                 0 | 0 | 0 | 0
                (3 row)
                
                SELECT BIT_OR(DISTINCT B), BIT_OR(DISTINCT I), BIT_OR(DISTINCT S), BIT_OR(DISTINCT T) FROM T;
                 B | I | S | T
                ---------------
                 3 | 3 | 3 | 3
                (3 row)
                
                SELECT BIT_XOR(DISTINCT B), BIT_XOR(DISTINCT I), BIT_XOR(DISTINCT S), BIT_XOR(DISTINCT T) FROM T;
                 B | I | S | T
                ---------------
                 3 | 3 | 3 | 3
                (3 row)""");
    }
}
