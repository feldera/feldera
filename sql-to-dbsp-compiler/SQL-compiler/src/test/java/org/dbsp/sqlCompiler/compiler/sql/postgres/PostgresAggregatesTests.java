package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/aggregates.out
public class PostgresAggregatesTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // Changed BIT(4) to BINARY(1)
        String setup = """
                CREATE TABLE bitwise_test(
                  i2 INT2,
                  i4 INT4,
                  i8 INT8,
                  i INTEGER,
                  x INT2,
                  y BINARY(1)
                );
                INSERT INTO bitwise_test VALUES(1, 1, 1, 1, 1, x'05');
                INSERT INTO bitwise_test VALUES(3, 3, 3, null, 2, x'04');
                INSERT INTO bitwise_test VALUES(7, 7, 7, 3, 4, x'0C');
                CREATE TABLE bool_test(
                  b1 BOOL,
                  b2 BOOL,
                  b3 BOOL,
                  b4 BOOL);
                INSERT INTO bool_test VALUES(TRUE, null, FALSE, null);
                INSERT INTO bool_test VALUES(FALSE, TRUE, null, null);
                INSERT INTO bool_test VALUES(null, TRUE, FALSE, null);""";
        compiler.compileStatements(setup);
    }

    @Test
    public void testBitAggs() {
        this.qs("""
                -- empty case
                SELECT
                  BIT_AND(i2),
                  BIT_OR(i4) ,
                  BIT_XOR(i8)
                FROM (SELECT * FROM bitwise_test WHERE FALSE);
                 a | o | x
                ---+---+---
                   |   | \s
                (1 row)

                SELECT
                  BIT_AND(i2),
                  BIT_AND(i4),
                  BIT_AND(i8),
                  BIT_AND(i),
                  BIT_AND(x),
                  BIT_AND(y),
                  BIT_OR(i2),
                  BIT_OR(i4),
                  BIT_OR(i8),
                  BIT_OR(i),
                  BIT_OR(x),
                  BIT_OR(y),
                  BIT_XOR(i2),
                  BIT_XOR(i4),
                  BIT_XOR(i8),
                  BIT_XOR(i),
                  BIT_XOR(x),
                  BIT_XOR(y)
                FROM bitwise_test;
                 1 | 1 | 1 | ? | 0 | 0100 | 7 | 7 | 7 | ? | 7 | 1101 | 5 | 5 | 5 | ? | 7 | 1101
                ---+---+---+---+---+------+---+---+---+---+---+------+---+---+---+---+---+------
                 1 | 1 | 1 | 1 | 0 | 04   | 7 | 7 | 7 | 3 | 7 | 0D   | 5 | 5 | 5 | 2 | 7 | 0D
                (1 row)""");
    }

    @Test
    public void testBoolAgg() {
        this.qs("""
                -- empty case
                SELECT
                  BOOL_AND(b1)   AS "n0",
                  BOOL_OR(b3)    AS "n1"
                FROM (SELECT * FROM bool_test WHERE FALSE);
                 n | n
                ---+---
                   |\s
                (1 row)


                SELECT
                  EVERY(b1)    ,
                  EVERY(b2)    ,
                  EVERY(b3)    ,
                  EVERY(b4)    ,
                  EVERY(NOT b2),
                  EVERY(NOT b3)
                FROM bool_test;
                 f | t | f | n | f | t
                ---+---+---+---+---+---
                 f | t | f |   | f | t
                (1 row)

                SELECT
                  BOOL_OR(b1)    ,
                  BOOL_OR(b2)    ,
                  BOOL_OR(b3)    ,
                  BOOL_OR(b4)    ,
                  BOOL_OR(NOT b2),
                  BOOL_OR(NOT b3)
                FROM bool_test;
                 t | t | f | n | f | t
                ---+---+---+---+---+---
                 t | t | f |   | f | t
                (1 row)""");
    }
}
