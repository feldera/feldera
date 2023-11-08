package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/aggregates.out#L779
public class PostgresAggregatesTests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        // Changed BIT(4) to BINARY(1)
        String setup = "CREATE TABLE bitwise_test(\n" +
                "  i2 INT2,\n" +
                "  i4 INT4,\n" +
                "  i8 INT8,\n" +
                "  i INTEGER,\n" +
                "  x INT2,\n" +
                "  y BINARY(1)\n" +
                ");\n" +
                "INSERT INTO bitwise_test VALUES(1, 1, 1, 1, 1, x'05');\n" +
                "INSERT INTO bitwise_test VALUES(3, 3, 3, null, 2, x'04');\n" +
                "INSERT INTO bitwise_test VALUES(7, 7, 7, 3, 4, x'0C');\n" +
                "CREATE TABLE bool_test(\n" +
                "  b1 BOOL,\n" +
                "  b2 BOOL,\n" +
                "  b3 BOOL,\n" +
                "  b4 BOOL);\n" +
                "INSERT INTO bool_test VALUES(TRUE, null, FALSE, null);\n" +
                "INSERT INTO bool_test VALUES(FALSE, TRUE, null, null);\n" +
                "INSERT INTO bool_test VALUES(null, TRUE, FALSE, null);";
        compiler.compileStatements(setup);
    }

    @Test
    public void testBitAggs() {
        this.qs("-- empty case\n" +
                "SELECT\n" +
                "  BIT_AND(i2),\n" +
                "  BIT_OR(i4) ,\n" +
                "  BIT_XOR(i8)\n" +
                "FROM (SELECT * FROM bitwise_test WHERE FALSE);\n" +
                " a | o | x \n" +
                "---+---+---\n" +
                "   |   |  \n" +
                "(1 row)\n" +
                "\n" +
                "SELECT\n" +
                "  BIT_AND(i2),\n" +
                "  BIT_AND(i4),\n" +
                "  BIT_AND(i8),\n" +
                "  BIT_AND(i), \n" +
                "  BIT_AND(x), \n" +
                "  BIT_AND(y), \n" +
                "  BIT_OR(i2), \n" +
                "  BIT_OR(i4), \n" +
                "  BIT_OR(i8), \n" +
                "  BIT_OR(i),  \n" +
                "  BIT_OR(x),  \n" +
                "  BIT_OR(y),  \n" +
                "  BIT_XOR(i2),\n" +
                "  BIT_XOR(i4),\n" +
                "  BIT_XOR(i8),\n" +
                "  BIT_XOR(i), \n" +
                "  BIT_XOR(x), \n" +
                "  BIT_XOR(y)  \n" +
                "FROM bitwise_test;\n" +
                " 1 | 1 | 1 | ? | 0 | 0100 | 7 | 7 | 7 | ? | 7 | 1101 | 5 | 5 | 5 | ? | 7 | 1101 \n" +
                "---+---+---+---+---+------+---+---+---+---+---+------+---+---+---+---+---+------\n" +
                " 1 | 1 | 1 | 1 | 0 | 04   | 7 | 7 | 7 | 3 | 7 | 0D   | 5 | 5 | 5 | 2 | 7 | 0D   \n" +
                "(1 row)");
    }

    @Test
    public void testBoolAgg() {
        this.qs("-- empty case\n" +
                "SELECT\n" +
                "  BOOL_AND(b1)   AS \"n0\",\n" +
                "  BOOL_OR(b3)    AS \"n1\"\n" +
                "FROM (SELECT * FROM bool_test WHERE FALSE);\n" +
                " n | n \n" +
                "---+---\n" +
                "   | \n" +
                "(1 row)\n" +
                "\n" +
                "\n" +
                "SELECT\n" +
                "  EVERY(b1)    ,\n" +
                "  EVERY(b2)    ,\n" +
                "  EVERY(b3)    ,\n" +
                "  EVERY(b4)    ,\n" +
                "  EVERY(NOT b2),\n" +
                "  EVERY(NOT b3)\n" +
                "FROM bool_test;\n" +
                " f | t | f | n | f | t \n" +
                "---+---+---+---+---+---\n" +
                " f | t | f |   | f | t\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT\n" +
                "  BOOL_OR(b1)    ,\n" +
                "  BOOL_OR(b2)    ,\n" +
                "  BOOL_OR(b3)    ,\n" +
                "  BOOL_OR(b4)    ,\n" +
                "  BOOL_OR(NOT b2),\n" +
                "  BOOL_OR(NOT b3)\n" +
                "FROM bool_test;\n" +
                " t | t | f | n | f | t \n" +
                "---+---+---+---+---+---\n" +
                " t | t | f |   | f | t\n" +
                "(1 row)");
    }
}
