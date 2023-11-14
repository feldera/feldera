package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

/**
 * <a href="https://github.com/postgres/postgres/blob/master/src/test/regress/expected/boolean.out">boolean.out</a>
 */
public class PostgresBoolTests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        compiler.compileStatements(
                        // "CREATE TABLE BOOLTBL1 (f1 bool);\n" +
                        // "INSERT INTO BOOLTBL1 (f1) VALUES (bool 't');\n" +
                        // "INSERT INTO BOOLTBL1 (f1) VALUES (bool 'True');\n" +
                        // "INSERT INTO BOOLTBL1 (f1) VALUES (bool 'true');\n" +
                        // "CREATE TABLE BOOLTBL2 (f1 bool);\n" +
                        // "INSERT INTO BOOLTBL2 (f1) VALUES (bool 'f');\n" +
                        // "INSERT INTO BOOLTBL2 (f1) VALUES (bool 'false');\n" +
                        // "INSERT INTO BOOLTBL2 (f1) VALUES (bool 'False');\n" +
                        // "INSERT INTO BOOLTBL2 (f1) VALUES (bool 'FALSE');" +
                        "CREATE TABLE BOOLTBL3 (d text, b bool, o int);\n" +
                        "INSERT INTO BOOLTBL3 (d, b, o) VALUES ('true', true, 1);\n" +
                        "INSERT INTO BOOLTBL3 (d, b, o) VALUES ('false', false, 2);\n" +
                        "INSERT INTO BOOLTBL3 (d, b, o) VALUES ('null', null, 3);\n" +
                        "CREATE TABLE booltbl4(isfalse bool, istrue bool, isnul bool);\n" +
                        "INSERT INTO booltbl4 VALUES (false, true, null);\n"
        );
    }

    @Test
    public void testOne() {
        this.q("SELECT 1 as 'one';\n" + " one \n" +
                "-----\n" +
                "   1");
    }

    @Test
    public void testFalse() {
        this.q("SELECT true AS true;\n" +
                " true \n" +
                "------\n" +
                " t");
        this.q("SELECT false AS false;\n" +
                " false \n" +
                "-------\n" +
                " f");
    }

    // SELECT bool 't' as 'true'
    // SELECT bool '   f           ' AS false
    // SELECT bool 'true' AS true
    // SELECT bool 'test' AS error;
    // ERROR:  invalid input syntax for type boolean: "test"
    // SELECT bool 'yes' AS true
    // SELECT bool 'yeah' AS error;
    // ERROR:  invalid input syntax for type boolean: "yeah"
    // SELECT bool 'no' AS false
    // SELECT bool 'on' AS true;
    // SELECT bool 'off' AS false;
    // SELECT bool 'of' AS false;
    // SELECT bool 'o' AS error;
    // ERROR:  invalid input syntax for type boolean: "o"
    // SELECT bool 'on_' AS error;
    // ERROR:  invalid input syntax for type boolean: "on_"
    // SELECT bool 'off_' AS error;
    // ERROR:  invalid input syntax for type boolean: "off_"
    // SELECT bool '1' AS true;
    // SELECT bool '11' AS error;
    // ERROR:  invalid input syntax for type boolean: "11"
    // SELECT bool '0' AS false;
    // SELECT bool '000' AS error;
    // SELECT bool '' AS error;
    // ERROR:  invalid input syntax for type boolean: ""

    @Test
    public void testIs() {
        this.q("SELECT\n" +
                "    d,\n" +
                "    b IS TRUE AS istrue,\n" +
                "    b IS NOT TRUE AS isnottrue,\n" +
                "    b IS FALSE AS isfalse,\n" +
                "    b IS NOT FALSE AS isnotfalse,\n" +
                "    b IS UNKNOWN AS isunknown,\n" +
                "    b IS NOT UNKNOWN AS isnotunknown\n" +
                "FROM booltbl3;\n" +
                "   d   | istrue | isnottrue | isfalse | isnotfalse | isunknown | isnotunknown \n" +
                "-------+--------+-----------+---------+------------+-----------+--------------\n" +
                " true|   t      | f         | f       | t          | f         | t\n" +
                " false|  f      | t         | t       | f          | f         | t\n" +
                " null|   f      | t         | f       | t          | t         | f");
    }

    @Test
    public void testShortcut() {
        // This is not from Postgres
        this.q("SELECT v0, v1, v0 OR v1 FROM " +
                "(SELECT b AS v0 FROM BOOLTBL3) CROSS JOIN (SELECT b as v1 FROM BOOLTBL3);\n" +
                " v0 | v1 | v0 OR v1 \n" +
                "--------------------\n" +
                " t  | t  | t\n" +
                " t  | f  | t\n" +
                " f  | t  | t\n" +
                " f  | f  | f\n" +
                "null| t  | t\n" +
                "null| f  |null\n" +
                " t  |null| t\n" +
                " f  |null|null\n" +
                "null|null|null");
        this.q("SELECT v0, v1, v0 AND v1 FROM " +
                "(SELECT b AS v0 FROM BOOLTBL3) CROSS JOIN (SELECT b as v1 FROM BOOLTBL3);\n" +
                " v0 | v1 | v0 AND v1 \n" +
                "--------------------\n" +
                " t  | t  | t\n" +
                " t  | f  | f\n" +
                " f  | t  | f\n" +
                " f  | f  | f\n" +
                "null| t  |null\n" +
                "null| f  | f\n" +
                " t  |null|null\n" +
                " f  |null| f\n" +
                "null|null|null");
        this.q("SELECT v0, NOT v0 FROM " +
                "(SELECT b AS v0 FROM BOOLTBL3);\n" +
                " v0 | NOT v0 \n" +
                "---------------\n" +
                " t  | f\n" +
                " f  | t\n" +
                "null|null");
    }

    @Test
    public void testBool() {
        this.qs("SELECT istrue AND isnul AND istrue FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                "null\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT istrue AND istrue AND isnul FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                "null\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT isnul AND istrue AND istrue FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                "null\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT isfalse AND isnul AND istrue FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                " f\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT istrue AND isfalse AND isnul FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                " f\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT isnul AND istrue AND isfalse FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                " f\n" +
                "(1 row)\n" +
                "\n" +
                "-- OR expression need to return null if there's any nulls and none\n" +
                "-- of the value is true\n" +
                "SELECT isfalse OR isnul OR isfalse FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                "null\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT isfalse OR isfalse OR isnul FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                "null\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT isnul OR isfalse OR isfalse FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                "null\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT isfalse OR isnul OR istrue FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                " t\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT istrue OR isfalse OR isnul FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                " t\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT isnul OR istrue OR isfalse FROM booltbl4;\n" +
                " ?column? \n" +
                "----------\n" +
                " t\n" +
                "(1 row)");
    }
}
