package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/**
 * <a href="https://github.com/postgres/postgres/blob/master/src/test/regress/expected/boolean.out">boolean.out</a>
 */
public class PostgresBoolTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation(
                        // "CREATE TABLE BOOLTBL1 (f1 bool);\n" +
                        // "INSERT INTO BOOLTBL1 (f1) VALUES (bool 't');\n" +
                        // "INSERT INTO BOOLTBL1 (f1) VALUES (bool 'True');\n" +
                        // "INSERT INTO BOOLTBL1 (f1) VALUES (bool 'true');\n" +
                        // "CREATE TABLE BOOLTBL2 (f1 bool);\n" +
                        // "INSERT INTO BOOLTBL2 (f1) VALUES (bool 'f');\n" +
                        // "INSERT INTO BOOLTBL2 (f1) VALUES (bool 'false');\n" +
                        // "INSERT INTO BOOLTBL2 (f1) VALUES (bool 'False');\n" +
                        // "INSERT INTO BOOLTBL2 (f1) VALUES (bool 'FALSE');" +
                """
                        CREATE TABLE BOOLTBL3 (d text, b bool, o int);
                        INSERT INTO BOOLTBL3 (d, b, o) VALUES ('true', true, 1);
                        INSERT INTO BOOLTBL3 (d, b, o) VALUES ('false', false, 2);
                        INSERT INTO BOOLTBL3 (d, b, o) VALUES ('null', null, 3);
                        CREATE TABLE booltbl4(isfalse bool, istrue bool, isnul bool);
                        INSERT INTO booltbl4 VALUES (false, true, null);
                        """
        );
    }

    @Test
    public void testOne() {
        this.q("""
                SELECT 1 as 'one';
                 one
                -----
                   1""");
    }

    @Test
    public void testFalse() {
        this.q("""
                SELECT true AS true;
                 true
                ------
                 t""");
        this.q("""
                SELECT false AS false;
                 false
                -------
                 f""");
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
        this.q("""
                SELECT
                    d,
                    b IS TRUE AS istrue,
                    b IS NOT TRUE AS isnottrue,
                    b IS FALSE AS isfalse,
                    b IS NOT FALSE AS isnotfalse,
                    b IS UNKNOWN AS isunknown,
                    b IS NOT UNKNOWN AS isnotunknown
                FROM booltbl3;
                   d   | istrue | isnottrue | isfalse | isnotfalse | isunknown | isnotunknown
                -------+--------+-----------+---------+------------+-----------+--------------
                 true|   t      | f         | f       | t          | f         | t
                 false|  f      | t         | t       | f          | f         | t
                 null|   f      | t         | f       | t          | t         | f""");
    }

    @Test
    public void testShortcut() {
        // This is not from Postgres
        this.q("""
                SELECT v0, v1, v0 OR v1 FROM (SELECT b AS v0 FROM BOOLTBL3) CROSS JOIN (SELECT b as v1 FROM BOOLTBL3);
                 v0 | v1 | v0 OR v1
                --------------------
                 t  | t  | t
                 t  | f  | t
                 f  | t  | t
                 f  | f  | f
                null| t  | t
                null| f  |null
                 t  |null| t
                 f  |null|null
                null|null|null""");
        this.q("""
                SELECT v0, v1, v0 AND v1 FROM (SELECT b AS v0 FROM BOOLTBL3) CROSS JOIN (SELECT b as v1 FROM BOOLTBL3);
                 v0 | v1 | v0 AND v1
                --------------------
                 t  | t  | t
                 t  | f  | f
                 f  | t  | f
                 f  | f  | f
                null| t  |null
                null| f  | f
                 t  |null|null
                 f  |null| f
                null|null|null""");
        this.q("""
                SELECT v0, NOT v0 FROM (SELECT b AS v0 FROM BOOLTBL3);
                 v0 | NOT v0
                ---------------
                 t  | f
                 f  | t
                null|null""");
    }

    @Test
    public void testBool() {
        this.qs("""
                SELECT istrue AND isnul AND istrue FROM booltbl4;
                 ?column?
                ----------
                null
                (1 row)

                SELECT istrue AND istrue AND isnul FROM booltbl4;
                 ?column?
                ----------
                null
                (1 row)

                SELECT isnul AND istrue AND istrue FROM booltbl4;
                 ?column?
                ----------
                null
                (1 row)

                SELECT isfalse AND isnul AND istrue FROM booltbl4;
                 ?column?
                ----------
                 f
                (1 row)

                SELECT istrue AND isfalse AND isnul FROM booltbl4;
                 ?column?
                ----------
                 f
                (1 row)

                SELECT isnul AND istrue AND isfalse FROM booltbl4;
                 ?column?
                ----------
                 f
                (1 row)

                -- OR expression need to return null if there's any nulls and none
                -- of the value is true
                SELECT isfalse OR isnul OR isfalse FROM booltbl4;
                 ?column?
                ----------
                null
                (1 row)

                SELECT isfalse OR isfalse OR isnul FROM booltbl4;
                 ?column?
                ----------
                null
                (1 row)

                SELECT isnul OR isfalse OR isfalse FROM booltbl4;
                 ?column?
                ----------
                null
                (1 row)

                SELECT isfalse OR isnul OR istrue FROM booltbl4;
                 ?column?
                ----------
                 t
                (1 row)

                SELECT istrue OR isfalse OR isnul FROM booltbl4;
                 ?column?
                ----------
                 t
                (1 row)

                SELECT isnul OR istrue OR isfalse FROM booltbl4;
                 ?column?
                ----------
                 t
                (1 row)""");
    }
}
