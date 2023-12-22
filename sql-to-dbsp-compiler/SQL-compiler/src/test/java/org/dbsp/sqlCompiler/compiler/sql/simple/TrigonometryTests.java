package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

public class TrigonometryTests extends SqlIoTest {
    // Tested using Postgres 15.2
    @Test
    public void testSin() {
        this.qs(
        "SELECT sin(null);\n" +
                " sin \n" +
                "-----\n" +
                "NULL\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT sin(0);\n" +
                " sin \n" +
                "-----\n" +
                " 0\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT sin(0.25);\n" +
                " sin \n" +
                "-----\n" +
                " 0.24740395925452294\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT sin(0.5);\n" +
                " sin \n" +
                "-----\n" +
                " 0.479425538604203\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT sin(0.75);\n" +
                " sin \n" +
                "-----\n" +
                " 0.6816387600233341\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT sin(1);\n" +
                " sin \n" +
                "-----\n" +
                " 0.8414709848078965\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT sin(pi);\n" +
                " sin \n" +
                "-----\n" +
                " 0\n" + // Postgres disagrees with us here, but currently we round off to 15 decimal places
                "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testSinDouble() {
        this.qs(
                """
                        SELECT sin(CAST(0 AS DOUBLE));
                         sin\s
                        -----
                         0
                        (1 row)

                        SELECT sin(CAST(0.25 AS DOUBLE));
                         sin\s
                        -----
                         0.24740395925452294
                        (1 row)

                        SELECT sin(CAST(0.5 AS DOUBLE));
                         sin\s
                        -----
                         0.479425538604203
                        (1 row)

                        SELECT sin(CAST(0.75 AS DOUBLE));
                         sin\s
                        -----
                         0.6816387600233341
                        (1 row)

                        SELECT sin(CAST(1 AS DOUBLE));
                         sin\s
                        -----
                         0.8414709848078965
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testCos() {
        this.qs(
                """
                        SELECT cos(null);
                         cos\s
                        -----
                        NULL
                        (1 row)

                        SELECT cos(0);
                         cos\s
                        -----
                         1
                        (1 row)

                        SELECT cos(0.25);
                         cos\s
                        -----
                         0.9689124217106447
                        (1 row)

                        SELECT cos(0.5);
                         cos\s
                        -----
                         0.8775825618903728
                        (1 row)

                        SELECT cos(0.75);
                         cos\s
                        -----
                         0.7316888688738209
                        (1 row)

                        SELECT cos(1);
                         cos\s
                        -----
                         0.5403023058681398
                        (1 row)

                        SELECT cos(pi);
                         cos\s
                        -----
                         -1
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testCosDouble() {
        this.qs(
                """
                        SELECT cos(CAST(0 AS DOUBLE));
                         cos\s
                        -----
                         1
                        (1 row)

                        SELECT cos(CAST(0.25 AS DOUBLE));
                         cos\s
                        -----
                         0.9689124217106447
                        (1 row)

                        SELECT cos(CAST(0.5 AS DOUBLE));
                         cos\s
                        -----
                         0.8775825618903728
                        (1 row)

                        SELECT cos(CAST(0.75 AS DOUBLE));
                         cos\s
                        -----
                         0.7316888688738209
                        (1 row)

                        SELECT cos(CAST(1 AS DOUBLE));
                         cos\s
                        -----
                         0.5403023058681398
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testPi() {
        this.q(
                """
                        SELECT PI;
                         pi\s
                        ----
                         3.141592653589793"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testTan() {
        this.qs(
                """
                        SELECT tan(null);
                         tan\s
                        -----
                        NULL
                        (1 row)

                        SELECT tan(0);
                         tan\s
                        -----
                         0
                        (1 row)

                        SELECT tan(0.25);
                         tan\s
                        -----
                         0.25534192122103627
                        (1 row)

                        SELECT tan(0.5);
                         tan\s
                        -----
                         0.5463024898437905
                        (1 row)

                        SELECT tan(0.75);
                         tan\s
                        -----
                         0.9315964599440725
                        (1 row)

                        SELECT tan(1);
                         tan\s
                        -----
                         1.5574077246549023
                        (1 row)

                        SELECT tan(pi);
                         tan\s
                        -----
                         -1.2246467991473532e-16
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testTanDouble() {
        this.qs(
                """
                        SELECT tan(CAST(null as DOUBLE));
                         tan\s
                        -----
                        NULL
                        (1 row)

                        SELECT tan(CAST(0 AS DOUBLE));
                         tan\s
                        -----
                         0
                        (1 row)

                        SELECT tan(CAST(0.25 AS DOUBLE));
                         tan\s
                        -----
                         0.25534192122103627
                        (1 row)

                        SELECT tan(CAST(0.5 AS DOUBLE));
                         tan\s
                        -----
                         0.5463024898437905
                        (1 row)

                        SELECT tan(CAST(0.75 AS DOUBLE));
                         tan\s
                        -----
                         0.9315964599440725
                        (1 row)

                        SELECT tan(CAST(1 AS DOUBLE));
                         tan\s
                        -----
                         1.5574077246549023
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testCot() {
        this.qs(
                """
                        SELECT cot(0);
                         cot\s
                        -----
                         Infinity
                        (1 row)

                        SELECT cot(pi);
                         cot\s
                        -----
                         -8.165619676597685e+15
                        (1 row)

                        SELECT cot(null);
                         cot\s
                        -----
                        NULL
                        (1 row)

                        SELECT cot(0.25);
                         cot\s
                        -----
                         3.91631736464594
                        (1 row)

                        SELECT cot(0.5);
                         cot\s
                        -----
                         1.830487721712452
                        (1 row)

                        SELECT cot(0.75);
                         cot\s
                        -----
                         1.0734261485493772
                        (1 row)

                        SELECT cot(1);
                         cot\s
                        -----
                         0.6420926159343306
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testCotDouble() {
        this.qs(
                """
                        SELECT cot(CAST(0 AS DOUBLE));
                         cot\s
                        -----
                         Infinity
                        (1 row)

                        SELECT cot(pi);
                         cot\s
                        -----
                         -8.165619676597685e+15
                        (1 row)

                        SELECT cot(CAST(null AS DOUBLE));
                         cot\s
                        -----
                        NULL
                        (1 row)

                        SELECT cot(CAST(0.25 AS DOUBLE));
                         cot\s
                        -----
                         3.91631736464594
                        (1 row)

                        SELECT cot(CAST(0.5 AS DOUBLE));
                         cot\s
                        -----
                         1.830487721712452
                        (1 row)

                        SELECT cot(CAST(0.75 AS DOUBLE));
                         cot\s
                        -----
                         1.0734261485493772
                        (1 row)

                        SELECT cot(CAST(1 AS DOUBLE));
                         cot\s
                        -----
                         0.6420926159343306
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    // Postgres throws an error if the argument is out of the range: [-1, 1]
    // Right now, we return a NaN, instead of throwing an error
    @Test
    public void testAsin() {
        this.qs(
                """
                        SELECT asin(null);
                         asin\s
                        ------
                        NULL
                        (1 row)

                        SELECT asin(-2);
                         asin\s
                        ------
                         NaN
                        (1 row)

                        SELECT asin(2);
                         asin\s
                        ------
                         NaN
                        (1 row)

                        SELECT asin(-1);
                         asin\s
                        ------
                         -1.5707963267948966
                        (1 row)

                        SELECT asin(-0.75);
                         asin\s
                        ------
                         -0.848062078981481
                        (1 row)

                        SELECT asin(-0.5);
                         asin\s
                        ------
                         -0.5235987755982989
                        (1 row)

                        SELECT asin(-0.25);
                         asin\s
                        ------
                         -0.25268025514207865
                        (1 row)

                        SELECT asin(0);
                         asin\s
                        ------
                         0
                        (1 row)

                        SELECT asin(0.25);
                         asin\s
                        ------
                         0.25268025514207865
                        (1 row)

                        SELECT asin(0.5);
                         asin\s
                        ------
                         0.5235987755982989
                        (1 row)

                        SELECT asin(0.75);
                         asin\s
                        ------
                         0.848062078981481
                        (1 row)

                        SELECT asin(1);
                         asin\s
                        ------
                         1.5707963267948966
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    // Postgres throws an error if the argument is out of the range: [-1, 1]
    // Right now, we return a NaN, instead of throwing an error
    @Test
    public void testAsinDouble() {
        this.qs(
                """
                        SELECT asin(CAST(-2 AS DOUBLE));
                         asin\s
                        ------
                         NaN
                        (1 row)

                        SELECT asin(CAST(2 AS DOUBLE));
                         asin\s
                        ------
                         NaN
                        (1 row)

                        SELECT asin(CAST(-1 AS DOUBLE));
                         asin\s
                        ------
                         -1.5707963267948966
                        (1 row)

                        SELECT asin(CAST(-0.75 AS DOUBLE));
                         asin\s
                        ------
                         -0.848062078981481
                        (1 row)

                        SELECT asin(CAST(-0.5 AS DOUBLE));
                         asin\s
                        ------
                         -0.5235987755982989
                        (1 row)

                        SELECT asin(CAST(-0.25 AS DOUBLE));
                         asin\s
                        ------
                         -0.25268025514207865
                        (1 row)

                        SELECT asin(CAST(0 AS DOUBLE));
                         asin\s
                        ------
                         0
                        (1 row)

                        SELECT asin(CAST(0.25 AS DOUBLE));
                         asin\s
                        ------
                         0.25268025514207865
                        (1 row)

                        SELECT asin(CAST(0.5 AS DOUBLE));
                         asin\s
                        ------
                         0.5235987755982989
                        (1 row)

                        SELECT asin(CAST(0.75 AS DOUBLE));
                         asin\s
                        ------
                         0.848062078981481
                        (1 row)

                        SELECT asin(CAST(1 AS DOUBLE));
                         asin\s
                        ------
                         1.5707963267948966
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    // Postgres throws an error if the argument is out of the range: [-1, 1]
    // Right now, we return a NaN, instead of throwing an error
    @Test
    public void testAcos() {
        this.qs(
                """
                        SELECT acos(null);
                         acos\s
                        ------
                        NULL
                        (1 row)

                        SELECT acos(-2);
                         acos\s
                        ------
                         NaN
                        (1 row)

                        SELECT acos(2);
                         acos\s
                        ------
                         NaN
                        (1 row)

                        SELECT acos(-1);
                         acos\s
                        ------
                         3.141592653589793
                        (1 row)

                        SELECT acos(-0.75);
                         acos\s
                        ------
                         2.4188584057763776
                        (1 row)

                        SELECT acos(-0.5);
                         acos\s
                        ------
                         2.0943951023931957
                        (1 row)

                        SELECT acos(-0.25);
                         acos\s
                        ------
                         1.8234765819369754
                        (1 row)

                        SELECT acos(0);
                         acos\s
                        ------
                         1.5707963267948966
                        (1 row)

                        SELECT acos(0.25);
                         acos\s
                        ------
                         1.318116071652818
                        (1 row)

                        SELECT acos(0.5);
                         acos\s
                        ------
                         1.0471975511965979
                        (1 row)

                        SELECT acos(0.75);
                         acos\s
                        ------
                         0.7227342478134157
                        (1 row)

                        SELECT acos(1);
                         acos\s
                        ------
                         0
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    // Postgres throws an error if the argument is out of the range: [-1, 1]
    // Right now, we return a NaN, instead of throwing an error
    @Test
    public void testAcosDouble() {
        this.qs(
                """
                        SELECT acos(CAST(-2 AS DOUBLE));
                         acos\s
                        ------
                         NaN
                        (1 row)

                        SELECT acos(CAST(2 AS DOUBLE));
                         acos\s
                        ------
                         NaN
                        (1 row)

                        SELECT acos(CAST(-1 AS DOUBLE));
                         acos\s
                        ------
                         3.141592653589793
                        (1 row)

                        SELECT acos(CAST(-0.75 AS DOUBLE));
                         acos\s
                        ------
                         2.4188584057763776
                        (1 row)

                        SELECT acos(CAST(-0.5 AS DOUBLE));
                         acos\s
                        ------
                         2.0943951023931957
                        (1 row)

                        SELECT acos(CAST(-0.25 AS DOUBLE));
                         acos\s
                        ------
                         1.8234765819369754
                        (1 row)

                        SELECT acos(CAST(0 AS DOUBLE));
                         acos\s
                        ------
                         1.5707963267948966
                        (1 row)

                        SELECT acos(CAST(0.25 AS DOUBLE));
                         acos\s
                        ------
                         1.318116071652818
                        (1 row)

                        SELECT acos(CAST(0.5 AS DOUBLE));
                         acos\s
                        ------
                         1.0471975511965979
                        (1 row)

                        SELECT acos(CAST(0.75 AS DOUBLE));
                         acos\s
                        ------
                         0.7227342478134157
                        (1 row)

                        SELECT acos(CAST(1 AS DOUBLE));
                         acos\s
                        ------
                         0
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testAtan() {
        this.qs(
                """
                        SELECT atan(null);
                         atan\s
                        ------
                        NULL
                        (1 row)

                        SELECT atan(-1);
                         atan\s
                        ------
                         -0.7853981633974483
                        (1 row)

                        SELECT atan(-0.75);
                         atan\s
                        ------
                         -0.6435011087932844
                        (1 row)

                        SELECT atan(-0.5);
                         atan\s
                        ------
                         -0.4636476090008061
                        (1 row)

                        SELECT atan(-0.25);
                         atan\s
                        ------
                         -0.24497866312686414
                        (1 row)

                        SELECT atan(0);
                         atan\s
                        ------
                         0
                        (1 row)

                        SELECT atan(0.25);
                         atan\s
                        ------
                         0.24497866312686414
                        (1 row)

                        SELECT atan(0.5);
                         atan\s
                        ------
                         0.4636476090008061
                        (1 row)

                        SELECT atan(0.75);
                         atan\s
                        ------
                         0.6435011087932844
                        (1 row)

                        SELECT atan(1);
                         atan\s
                        ------
                         0.7853981633974483
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testAtanDouble() {
        this.qs(
                """
                        SELECT atan(CAST(null as DOUBLE));
                         atan\s
                        ------
                        NULL
                        (1 row)

                        SELECT atan(CAST(-1 AS DOUBLE));
                         atan\s
                        ------
                         -0.7853981633974483
                        (1 row)

                        SELECT atan(CAST(-0.75 AS DOUBLE));
                         atan\s
                        ------
                         -0.6435011087932844
                        (1 row)

                        SELECT atan(CAST(-0.5 AS DOUBLE));
                         atan\s
                        ------
                         -0.4636476090008061
                        (1 row)

                        SELECT atan(CAST(-0.25 AS DOUBLE));
                         atan\s
                        ------
                         -0.24497866312686414
                        (1 row)

                        SELECT atan(CAST(0 AS DOUBLE));
                         atan\s
                        ------
                         0
                        (1 row)

                        SELECT atan(CAST(0.25 AS DOUBLE));
                         atan\s
                        ------
                         0.24497866312686414
                        (1 row)

                        SELECT atan(CAST(0.5 AS DOUBLE));
                         atan\s
                        ------
                         0.4636476090008061
                        (1 row)

                        SELECT atan(CAST(0.75 AS DOUBLE));
                         atan\s
                        ------
                         0.6435011087932844
                        (1 row)

                        SELECT atan(CAST(1 AS DOUBLE));
                         atan\s
                        ------
                         0.7853981633974483
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testAtan2() {
        this.qs(
                """
                        SELECT atan2(null, null);
                         atan2\s
                        -------
                        NULL
                        (1 row)

                        SELECT atan2(0, 0);
                         atan2\s
                        -------
                         0
                        (1 row)

                        SELECT atan2((SELECT PI), 0);
                         atan2\s
                        -------
                         1.5707963267948966
                        (1 row)

                        SELECT atan2(0, (SELECT PI));
                         atan2\s
                        -------
                         0
                        (1 row)

                        SELECT atan2(2, (SELECT PI));
                         atan2\s
                        -------
                         0.5669115049410094
                        (1 row)

                        SELECT atan2((SELECT PI), (SELECT PI));
                         atan2\s
                        -------
                         0.7853981633974483
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testAtan2Double() {
        this.qs(
                """
                        SELECT atan2(CAST(0 AS DOUBLE), CAST(0 AS DOUBLE));
                         atan2\s
                        -------
                         0
                        (1 row)

                        SELECT atan2((SELECT PI), CAST(0 AS DOUBLE));
                         atan2\s
                        -------
                         1.5707963267948966
                        (1 row)

                        SELECT atan2(CAST(0 AS DOUBLE), (SELECT PI));
                         atan2\s
                        -------
                         0
                        (1 row)

                        SELECT atan2(CAST(2 AS DOUBLE), (SELECT PI));
                         atan2\s
                        -------
                         0.5669115049410094
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testRadians() {
        this.qs(
                """
                        SELECT radians(null);
                         radians\s
                        ---------
                        NULL
                        (1 row)

                        SELECT radians(0);
                         radians\s
                        ---------
                         0
                        (1 row)

                        SELECT radians(30);
                         radians\s
                        ---------
                         0.5235987755982988
                        (1 row)

                        SELECT radians(45);
                         radians\s
                        ---------
                         0.7853981633974483
                        (1 row)

                        SELECT radians(60);
                         radians\s
                        ---------
                         1.0471975511965976
                        (1 row)

                        SELECT radians(90);
                         radians\s
                        ---------
                         1.5707963267948966
                        (1 row)

                        SELECT radians(120);
                         radians\s
                        ---------
                         2.0943951023931953
                        (1 row)

                        SELECT radians(180);
                         radians\s
                        ---------
                         3.141592653589793
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testRadiansDouble() {
        this.qs(
                """
                        SELECT radians(CAST(null AS DOUBLE));
                         radians\s
                        ---------
                        NULL
                        (1 row)

                        SELECT radians(CAST(0 AS DOUBLE));
                         radians\s
                        ---------
                         0
                        (1 row)

                        SELECT radians(CAST(30 AS DOUBLE));
                         radians\s
                        ---------
                         0.5235987755982988
                        (1 row)

                        SELECT radians(CAST(45 AS DOUBLE));
                         radians\s
                        ---------
                         0.7853981633974483
                        (1 row)

                        SELECT radians(CAST(60 AS DOUBLE));
                         radians\s
                        ---------
                         1.0471975511965976
                        (1 row)

                        SELECT radians(CAST(90 AS DOUBLE));
                         radians\s
                        ---------
                         1.5707963267948966
                        (1 row)

                        SELECT radians(CAST(120 AS DOUBLE));
                         radians\s
                        ---------
                         2.0943951023931953
                        (1 row)

                        SELECT radians(CAST(180 AS DOUBLE));
                         radians\s
                        ---------
                         3.141592653589793
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testDegrees() {
        this.qs(
                """
                        SELECT degrees(null);
                         degrees\s
                        ---------
                        NULL
                        (1 row)

                        SELECT degrees(0);
                         degrees\s
                        ---------
                         0
                        (1 row)

                        SELECT degrees((SELECT PI / 6));
                         degrees\s
                        ---------
                         29.999999999999996
                        (1 row)

                        SELECT degrees((SELECT PI / 4));
                         degrees\s
                        ---------
                         45
                        (1 row)

                        SELECT degrees((SELECT PI / 3));
                         degrees\s
                        ---------
                         59.99999999999999
                        (1 row)

                        SELECT degrees((SELECT PI / 2));
                         degrees\s
                        ---------
                         90
                        (1 row)

                        SELECT degrees((SELECT PI));
                         degrees\s
                        ---------
                         180
                        (1 row)

                        SELECT degrees((SELECT PI * 2));
                         degrees\s
                        ---------
                         360
                        (1 row)"""
        );
    }

    // Tested on Postgres
    @Test
    public void validInputs() {
        this.qs(
                """
                        SELECT sin('-3');
                         sin\s
                        -----
                         -0.1411200080598672
                        (1 row)

                        SELECT sin(CAST(-3.0 AS DECIMAL(3, 2)));
                         sin\s
                        -----
                         -0.1411200080598672
                        (1 row)

                        SELECT sin(CAST(-3 AS TINYINT));
                         sin\s
                        -----
                         -0.1411200080598672
                        (1 row)

                        SELECT sin(CAST(-3 AS BIGINT));
                         sin\s
                        -----
                         -0.1411200080598672
                        (1 row)"""
        );
    }

    @Test
    public void invalidInputs() {
        this.shouldFail(
                "SELECT sin(CAST('2023-12-14' AS DATE))",
                "Error in SQL statement: Cannot apply 'SIN' to arguments of type 'SIN(<DATE>)'. Supported form(s): 'SIN(<NUMERIC>)'"
        );

        this.shouldFail("SELECT sin(true)",
                "Error in SQL statement: Cannot apply 'SIN' to arguments of type 'SIN(<BOOLEAN>)'. Supported form(s): 'SIN(<NUMERIC>)'"
        );

        this.shouldFail("SELECT sin(CAST('101' AS BINARY(3)))",
                "Error in SQL statement: Cannot apply 'SIN' to arguments of type 'SIN(<BINARY(3)>)'. Supported form(s): 'SIN(<NUMERIC>)'"
        );

        this.shouldFail("SELECT sin(CAST('15:06:51.06731' AS TIME))",
                "Error in SQL statement: Cannot apply 'SIN' to arguments of type 'SIN(<TIME(0)>)'. Supported form(s): 'SIN(<NUMERIC>)'"
        );
    }
}
