package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

public class TrigonometryTests extends SqlIoTest {
    // Tested using Postgres 15.2
    @Test
    public void testSin() {
        // Postgres disagrees with us here, but currently we round off to 15 decimal places
        this.qs(
                """
                        SELECT sin(null);
                         sin
                        -----
                        NULL
                        (1 row)

                        SELECT sin(0);
                         sin
                        -----
                         0
                        (1 row)

                        SELECT sin(0.25);
                         sin
                        -----
                         0.24740395925452294
                        (1 row)

                        SELECT sin(0.5);
                         sin
                        -----
                         0.479425538604203
                        (1 row)

                        SELECT sin(0.75);
                         sin
                        -----
                         0.6816387600233341
                        (1 row)

                        SELECT sin(1);
                         sin
                        -----
                         0.8414709848078965
                        (1 row)

                        SELECT sin(pi);
                         sin
                        -----
                         0
                        (1 row)"""
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testSinDouble() {
        this.qs(
                """
                        SELECT sin(CAST(0 AS DOUBLE));
                         sin
                        -----
                         0
                        (1 row)

                        SELECT sin(CAST(0.25 AS DOUBLE));
                         sin
                        -----
                         0.24740395925452294
                        (1 row)

                        SELECT sin(CAST(0.5 AS DOUBLE));
                         sin
                        -----
                         0.479425538604203
                        (1 row)

                        SELECT sin(CAST(0.75 AS DOUBLE));
                         sin
                        -----
                         0.6816387600233341
                        (1 row)

                        SELECT sin(CAST(1 AS DOUBLE));
                         sin
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
                         cos
                        -----
                        NULL
                        (1 row)

                        SELECT cos(0);
                         cos
                        -----
                         1
                        (1 row)

                        SELECT cos(0.25);
                         cos
                        -----
                         0.9689124217106447
                        (1 row)

                        SELECT cos(0.5);
                         cos
                        -----
                         0.8775825618903728
                        (1 row)

                        SELECT cos(0.75);
                         cos
                        -----
                         0.7316888688738209
                        (1 row)

                        SELECT cos(1);
                         cos
                        -----
                         0.5403023058681398
                        (1 row)

                        SELECT cos(pi);
                         cos
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
                         cos
                        -----
                         1
                        (1 row)

                        SELECT cos(CAST(0.25 AS DOUBLE));
                         cos
                        -----
                         0.9689124217106447
                        (1 row)

                        SELECT cos(CAST(0.5 AS DOUBLE));
                         cos
                        -----
                         0.8775825618903728
                        (1 row)

                        SELECT cos(CAST(0.75 AS DOUBLE));
                         cos
                        -----
                         0.7316888688738209
                        (1 row)

                        SELECT cos(CAST(1 AS DOUBLE));
                         cos
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
                         pi
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
                         tan
                        -----
                        NULL
                        (1 row)

                        SELECT tan(0);
                         tan
                        -----
                         0
                        (1 row)

                        SELECT tan(0.25);
                         tan
                        -----
                         0.25534192122103627
                        (1 row)

                        SELECT tan(0.5);
                         tan
                        -----
                         0.5463024898437905
                        (1 row)

                        SELECT tan(0.75);
                         tan
                        -----
                         0.9315964599440725
                        (1 row)

                        SELECT tan(1);
                         tan
                        -----
                         1.5574077246549023
                        (1 row)

                        SELECT tan(pi);
                         tan
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
                         tan
                        -----
                        NULL
                        (1 row)

                        SELECT tan(CAST(0 AS DOUBLE));
                         tan
                        -----
                         0
                        (1 row)

                        SELECT tan(CAST(0.25 AS DOUBLE));
                         tan
                        -----
                         0.25534192122103627
                        (1 row)

                        SELECT tan(CAST(0.5 AS DOUBLE));
                         tan
                        -----
                         0.5463024898437905
                        (1 row)

                        SELECT tan(CAST(0.75 AS DOUBLE));
                         tan
                        -----
                         0.9315964599440725
                        (1 row)

                        SELECT tan(CAST(1 AS DOUBLE));
                         tan
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
                         cot
                        -----
                         Infinity
                        (1 row)

                        SELECT cot(pi);
                         cot
                        -----
                         -8.165619676597685e+15
                        (1 row)

                        SELECT cot(null);
                         cot
                        -----
                        NULL
                        (1 row)

                        SELECT cot(0.25);
                         cot
                        -----
                         3.91631736464594
                        (1 row)

                        SELECT cot(0.5);
                         cot
                        -----
                         1.830487721712452
                        (1 row)

                        SELECT cot(0.75);
                         cot
                        -----
                         1.0734261485493772
                        (1 row)

                        SELECT cot(1);
                         cot
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
                         cot
                        -----
                         Infinity
                        (1 row)

                        SELECT cot(pi);
                         cot
                        -----
                         -8.165619676597685e+15
                        (1 row)

                        SELECT cot(CAST(null AS DOUBLE));
                         cot
                        -----
                        NULL
                        (1 row)

                        SELECT cot(CAST(0.25 AS DOUBLE));
                         cot
                        -----
                         3.91631736464594
                        (1 row)

                        SELECT cot(CAST(0.5 AS DOUBLE));
                         cot
                        -----
                         1.830487721712452
                        (1 row)

                        SELECT cot(CAST(0.75 AS DOUBLE));
                         cot
                        -----
                         1.0734261485493772
                        (1 row)

                        SELECT cot(CAST(1 AS DOUBLE));
                         cot
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
                         asin
                        ------
                        NULL
                        (1 row)

                        SELECT asin(-2);
                         asin
                        ------
                         NaN
                        (1 row)

                        SELECT asin(2);
                         asin
                        ------
                         NaN
                        (1 row)

                        SELECT asin(-1);
                         asin
                        ------
                         -1.5707963267948966
                        (1 row)

                        SELECT asin(-0.75);
                         asin
                        ------
                         -0.848062078981481
                        (1 row)

                        SELECT asin(-0.5);
                         asin
                        ------
                         -0.5235987755982989
                        (1 row)

                        SELECT asin(-0.25);
                         asin
                        ------
                         -0.25268025514207865
                        (1 row)

                        SELECT asin(0);
                         asin
                        ------
                         0
                        (1 row)

                        SELECT asin(0.25);
                         asin
                        ------
                         0.25268025514207865
                        (1 row)

                        SELECT asin(0.5);
                         asin
                        ------
                         0.5235987755982989
                        (1 row)

                        SELECT asin(0.75);
                         asin
                        ------
                         0.848062078981481
                        (1 row)

                        SELECT asin(1);
                         asin
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
                         asin
                        ------
                         NaN
                        (1 row)

                        SELECT asin(CAST(2 AS DOUBLE));
                         asin
                        ------
                         NaN
                        (1 row)

                        SELECT asin(CAST(-1 AS DOUBLE));
                         asin
                        ------
                         -1.5707963267948966
                        (1 row)

                        SELECT asin(CAST(-0.75 AS DOUBLE));
                         asin
                        ------
                         -0.848062078981481
                        (1 row)

                        SELECT asin(CAST(-0.5 AS DOUBLE));
                         asin
                        ------
                         -0.5235987755982989
                        (1 row)

                        SELECT asin(CAST(-0.25 AS DOUBLE));
                         asin
                        ------
                         -0.25268025514207865
                        (1 row)

                        SELECT asin(CAST(0 AS DOUBLE));
                         asin
                        ------
                         0
                        (1 row)

                        SELECT asin(CAST(0.25 AS DOUBLE));
                         asin
                        ------
                         0.25268025514207865
                        (1 row)

                        SELECT asin(CAST(0.5 AS DOUBLE));
                         asin
                        ------
                         0.5235987755982989
                        (1 row)

                        SELECT asin(CAST(0.75 AS DOUBLE));
                         asin
                        ------
                         0.848062078981481
                        (1 row)

                        SELECT asin(CAST(1 AS DOUBLE));
                         asin
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
                         acos
                        ------
                        NULL
                        (1 row)

                        SELECT acos(-2);
                         acos
                        ------
                         NaN
                        (1 row)

                        SELECT acos(2);
                         acos
                        ------
                         NaN
                        (1 row)

                        SELECT acos(-1);
                         acos
                        ------
                         3.141592653589793
                        (1 row)

                        SELECT acos(-0.75);
                         acos
                        ------
                         2.4188584057763776
                        (1 row)

                        SELECT acos(-0.5);
                         acos
                        ------
                         2.0943951023931957
                        (1 row)

                        SELECT acos(-0.25);
                         acos
                        ------
                         1.8234765819369754
                        (1 row)

                        SELECT acos(0);
                         acos
                        ------
                         1.5707963267948966
                        (1 row)

                        SELECT acos(0.25);
                         acos
                        ------
                         1.318116071652818
                        (1 row)

                        SELECT acos(0.5);
                         acos
                        ------
                         1.0471975511965979
                        (1 row)

                        SELECT acos(0.75);
                         acos
                        ------
                         0.7227342478134157
                        (1 row)

                        SELECT acos(1);
                         acos
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
                         acos
                        ------
                         NaN
                        (1 row)

                        SELECT acos(CAST(2 AS DOUBLE));
                         acos
                        ------
                         NaN
                        (1 row)

                        SELECT acos(CAST(-1 AS DOUBLE));
                         acos
                        ------
                         3.141592653589793
                        (1 row)

                        SELECT acos(CAST(-0.75 AS DOUBLE));
                         acos
                        ------
                         2.4188584057763776
                        (1 row)

                        SELECT acos(CAST(-0.5 AS DOUBLE));
                         acos
                        ------
                         2.0943951023931957
                        (1 row)

                        SELECT acos(CAST(-0.25 AS DOUBLE));
                         acos
                        ------
                         1.8234765819369754
                        (1 row)

                        SELECT acos(CAST(0 AS DOUBLE));
                         acos
                        ------
                         1.5707963267948966
                        (1 row)

                        SELECT acos(CAST(0.25 AS DOUBLE));
                         acos
                        ------
                         1.318116071652818
                        (1 row)

                        SELECT acos(CAST(0.5 AS DOUBLE));
                         acos
                        ------
                         1.0471975511965979
                        (1 row)

                        SELECT acos(CAST(0.75 AS DOUBLE));
                         acos
                        ------
                         0.7227342478134157
                        (1 row)

                        SELECT acos(CAST(1 AS DOUBLE));
                         acos
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
                         atan
                        ------
                        NULL
                        (1 row)

                        SELECT atan(-1);
                         atan
                        ------
                         -0.7853981633974483
                        (1 row)

                        SELECT atan(-0.75);
                         atan
                        ------
                         -0.6435011087932844
                        (1 row)

                        SELECT atan(-0.5);
                         atan
                        ------
                         -0.4636476090008061
                        (1 row)

                        SELECT atan(-0.25);
                         atan
                        ------
                         -0.24497866312686414
                        (1 row)

                        SELECT atan(0);
                         atan
                        ------
                         0
                        (1 row)

                        SELECT atan(0.25);
                         atan
                        ------
                         0.24497866312686414
                        (1 row)

                        SELECT atan(0.5);
                         atan
                        ------
                         0.4636476090008061
                        (1 row)

                        SELECT atan(0.75);
                         atan
                        ------
                         0.6435011087932844
                        (1 row)

                        SELECT atan(1);
                         atan
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
                         atan
                        ------
                        NULL
                        (1 row)

                        SELECT atan(CAST(-1 AS DOUBLE));
                         atan
                        ------
                         -0.7853981633974483
                        (1 row)

                        SELECT atan(CAST(-0.75 AS DOUBLE));
                         atan
                        ------
                         -0.6435011087932844
                        (1 row)

                        SELECT atan(CAST(-0.5 AS DOUBLE));
                         atan
                        ------
                         -0.4636476090008061
                        (1 row)

                        SELECT atan(CAST(-0.25 AS DOUBLE));
                         atan
                        ------
                         -0.24497866312686414
                        (1 row)

                        SELECT atan(CAST(0 AS DOUBLE));
                         atan
                        ------
                         0
                        (1 row)

                        SELECT atan(CAST(0.25 AS DOUBLE));
                         atan
                        ------
                         0.24497866312686414
                        (1 row)

                        SELECT atan(CAST(0.5 AS DOUBLE));
                         atan
                        ------
                         0.4636476090008061
                        (1 row)

                        SELECT atan(CAST(0.75 AS DOUBLE));
                         atan
                        ------
                         0.6435011087932844
                        (1 row)

                        SELECT atan(CAST(1 AS DOUBLE));
                         atan
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
                         atan2
                        -------
                        NULL
                        (1 row)

                        SELECT atan2(0, 0);
                         atan2
                        -------
                         0
                        (1 row)

                        SELECT atan2((SELECT PI), 0);
                         atan2
                        -------
                         1.5707963267948966
                        (1 row)

                        SELECT atan2(0, (SELECT PI));
                         atan2
                        -------
                         0
                        (1 row)

                        SELECT atan2(2, (SELECT PI));
                         atan2
                        -------
                         0.5669115049410094
                        (1 row)

                        SELECT atan2((SELECT PI), (SELECT PI));
                         atan2
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
                         atan2
                        -------
                         0
                        (1 row)

                        SELECT atan2((SELECT PI), CAST(0 AS DOUBLE));
                         atan2
                        -------
                         1.5707963267948966
                        (1 row)

                        SELECT atan2(CAST(0 AS DOUBLE), (SELECT PI));
                         atan2
                        -------
                         0
                        (1 row)

                        SELECT atan2(CAST(2 AS DOUBLE), (SELECT PI));
                         atan2
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
                         radians
                        ---------
                        NULL
                        (1 row)

                        SELECT radians(0);
                         radians
                        ---------
                         0
                        (1 row)

                        SELECT radians(30);
                         radians
                        ---------
                         0.5235987755982988
                        (1 row)

                        SELECT radians(45);
                         radians
                        ---------
                         0.7853981633974483
                        (1 row)

                        SELECT radians(60);
                         radians
                        ---------
                         1.0471975511965976
                        (1 row)

                        SELECT radians(90);
                         radians
                        ---------
                         1.5707963267948966
                        (1 row)

                        SELECT radians(120);
                         radians
                        ---------
                         2.0943951023931953
                        (1 row)

                        SELECT radians(180);
                         radians
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
                         radians
                        ---------
                        NULL
                        (1 row)

                        SELECT radians(CAST(0 AS DOUBLE));
                         radians
                        ---------
                         0
                        (1 row)

                        SELECT radians(CAST(30 AS DOUBLE));
                         radians
                        ---------
                         0.5235987755982988
                        (1 row)

                        SELECT radians(CAST(45 AS DOUBLE));
                         radians
                        ---------
                         0.7853981633974483
                        (1 row)

                        SELECT radians(CAST(60 AS DOUBLE));
                         radians
                        ---------
                         1.0471975511965976
                        (1 row)

                        SELECT radians(CAST(90 AS DOUBLE));
                         radians
                        ---------
                         1.5707963267948966
                        (1 row)

                        SELECT radians(CAST(120 AS DOUBLE));
                         radians
                        ---------
                         2.0943951023931953
                        (1 row)

                        SELECT radians(CAST(180 AS DOUBLE));
                         radians
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
                         degrees
                        ---------
                        NULL
                        (1 row)

                        SELECT degrees(0);
                         degrees
                        ---------
                         0
                        (1 row)

                        SELECT degrees((SELECT PI / 6));
                         degrees
                        ---------
                         29.999999999999996
                        (1 row)

                        SELECT degrees(PI / 4);
                         degrees
                        ---------
                         45
                        (1 row)

                        SELECT degrees(PI / 3);
                         degrees
                        ---------
                         59.99999999999999
                        (1 row)

                        SELECT degrees(PI / 2);
                         degrees
                        ---------
                         90
                        (1 row)

                        SELECT degrees(PI);
                         degrees
                        ---------
                         180
                        (1 row)

                        SELECT degrees(PI * 2);
                         degrees
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
                         sin
                        -----
                         -0.1411200080598672
                        (1 row)

                        SELECT sin(CAST(-3.0 AS DECIMAL(3, 2)));
                         sin
                        -----
                         -0.1411200080598672
                        (1 row)

                        SELECT sin(CAST(-3 AS TINYINT));
                         sin
                        -----
                         -0.1411200080598672
                        (1 row)

                        SELECT sin(CAST(-3 AS BIGINT));
                         sin
                        -----
                         -0.1411200080598672
                        (1 row)"""
        );
    }

    @Test
    public void invalidInputs() {
        this.shouldFail(
                "SELECT sin(CAST('2023-12-14' AS DATE))",
                "Error in SQL statement: Cannot apply 'SIN' to arguments of type 'SIN(<DATE>)'. " +
                        "Supported form(s): 'SIN(<NUMERIC>)'"
        );

        this.shouldFail("SELECT sin(true)",
                "Error in SQL statement: Cannot apply 'SIN' to arguments of type 'SIN(<BOOLEAN>)'. " +
                        "Supported form(s): 'SIN(<NUMERIC>)'"
        );

        this.shouldFail("SELECT sin(CAST('101' AS BINARY(3)))",
                "Error in SQL statement: Cannot apply 'SIN' to arguments of type 'SIN(<BINARY(3)>)'. " +
                        "Supported form(s): 'SIN(<NUMERIC>)'"
        );

        this.shouldFail("SELECT sin(CAST('15:06:51.06731' AS TIME))",
                "Error in SQL statement: Cannot apply 'SIN' to arguments of type 'SIN(<TIME(0)>)'. " +
                        "Supported form(s): 'SIN(<NUMERIC>)'"
        );
    }
}
