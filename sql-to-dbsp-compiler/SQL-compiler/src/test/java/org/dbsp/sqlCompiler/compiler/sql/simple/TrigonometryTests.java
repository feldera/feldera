package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
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
                "SELECT sin(CAST(0 AS DOUBLE));\n" +
                        " sin \n" +
                        "-----\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT sin(CAST(0.25 AS DOUBLE));\n" +
                        " sin \n" +
                        "-----\n" +
                        " 0.24740395925452294\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT sin(CAST(0.5 AS DOUBLE));\n" +
                        " sin \n" +
                        "-----\n" +
                        " 0.479425538604203\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT sin(CAST(0.75 AS DOUBLE));\n" +
                        " sin \n" +
                        "-----\n" +
                        " 0.6816387600233341\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT sin(CAST(1 AS DOUBLE));\n" +
                        " sin \n" +
                        "-----\n" +
                        " 0.8414709848078965\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testCos() {
        this.qs(
        "SELECT cos(null);\n" +
                " cos \n" +
                "-----\n" +
                "NULL\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT cos(0);\n" +
                " cos \n" +
                "-----\n" +
                " 1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT cos(0.25);\n" +
                " cos \n" +
                "-----\n" +
                " 0.9689124217106447\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT cos(0.5);\n" +
                " cos \n" +
                "-----\n" +
                " 0.8775825618903728\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT cos(0.75);\n" +
                " cos \n" +
                "-----\n" +
                " 0.7316888688738209\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT cos(1);\n" +
                " cos \n" +
                "-----\n" +
                " 0.5403023058681398\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT cos(pi);\n" +
                " cos \n" +
                "-----\n" +
                " -1\n" +
                "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testCosDouble() {
        this.qs(
                "SELECT cos(CAST(0 AS DOUBLE));\n" +
                        " cos \n" +
                        "-----\n" +
                        " 1\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cos(CAST(0.25 AS DOUBLE));\n" +
                        " cos \n" +
                        "-----\n" +
                        " 0.9689124217106447\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cos(CAST(0.5 AS DOUBLE));\n" +
                        " cos \n" +
                        "-----\n" +
                        " 0.8775825618903728\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cos(CAST(0.75 AS DOUBLE));\n" +
                        " cos \n" +
                        "-----\n" +
                        " 0.7316888688738209\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cos(CAST(1 AS DOUBLE));\n" +
                        " cos \n" +
                        "-----\n" +
                        " 0.5403023058681398\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testPi() {
        this.q(
            "SELECT PI;\n" +
            " pi \n" +
            "----\n" +
            " 3.141592653589793"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testTan() {
        this.qs(
                "SELECT tan(null);\n" +
                        " tan \n" +
                        "-----\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(0);\n" +
                        " tan \n" +
                        "-----\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(0.25);\n" +
                        " tan \n" +
                        "-----\n" +
                        " 0.25534192122103627\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(0.5);\n" +
                        " tan \n" +
                        "-----\n" +
                        " 0.5463024898437905\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(0.75);\n" +
                        " tan \n" +
                        "-----\n" +
                        " 0.9315964599440725\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(1);\n" +
                        " tan \n" +
                        "-----\n" +
                        " 1.5574077246549023\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(pi);\n" +
                        " tan \n" +
                        "-----\n" +
                        " -1.2246467991473532e-16\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testTanDouble() {
        this.qs(
                "SELECT tan(CAST(null as DOUBLE));\n" +
                        " tan \n" +
                        "-----\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(CAST(0 AS DOUBLE));\n" +
                        " tan \n" +
                        "-----\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(CAST(0.25 AS DOUBLE));\n" +
                        " tan \n" +
                        "-----\n" +
                        " 0.25534192122103627\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(CAST(0.5 AS DOUBLE));\n" +
                        " tan \n" +
                        "-----\n" +
                        " 0.5463024898437905\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(CAST(0.75 AS DOUBLE));\n" +
                        " tan \n" +
                        "-----\n" +
                        " 0.9315964599440725\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT tan(CAST(1 AS DOUBLE));\n" +
                        " tan \n" +
                        "-----\n" +
                        " 1.5574077246549023\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testCot() {
        this.qs(
                        "SELECT cot(0);\n" +
                        " cot \n" +
                        "-----\n" +
                        " Infinity\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(pi);\n" +
                        " cot \n" +
                        "-----\n" +
                        " -8.165619676597685e+15\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(null);\n" +
                        " cot \n" +
                        "-----\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(0.25);\n" +
                        " cot \n" +
                        "-----\n" +
                        " 3.91631736464594\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(0.5);\n" +
                        " cot \n" +
                        "-----\n" +
                        " 1.830487721712452\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(0.75);\n" +
                        " cot \n" +
                        "-----\n" +
                        " 1.0734261485493772\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(1);\n" +
                        " cot \n" +
                        "-----\n" +
                        " 0.6420926159343306\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testCotDouble() {
        this.qs(
                        "SELECT cot(CAST(0 AS DOUBLE));\n" +
                        " cot \n" +
                        "-----\n" +
                        " Infinity\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(pi);\n" +
                        " cot \n" +
                        "-----\n" +
                        " -8.165619676597685e+15\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(CAST(null AS DOUBLE));\n" +
                        " cot \n" +
                        "-----\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(CAST(0.25 AS DOUBLE));\n" +
                        " cot \n" +
                        "-----\n" +
                        " 3.91631736464594\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(CAST(0.5 AS DOUBLE));\n" +
                        " cot \n" +
                        "-----\n" +
                        " 1.830487721712452\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(CAST(0.75 AS DOUBLE));\n" +
                        " cot \n" +
                        "-----\n" +
                        " 1.0734261485493772\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cot(CAST(1 AS DOUBLE));\n" +
                        " cot \n" +
                        "-----\n" +
                        " 0.6420926159343306\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    // Postgres throws an error if the argument is out of the range: [-1, 1]
    // Right now, we return a NaN, instead of throwing an error
    @Test
    public void testAsin() {
        this.qs(
                "SELECT asin(null);\n" +
                        " asin \n" +
                        "------\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(-2);\n" +
                        " asin \n" +
                        "------\n" +
                        " NaN\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(2);\n" +
                        " asin \n" +
                        "------\n" +
                        " NaN\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(-1);\n" +
                        " asin \n" +
                        "------\n" +
                        " -1.5707963267948966\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(-0.75);\n" +
                        " asin \n" +
                        "------\n" +
                        " -0.848062078981481\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(-0.5);\n" +
                        " asin \n" +
                        "------\n" +
                        " -0.5235987755982989\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(-0.25);\n" +
                        " asin \n" +
                        "------\n" +
                        " -0.25268025514207865\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(0);\n" +
                        " asin \n" +
                        "------\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(0.25);\n" +
                        " asin \n" +
                        "------\n" +
                        " 0.25268025514207865\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(0.5);\n" +
                        " asin \n" +
                        "------\n" +
                        " 0.5235987755982989\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(0.75);\n" +
                        " asin \n" +
                        "------\n" +
                        " 0.848062078981481\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(1);\n" +
                        " asin \n" +
                        "------\n" +
                        " 1.5707963267948966\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    // Postgres throws an error if the argument is out of the range: [-1, 1]
    // Right now, we return a NaN, instead of throwing an error
    @Test
    public void testAsinDouble() {
        this.qs(
                "SELECT asin(CAST(-2 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " NaN\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(2 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " NaN\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(-1 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " -1.5707963267948966\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(-0.75 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " -0.848062078981481\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(-0.5 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " -0.5235987755982989\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(-0.25 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " -0.25268025514207865\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(0 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(0.25 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " 0.25268025514207865\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(0.5 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " 0.5235987755982989\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(0.75 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " 0.848062078981481\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT asin(CAST(1 AS DOUBLE));\n" +
                        " asin \n" +
                        "------\n" +
                        " 1.5707963267948966\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    // Postgres throws an error if the argument is out of the range: [-1, 1]
    // Right now, we return a NaN, instead of throwing an error
    @Test
    public void testAcos() {
        this.qs(
                "SELECT acos(null);\n" +
                        " acos \n" +
                        "------\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(-2);\n" +
                        " acos \n" +
                        "------\n" +
                        " NaN\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(2);\n" +
                        " acos \n" +
                        "------\n" +
                        " NaN\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(-1);\n" +
                        " acos \n" +
                        "------\n" +
                        " 3.141592653589793\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(-0.75);\n" +
                        " acos \n" +
                        "------\n" +
                        " 2.4188584057763776\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(-0.5);\n" +
                        " acos \n" +
                        "------\n" +
                        " 2.0943951023931957\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(-0.25);\n" +
                        " acos \n" +
                        "------\n" +
                        " 1.8234765819369754\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(0);\n" +
                        " acos \n" +
                        "------\n" +
                        " 1.5707963267948966\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(0.25);\n" +
                        " acos \n" +
                        "------\n" +
                        " 1.318116071652818\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(0.5);\n" +
                        " acos \n" +
                        "------\n" +
                        " 1.0471975511965979\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(0.75);\n" +
                        " acos \n" +
                        "------\n" +
                        " 0.7227342478134157\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(1);\n" +
                        " acos \n" +
                        "------\n" +
                        " 0\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    // Postgres throws an error if the argument is out of the range: [-1, 1]
    // Right now, we return a NaN, instead of throwing an error
    @Test
    public void testAcosDouble() {
        this.qs(
                "SELECT acos(CAST(-2 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " NaN\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(2 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " NaN\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(-1 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " 3.141592653589793\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(-0.75 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " 2.4188584057763776\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(-0.5 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " 2.0943951023931957\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(-0.25 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " 1.8234765819369754\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(0 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " 1.5707963267948966\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(0.25 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " 1.318116071652818\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(0.5 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " 1.0471975511965979\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(0.75 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " 0.7227342478134157\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT acos(CAST(1 AS DOUBLE));\n" +
                        " acos \n" +
                        "------\n" +
                        " 0\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testAtan() {
        this.qs(
                "SELECT atan(null);\n" +
                        " atan \n" +
                        "------\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(-1);\n" +
                        " atan \n" +
                        "------\n" +
                        " -0.7853981633974483\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(-0.75);\n" +
                        " atan \n" +
                        "------\n" +
                        " -0.6435011087932844\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(-0.5);\n" +
                        " atan \n" +
                        "------\n" +
                        " -0.4636476090008061\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(-0.25);\n" +
                        " atan \n" +
                        "------\n" +
                        " -0.24497866312686414\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(0);\n" +
                        " atan \n" +
                        "------\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(0.25);\n" +
                        " atan \n" +
                        "------\n" +
                        " 0.24497866312686414\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(0.5);\n" +
                        " atan \n" +
                        "------\n" +
                        " 0.4636476090008061\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(0.75);\n" +
                        " atan \n" +
                        "------\n" +
                        " 0.6435011087932844\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(1);\n" +
                        " atan \n" +
                        "------\n" +
                        " 0.7853981633974483\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testAtanDouble() {
        this.qs(
                "SELECT atan(CAST(null as DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(CAST(-1 AS DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        " -0.7853981633974483\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(CAST(-0.75 AS DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        " -0.6435011087932844\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(CAST(-0.5 AS DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        " -0.4636476090008061\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(CAST(-0.25 AS DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        " -0.24497866312686414\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(CAST(0 AS DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(CAST(0.25 AS DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        " 0.24497866312686414\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(CAST(0.5 AS DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        " 0.4636476090008061\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(CAST(0.75 AS DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        " 0.6435011087932844\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT atan(CAST(1 AS DOUBLE));\n" +
                        " atan \n" +
                        "------\n" +
                        " 0.7853981633974483\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testAtan2() {
        this.qs(
                "SELECT atan2(null, null);\n" +
                " atan2 \n" +
                "-------\n" +
                "NULL\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT atan2(0, 0);\n" +
                " atan2 \n" +
                "-------\n" +
                " 0\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT atan2((SELECT PI), 0);\n" +
                " atan2 \n" +
                "-------\n" +
                " 1.5707963267948966\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT atan2(0, (SELECT PI));\n" +
                " atan2 \n" +
                "-------\n" +
                " 0\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT atan2(2, (SELECT PI));\n" +
                " atan2 \n" +
                "-------\n" +
                " 0.5669115049410094\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT atan2((SELECT PI), (SELECT PI));\n" +
                " atan2 \n" +
                "-------\n" +
                " 0.7853981633974483\n" +
                "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testAtan2Double() {
        this.qs(
        "SELECT atan2(CAST(0 AS DOUBLE), CAST(0 AS DOUBLE));\n" +
                " atan2 \n" +
                "-------\n" +
                " 0\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT atan2((SELECT PI), CAST(0 AS DOUBLE));\n" +
                " atan2 \n" +
                "-------\n" +
                " 1.5707963267948966\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT atan2(CAST(0 AS DOUBLE), (SELECT PI));\n" +
                " atan2 \n" +
                "-------\n" +
                " 0\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT atan2(CAST(2 AS DOUBLE), (SELECT PI));\n" +
                " atan2 \n" +
                "-------\n" +
                " 0.5669115049410094\n" +
                "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testRadians() {
        this.qs(
                "SELECT radians(null);\n" +
                        " radians \n" +
                        "---------\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(0);\n" +
                        " radians \n" +
                        "---------\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(30);\n" +
                        " radians \n" +
                        "---------\n" +
                        " 0.5235987755982988\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(45);\n" +
                        " radians \n" +
                        "---------\n" +
                        " 0.7853981633974483\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(60);\n" +
                        " radians \n" +
                        "---------\n" +
                        " 1.0471975511965976\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(90);\n" +
                        " radians \n" +
                        "---------\n" +
                        " 1.5707963267948966\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(120);\n" +
                        " radians \n" +
                        "---------\n" +
                        " 2.0943951023931953\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(180);\n" +
                        " radians \n" +
                        "---------\n" +
                        " 3.141592653589793\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testRadiansDouble() {
        this.qs(
                "SELECT radians(CAST(null AS DOUBLE));\n" +
                        " radians \n" +
                        "---------\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(CAST(0 AS DOUBLE));\n" +
                        " radians \n" +
                        "---------\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(CAST(30 AS DOUBLE));\n" +
                        " radians \n" +
                        "---------\n" +
                        " 0.5235987755982988\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(CAST(45 AS DOUBLE));\n" +
                        " radians \n" +
                        "---------\n" +
                        " 0.7853981633974483\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(CAST(60 AS DOUBLE));\n" +
                        " radians \n" +
                        "---------\n" +
                        " 1.0471975511965976\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(CAST(90 AS DOUBLE));\n" +
                        " radians \n" +
                        "---------\n" +
                        " 1.5707963267948966\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(CAST(120 AS DOUBLE));\n" +
                        " radians \n" +
                        "---------\n" +
                        " 2.0943951023931953\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT radians(CAST(180 AS DOUBLE));\n" +
                        " radians \n" +
                        "---------\n" +
                        " 3.141592653589793\n" +
                        "(1 row)"
        );
    }

    // Tested using Postgres 15.2
    @Test
    public void testDegrees() {
        this.qs(
                "SELECT degrees(null);\n" +
                        " degrees \n" +
                        "---------\n" +
                        "NULL\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT degrees(0);\n" +
                        " degrees \n" +
                        "---------\n" +
                        " 0\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT degrees((SELECT PI / 6));\n" +
                        " degrees \n" +
                        "---------\n" +
                        " 29.999999999999996\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT degrees((SELECT PI / 4));\n" +
                        " degrees \n" +
                        "---------\n" +
                        " 45\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT degrees((SELECT PI / 3));\n" +
                        " degrees \n" +
                        "---------\n" +
                        " 59.99999999999999\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT degrees((SELECT PI / 2));\n" +
                        " degrees \n" +
                        "---------\n" +
                        " 90\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT degrees((SELECT PI));\n" +
                        " degrees \n" +
                        "---------\n" +
                        " 180\n" +
                        "(1 row)\n" +
                        "\n" +
                        "SELECT degrees((SELECT PI * 2));\n" +
                        " degrees \n" +
                        "---------\n" +
                        " 360\n" +
                        "(1 row)"
        );
    }

    // Tested on Postgres
    @Test
    public void validInputs() {
        this.qs(
                "SELECT sin('-3');\n" +
                        " sin \n" +
                        "-----\n" +
                        " -0.1411200080598672\n" +
                        "(1 row)\n" +
                        "\n" +
                "SELECT sin(CAST(-3.0 AS DECIMAL(3, 2)));\n" +
                        " sin \n" +
                        "-----\n" +
                        " -0.1411200080598672\n" +
                        "(1 row)\n" +
                        "\n" +
                "SELECT sin(CAST(-3 AS TINYINT));\n" +
                        " sin \n" +
                        "-----\n" +
                        " -0.1411200080598672\n" +
                        "(1 row)\n" +
                        "\n" +
                "SELECT sin(CAST(-3 AS BIGINT));\n" +
                        " sin \n" +
                        "-----\n" +
                        " -0.1411200080598672\n" +
                        "(1 row)"
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


    // This test fails right now
    // When sin casts to Double, 'test' gets turned into 0 instead of returning an error
    @Test @Ignore
    public void testStringAsInput() {
        this.shouldFail("SELECT sin('test')",
                "Cannot apply 'SIN' to arguments of type 'SIN(<String>)'."
        );
    }
}
