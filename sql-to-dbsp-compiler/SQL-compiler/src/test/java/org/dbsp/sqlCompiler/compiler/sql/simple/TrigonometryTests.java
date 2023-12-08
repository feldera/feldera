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
                        "(1 row)\n" +
                        "\n" +
                        "SELECT sin(CAST(pi AS DOUBLE));\n" +
                        " sin \n" +
                        "-----\n" +
                        " 0\n" +
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
                        "(1 row)\n" +
                        "\n" +
                        "SELECT cos(CAST(pi AS DOUBLE));\n" +
                        " cos \n" +
                        "-----\n" +
                        " -1\n" +
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
}
