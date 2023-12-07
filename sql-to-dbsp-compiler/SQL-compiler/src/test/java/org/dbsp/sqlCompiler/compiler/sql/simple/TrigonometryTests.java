package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

public class TrigonometryTests extends SqlIoTest {
    @Test
    public void testSin() {
        this.qs(
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
                "SELECT sin(3.14);\n" +
                " sin \n" +
                "-----\n" +
                " 0.0015926529164868282\n" +
                "(1 row)"
        );
    }


    @Test
    public void testCos() {
        this.qs(
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
                "SELECT cos(3.14);\n" +
                " cos \n" +
                "-----\n" +
                " -0.9999987317275395\n" +
                "(1 row)"
        );
    }
}
