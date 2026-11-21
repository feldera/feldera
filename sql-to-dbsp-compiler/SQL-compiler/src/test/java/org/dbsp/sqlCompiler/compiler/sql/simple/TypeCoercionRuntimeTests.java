package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.junit.Test;

/** Value and multiplicity checks for the coercions exercised by the P0 probes. */
public class TypeCoercionRuntimeTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.incrementalize = true;
        return options;
    }

    @Test
    public void booleanNumericEquality() {
        for (String numeric: new String[] {"REAL", "DOUBLE", "DECIMAL(10,2)"}) {
            var ccs = this.getCCS("CREATE TABLE t(a BOOLEAN, b " + numeric + ");"
                    + "CREATE VIEW v AS SELECT a = b AS eq, b = a AS reversed, a <> b AS ne FROM t;");
            ccs.step("""
                    INSERT INTO t VALUES (TRUE, 1), (FALSE, 0), (TRUE, 2), (FALSE, 2),
                                         (NULL, 1), (TRUE, NULL), (NULL, NULL);
                    """, """
                     eq   | reversed | ne   | weight
                    ------+----------+------+-------
                     true | true     | false| 2
                     false| false    | true | 2
                     NULL | NULL     | NULL | 3""");
            ccs.step("REMOVE FROM t VALUES (TRUE, 1);", """
                     eq   | reversed | ne   | weight
                    ------+----------+------+-------
                     true | true     | false| -1""");
        }
    }

    @Test
    public void intersectAllWidensNullableInput() {
        var ccs = this.getCCS("""
                CREATE TABLE l(a INT NOT NULL);
                CREATE TABLE r(a BIGINT);
                CREATE VIEW v AS SELECT a FROM l INTERSECT ALL SELECT a FROM r;
                """);
        ccs.step("""
                INSERT INTO l VALUES (1), (1), (2), (3);
                INSERT INTO r VALUES (1), (1), (1), (NULL), (2);
                """, """
                 a | weight
                ---+-------
                 1 | 2
                 2 | 1""");
        ccs.step("REMOVE FROM r VALUES (1), (1), (1);", """
                 a | weight
                ---+-------
                 1 | -2""");
    }

    @Test
    public void exceptAllWidensNullableInput() {
        var ccs = this.getCCS("""
                CREATE TABLE l(a INT NOT NULL);
                CREATE TABLE r(a BIGINT);
                CREATE VIEW v AS SELECT a FROM l EXCEPT ALL SELECT a FROM r;
                """);
        ccs.step("""
                INSERT INTO l VALUES (1), (1), (2), (3);
                INSERT INTO r VALUES (1), (NULL);
                """, """
                 a | weight
                ---+-------
                 1 | 1
                 2 | 1
                 3 | 1""");
        ccs.step("INSERT INTO r VALUES (1);", """
                 a | weight
                ---+-------
                 1 | -1""");
    }
}
