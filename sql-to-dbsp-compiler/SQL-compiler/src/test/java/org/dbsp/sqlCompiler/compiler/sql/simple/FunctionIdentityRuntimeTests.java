package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Test;

/** Execute identity functions whose projects the relational optimizer removes. */
public class FunctionIdentityRuntimeTests extends BaseSQLTests {
    @Test
    public void arrayIdentityPreservesValuesAndNulls() {
        var ccs = this.getCCS("""
                CREATE FUNCTION identity_array(a INT ARRAY) RETURNS INT ARRAY AS a;
                CREATE TABLE t(a INT ARRAY);
                CREATE VIEW v AS SELECT ARRAY_REVERSE(identity_array(a)) AS result FROM t;
                """);
        ccs.stepWeightOne("""
                INSERT INTO t VALUES (ARRAY[2, 3, 4]), (ARRAY[1, NULL]), (NULL);
                """, """
                 result
                -----------
                 {4, 3, 2}
                 {NULL, 1}
                 NULL
                """);
    }

    @Test
    public void binaryIdentityRetainsItsScalarType() {
        var ccs = this.getCCS("""
                CREATE FUNCTION identity_binary(a VARBINARY NOT NULL) RETURNS VARBINARY NOT NULL AS a;
                CREATE TABLE t(b VARBINARY NOT NULL);
                CREATE VIEW v AS SELECT OCTET_LENGTH(identity_binary(b)) AS size FROM t;
                """);
        ccs.stepWeightOne("INSERT INTO t VALUES (X'01FF'), (X'03');", """
                 size
                ------
                 2
                 1
                """);
    }
}
