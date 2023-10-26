package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.junit.Ignore;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/03734a7fed7d924679770adb78a7db8a37d14188/src/test/regress/expected/arrays.out
public class PostgresArrayTests extends PostgresBaseTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
    }

    @Test @Ignore("Two bugs in calcite: https://issues.apache.org/jira/browse/CALCITE-5879 and " +
            "https://issues.apache.org/jira/browse/CALCITE-5882")
    public void testSplit() {
        // Renamed 'string_to_array' to 'split'
        this.qs("select split('1|2|3', '|');\n" +
                " split \n" +
                "-----------------\n" +
                " {1,2,3}\n" +
                "(1 row)\n" +
                "\n" +
                "select split('1|2|3|', '|');\n" +
                " split \n" +
                "-----------------\n" +
                " {1,2,3,}\n" +
                "(1 row)\n" +
                "\n" +
                "select split('1||2|3||', '||');\n" +
                " split \n" +
                "-----------------\n" +
                " {1,2|3,}\n" +
                "(1 row)\n" +
                "\n" +
                "select split('1|2|3', '');\n" +
                " split \n" +
                "-----------------\n" +
                " {1|2|3}\n" +
                "(1 row)\n" +
                "\n" +
                "select split('', '|');\n" +
                " split \n" +
                "-----------------\n" +
                " {}\n" +
                "(1 row)\n" +
                "\n" +
                "select split('1|2|3', NULL);\n" +
                " split \n" +
                "-----------------\n" +
                " {1,|,2,|,3}\n" +
                "(1 row)\n" +
                "\n" +
                "select split(NULL, '|') IS NULL;\n" +
                " ?column? \n" +
                "----------\n" +
                " t\n" +
                "(1 row)\n" +
                "\n" +
                "select split('abc', '');\n" +
                " split \n" +
                "-----------------\n" +
                " {abc}\n" +
                "(1 row)\n" +
                "\n" +
                "select split('abc', ',');\n" +
                " split \n" +
                "-----------------\n" +
                " {abc}\n" +
                "(1 row)\n" +
                "\n" +
                "select split('1,2,3,4,,6', ',');\n" +
                " split \n" +
                "-----------------\n" +
                " {1,2,3,4,,6}\n" +
                "(1 row)");
        // Deleted some three-operand cases
    }

    @Test
    public void testArrayToString() {
        // In Calcite array_to_string requires all arguments to be strings
        this.qs("select array_to_string(NULL::TEXT ARRAY, ',') IS NULL;\n" +
                " ?column? \n" +
                "----------\n" +
                " t\n" +
                "(1 row)\n" +
                "\n" +
                "select array_to_string(ARRAY()::TEXT ARRAY, ',');\n" +
                " array_to_string \n" +
                "-----------------\n" +
                " \n" +
                "(1 row)\n" +
                "\n" +
                "select array_to_string(array['1','2','3','4',NULL,'6'], ',');\n" +
                " array_to_string \n" +
                "-----------------\n" +
                " 1,2,3,4,6\n" +
                "(1 row)\n" +
                "\n" +
                "select array_to_string(array['1','2','3','4',NULL,'6'], ',', '*');\n" +
                " array_to_string \n" +
                "-----------------\n" +
                " 1,2,3,4,*,6\n" +
                "(1 row)\n" +
                "\n" +
                "select array_to_string(array['1','2','3','4',NULL,'6'], NULL);\n" +
                " array_to_string \n" +
                "-----------------\n" +
                "NULL\n" +
                "(1 row)\n" +
                "\n" +
                "select array_to_string(split('1|2|3', '|'), '|');\n" +
                " array_to_string \n" +
                "-----------------\n" +
                " 1|2|3\n" +
                "(1 row)\n");
    }

    @Test
    public void testArrayToStringNoNull() {
        // semantics is different from Postgres
        this.q("select array_to_string(array['1','2','3','4',NULL,'6'], ',', NULL);\n" +
                " array_to_string \n" +
                "-----------------\n" +
                "NULL");
    }
}
