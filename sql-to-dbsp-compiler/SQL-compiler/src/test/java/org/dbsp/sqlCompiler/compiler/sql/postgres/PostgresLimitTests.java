package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

// Adapted from https://github.com/postgres/postgres/src/test/regress/expected/limit.out
public class PostgresLimitTests extends SqlIoTest {
    @Override
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.languageOptions.ignoreOrderBy = true;
        return options;
    }

    @Override
    public void prepareData(DBSPCompiler compiler) {
        // Replaced 'name' with 'varchar'
        String setup = "CREATE TABLE onek (\n" +
                "     unique1       int4,\n" +
                "     unique2       int4,\n" +
                "     two           int4,\n" +
                "     four          int4,\n" +
                "     ten           int4,\n" +
                "     twenty        int4,\n" +
                "     hundred       int4,\n" +
                "     thousand      int4,\n" +
                "     twothousand   int4,\n" +
                "     fivethous     int4,\n" +
                "     tenthous      int4,\n" +
                "     odd           int4,\n" +
                "     even          int4,\n" +
                "     stringu1      varchar,\n" +
                "     stringu2      varchar,\n" +
                "     string4       varchar\n" +
                ")";
        compiler.compileStatement(setup);
        this.insertFromResource("onek", compiler);
    }

    @Test
    public void testLimit() {
        // removed empty columns from test.
        this.qs("SELECT unique1, unique2, stringu1\n" +
                "        FROM onek WHERE unique1 > 50\n" +
                "        ORDER BY unique1 LIMIT 2;\n" +
                " unique1 | unique2 | stringu1 \n" +
                "---------+---------+----------\n" +
                "      51 |      76 | ZBAAAA\n" +
                "      52 |     985 | ACAAAA\n" +
                "(2 rows)\n" +
                "\n" +
                "SELECT unique1, unique2, stringu1\n" +
                "        FROM onek WHERE unique1 > 60\n" +
                "        ORDER BY unique1 LIMIT 5;\n" +
                " unique1 | unique2 | stringu1 \n" +
                "---------+---------+----------\n" +
                "      61 |     560 | JCAAAA\n" +
                "      62 |     633 | KCAAAA\n" +
                "      63 |     296 | LCAAAA\n" +
                "      64 |     479 | MCAAAA\n" +
                "      65 |      64 | NCAAAA\n" +
                "(5 rows)\n" +
                "\n" +
                "SELECT unique1, unique2, stringu1\n" +
                "        FROM onek WHERE unique1 > 60 AND unique1 < 63\n" +
                "        ORDER BY unique1 LIMIT 5;\n" +
                " unique1 | unique2 | stringu1 \n" +
                "---------+---------+----------\n" +
                "      61 |     560 | JCAAAA\n" +
                "      62 |     633 | KCAAAA\n" +
                "(2 rows)");
    }

    @Test @Ignore("OFFSET not yet implemented")
    public void testOffset() {
        this.qs("SELECT unique1, unique2, stringu1\n" +
                "        FROM onek WHERE unique1 > 100\n" +
                "        ORDER BY unique1 LIMIT 3 OFFSET 20;\n" +
                " unique1 | unique2 | stringu1 \n" +
                "---------+---------+----------\n" +
                "     121 |     700 | REAAAA\n" +
                "     122 |     519 | SEAAAA\n" +
                "     123 |     777 | TEAAAA\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT unique1, unique2, stringu1\n" +
                "        FROM onek WHERE unique1 < 50\n" +
                "        ORDER BY unique1 DESC LIMIT 8 OFFSET 99;\n" +
                " unique1 | unique2 | stringu1 \n" +
                "---------+---------+----------\n" +
                "(0 rows)\n" +
                "\n" +
                "SELECT unique1, unique2, stringu1\n" +
                "        FROM onek WHERE unique1 < 50\n" +
                "        ORDER BY unique1 DESC LIMIT 20 OFFSET 39;\n" +
                " unique1 | unique2 | stringu1 \n" +
                "---------+---------+----------\n" +
                "      10 |     520 | KAAAAA\n" +
                "       9 |      49 | JAAAAA\n" +
                "       8 |     653 | IAAAAA\n" +
                "       7 |     647 | HAAAAA\n" +
                "       6 |     978 | GAAAAA\n" +
                "       5 |     541 | FAAAAA\n" +
                "       4 |     833 | EAAAAA\n" +
                "       3 |     431 | DAAAAA\n" +
                "       2 |     326 | CAAAAA\n" +
                "       1 |     214 | BAAAAA\n" +
                "       0 |     998 | AAAAAA\n" +
                "(11 rows)\n" +
                "\n" +
                "SELECT unique1, unique2, stringu1\n" +
                "        FROM onek\n" +
                "        ORDER BY unique1 OFFSET 990;\n" +
                " unique1 | unique2 | stringu1 \n" +
                "---------+---------+----------\n" +
                "     990 |     369 | CMAAAA\n" +
                "     991 |     426 | DMAAAA\n" +
                "     992 |     363 | EMAAAA\n" +
                "     993 |     661 | FMAAAA\n" +
                "     994 |     695 | GMAAAA\n" +
                "     995 |     144 | HMAAAA\n" +
                "     996 |     258 | IMAAAA\n" +
                "     997 |      21 | JMAAAA\n" +
                "     998 |     549 | KMAAAA\n" +
                "     999 |     152 | LMAAAA\n" +
                "(10 rows)\n" +
                "\n" +
                "SELECT unique1, unique2, stringu1\n" +
                "        FROM onek\n" +
                "        ORDER BY unique1 OFFSET 990 LIMIT 5;\n" +
                " unique1 | unique2 | stringu1 \n" +
                "---------+---------+----------\n" +
                "     990 |     369 | CMAAAA\n" +
                "     991 |     426 | DMAAAA\n" +
                "     992 |     363 | EMAAAA\n" +
                "     993 |     661 | FMAAAA\n" +
                "     994 |     695 | GMAAAA\n" +
                "(5 rows)\n" +
                "\n" +
                "SELECT unique1, unique2, stringu1\n" +
                "        FROM onek\n" +
                "        ORDER BY unique1 LIMIT 5 OFFSET 900;\n" +
                " unique1 | unique2 | stringu1 \n" +
                "---------+---------+----------\n" +
                "     900 |     913 | QIAAAA\n" +
                "     901 |     931 | RIAAAA\n" +
                "     902 |     702 | SIAAAA\n" +
                "     903 |     641 | TIAAAA\n" +
                "     904 |     793 | UIAAAA\n" +
                "(5 rows)");
    }
}
