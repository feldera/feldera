package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

// Adapted from https://github.com/postgres/postgres/src/test/regress/expected/limit.out
public class PostgresLimitTests extends SqlIoTest {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.ignoreOrderBy = true;
        return options;
    }

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // Replaced 'name' with 'varchar'
        String setup = """
                CREATE TABLE onek (
                     unique1       int4,
                     unique2       int4,
                     two           int4,
                     four          int4,
                     ten           int4,
                     twenty        int4,
                     hundred       int4,
                     thousand      int4,
                     twothousand   int4,
                     fivethous     int4,
                     tenthous      int4,
                     odd           int4,
                     even          int4,
                     stringu1      varchar,
                     stringu2      varchar,
                     string4       varchar
                )""";
        compiler.submitStatementForCompilation(setup);
        this.insertFromResource("onek", compiler);
    }

    @Test
    public void testLimit() {
        // removed empty columns from test.
        this.qs("""
                SELECT unique1, unique2, stringu1
                        FROM onek WHERE unique1 > 50
                        ORDER BY unique1 LIMIT 2;
                 unique1 | unique2 | stringu1
                ---------+---------+----------
                      51 |      76 | ZBAAAA
                      52 |     985 | ACAAAA
                (2 rows)

                SELECT unique1, unique2, stringu1
                        FROM onek WHERE unique1 > 60
                        ORDER BY unique1 LIMIT 5;
                 unique1 | unique2 | stringu1
                ---------+---------+----------
                      61 |     560 | JCAAAA
                      62 |     633 | KCAAAA
                      63 |     296 | LCAAAA
                      64 |     479 | MCAAAA
                      65 |      64 | NCAAAA
                (5 rows)

                SELECT unique1, unique2, stringu1
                        FROM onek WHERE unique1 > 60 AND unique1 < 63
                        ORDER BY unique1 LIMIT 5;
                 unique1 | unique2 | stringu1
                ---------+---------+----------
                      61 |     560 | JCAAAA
                      62 |     633 | KCAAAA
                (2 rows)""");
    }

    @Test @Ignore("OFFSET not yet implemented")
    public void testOffset() {
        this.qs("""
                SELECT unique1, unique2, stringu1
                        FROM onek WHERE unique1 > 100
                        ORDER BY unique1 LIMIT 3 OFFSET 20;
                 unique1 | unique2 | stringu1
                ---------+---------+----------
                     121 |     700 | REAAAA
                     122 |     519 | SEAAAA
                     123 |     777 | TEAAAA
                (3 rows)

                SELECT unique1, unique2, stringu1
                        FROM onek WHERE unique1 < 50
                        ORDER BY unique1 DESC LIMIT 8 OFFSET 99;
                 unique1 | unique2 | stringu1
                ---------+---------+----------
                (0 rows)

                SELECT unique1, unique2, stringu1
                        FROM onek WHERE unique1 < 50
                        ORDER BY unique1 DESC LIMIT 20 OFFSET 39;
                 unique1 | unique2 | stringu1
                ---------+---------+----------
                      10 |     520 | KAAAAA
                       9 |      49 | JAAAAA
                       8 |     653 | IAAAAA
                       7 |     647 | HAAAAA
                       6 |     978 | GAAAAA
                       5 |     541 | FAAAAA
                       4 |     833 | EAAAAA
                       3 |     431 | DAAAAA
                       2 |     326 | CAAAAA
                       1 |     214 | BAAAAA
                       0 |     998 | AAAAAA
                (11 rows)

                SELECT unique1, unique2, stringu1
                        FROM onek
                        ORDER BY unique1 OFFSET 990;
                 unique1 | unique2 | stringu1
                ---------+---------+----------
                     990 |     369 | CMAAAA
                     991 |     426 | DMAAAA
                     992 |     363 | EMAAAA
                     993 |     661 | FMAAAA
                     994 |     695 | GMAAAA
                     995 |     144 | HMAAAA
                     996 |     258 | IMAAAA
                     997 |      21 | JMAAAA
                     998 |     549 | KMAAAA
                     999 |     152 | LMAAAA
                (10 rows)

                SELECT unique1, unique2, stringu1
                        FROM onek
                        ORDER BY unique1 OFFSET 990 LIMIT 5;
                 unique1 | unique2 | stringu1
                ---------+---------+----------
                     990 |     369 | CMAAAA
                     991 |     426 | DMAAAA
                     992 |     363 | EMAAAA
                     993 |     661 | FMAAAA
                     994 |     695 | GMAAAA
                (5 rows)

                SELECT unique1, unique2, stringu1
                        FROM onek
                        ORDER BY unique1 LIMIT 5 OFFSET 900;
                 unique1 | unique2 | stringu1
                ---------+---------+----------
                     900 |     913 | QIAAAA
                     901 |     931 | RIAAAA
                     902 |     702 | SIAAAA
                     903 |     641 | TIAAAA
                     904 |     793 | UIAAAA
                (5 rows)""");
    }
}
