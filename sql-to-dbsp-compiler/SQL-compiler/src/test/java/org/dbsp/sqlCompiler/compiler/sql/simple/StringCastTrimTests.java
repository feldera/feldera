package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Assert;
import org.junit.Test;

/** Casts of strings padded with characters up to U+0020: the compiler folds the casts of
 * constants, and the runtime evaluates the casts of the columns; both give the same result. */
public class StringCastTrimTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // Columns v and n hold the same value, padded with U+0001, a space, a tab, and a newline
        compiler.submitStatementsForCompilation("""
                CREATE TABLE padded(k VARCHAR NOT NULL, v VARCHAR, n VARCHAR NOT NULL);
                INSERT INTO padded VALUES
                ('decimal', U&'\\0001 \\00091.5 \\000A', U&'\\0001 \\00091.5 \\000A'),
                ('int', U&'\\0001 \\00097 \\000A', U&'\\0001 \\00097 \\000A'),
                ('double', U&'\\0001 \\00091.5 \\000A', U&'\\0001 \\00091.5 \\000A'),
                ('boolean', U&'\\0001 \\0009TRUE \\000A', U&'\\0001 \\0009TRUE \\000A'),
                ('date', U&'\\0001 \\00092020-01-02 \\000A', U&'\\0001 \\00092020-01-02 \\000A'),
                ('time', U&'\\0001 \\000910:11:12 \\000A', U&'\\0001 \\000910:11:12 \\000A'),
                ('timestamp', U&'\\0001 \\00092020-01-02 10:11:12 \\000A',
                              U&'\\0001 \\00092020-01-02 10:11:12 \\000A'),
                ('tz', U&'\\0001 \\00092020-01-02 10:11:12 UTC \\000A',
                       U&'\\0001 \\00092020-01-02 10:11:12 UTC \\000A');""");
    }

    @Test
    public void paddedStringCasts() {
        this.qst("""
                SELECT CAST(v AS DECIMAL(10, 2)) AS v, CAST(n AS DECIMAL(10, 2)) AS n,
                       CAST(U&'\\0001 \\00091.5 \\000A' AS DECIMAL(10, 2)) AS c
                FROM padded WHERE k = 'decimal';
                 v   | n   | c
                -----------------
                 1.5 | 1.5 | 1.5
                (1 row)

                SELECT CAST(v AS INT) AS v, CAST(n AS INT) AS n,
                       CAST(U&'\\0001 \\00097 \\000A' AS INT) AS c
                FROM padded WHERE k = 'int';
                 v | n | c
                -----------
                 7 | 7 | 7
                (1 row)

                SELECT CAST(CAST(v AS DOUBLE) AS VARCHAR) AS v,
                       CAST(CAST(n AS REAL) AS VARCHAR) AS n,
                       CAST(CAST(U&'\\0001 \\00091.5 \\000A' AS DOUBLE) AS VARCHAR) AS c
                FROM padded WHERE k = 'double';
                 v   | n   | c
                -----------------
                 1.5 | 1.5 | 1.5
                (1 row)

                SELECT CAST(v AS BOOLEAN) AS v, CAST(n AS BOOLEAN) AS n,
                       CAST(U&'\\0001 \\0009TRUE \\000A' AS BOOLEAN) AS c
                FROM padded WHERE k = 'boolean';
                 v    | n    | c
                --------------------
                 true | true | true
                (1 row)

                SELECT CAST(v AS DATE) AS v, CAST(n AS DATE) AS n,
                       CAST(U&'\\0001 \\00092020-01-02 \\000A' AS DATE) AS c
                FROM padded WHERE k = 'date';
                 v          | n          | c
                --------------------------------------
                 2020-01-02 | 2020-01-02 | 2020-01-02
                (1 row)

                SELECT CAST(v AS TIME) AS v, CAST(n AS TIME) AS n,
                       CAST(U&'\\0001 \\000910:11:12 \\000A' AS TIME) AS c
                FROM padded WHERE k = 'time';
                 v        | n        | c
                --------------------------------
                 10:11:12 | 10:11:12 | 10:11:12
                (1 row)

                SELECT CAST(v AS TIMESTAMP) AS v, CAST(n AS TIMESTAMP) AS n,
                       CAST(U&'\\0001 \\00092020-01-02 10:11:12 \\000A' AS TIMESTAMP) AS c
                FROM padded WHERE k = 'timestamp';
                 v                   | n                   | c
                -----------------------------------------------------------------
                 2020-01-02 10:11:12 | 2020-01-02 10:11:12 | 2020-01-02 10:11:12
                (1 row)

                SELECT CAST(v AS TIMESTAMP WITH TIME ZONE) = TIMESTAMP WITH TIME ZONE '2020-01-02 10:11:12 UTC' AS v,
                       CAST(n AS TIMESTAMP WITH TIME ZONE) = TIMESTAMP WITH TIME ZONE '2020-01-02 10:11:12 UTC' AS n,
                       CAST(U&'\\0001 \\00092020-01-02 10:11:12 UTC \\000A' AS TIMESTAMP WITH TIME ZONE) =
                           TIMESTAMP WITH TIME ZONE '2020-01-02 10:11:12 UTC' AS c
                FROM padded WHERE k = 'tz';
                 v    | n    | c
                --------------------
                 true | true | true
                (1 row)""");
    }

    @Test
    public void noWarningsForPaddedConstants() {
        // The compiler folds these casts without reporting that a string is not a valid value
        String sql = """
                CREATE TABLE T0(x INT);
                CREATE VIEW V AS SELECT x,
                CAST(' 1.5\t' AS DECIMAL(10, 2)) AS d,
                CAST(' true\t' AS BOOLEAN) AS b,
                CAST(' 2020-01-02\t' AS DATE) AS dt,
                CAST(' 10:11:12\t' AS TIME) AS t,
                CAST(' 2020-01-02 10:11:12\t' AS TIMESTAMP) AS ts
                FROM T0;""";
        DBSPCompiler compiler = this.chattyCompiler();
        compiler.submitStatementsForCompilation(sql);
        compiler.getFinalCircuit(true);
        Assert.assertEquals(compiler.messages.toString(), 0, compiler.messages.warningCount());
    }
}
