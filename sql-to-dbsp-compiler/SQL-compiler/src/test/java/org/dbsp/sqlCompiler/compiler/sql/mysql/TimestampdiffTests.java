package org.dbsp.sqlCompiler.compiler.sql.mysql;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

public class TimestampdiffTests extends SqlIoTest {
    // Test data obtained from
    // https://github.com/mysql/mysql-server/blob/ea1efa9822d81044b726aab20c857d5e1b7e046a/mysql-test/r/func_time.result#L715
    @Test
    public void testDateDiff() {
        this.qs("select timestampdiff(MONTH, DATE '2001-02-01', DATE '2001-05-01') as a;\n" +
                "a\n" +
                "-----\n" +
                " 3\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(YEAR, DATE '2002-05-01', DATE '2001-01-01') as a;\n" +
                "a\n" +
                "-----\n" +
                " -1\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(QUARTER, DATE '2002-05-01', DATE '2001-01-01') as a;\n" +
                "a\n" +
                "-----\n" +
                " -5\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(MONTH, DATE '2000-03-28', DATE '2000-02-29') as a;\n" +
                "a\n" +
                "-----\n" +
                " 0\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(MONTH, DATE '1991-03-28', DATE '2000-02-29') as a;\n" +
                "a\n" +
                "-----\n" +
                " 107\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(SQL_TSI_WEEK, DATE '2001-02-01', DATE '2001-05-01') as a;\n" +
                "a\n" +
                "-----\n" +
                "12\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(SQL_TSI_HOUR, DATE '2001-02-01', DATE '2001-05-01') as a;\n" +
                "a\n" +
                "-----\n" +
                "2136\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(SQL_TSI_DAY, DATE '2001-02-01', DATE '2001-05-01') as a;\n" +
                "a\n" +
                "-----\n" +
                "89\n" +
                "(1 row)");
    }
    
    @Test
    public void testDateDayDiff() {
        this.qs("select timestampdiff(SQL_TSI_DAY, DATE '1986-02-01', DATE '1986-03-01') as a1,\n" +
                "timestampdiff(SQL_TSI_DAY, DATE '1900-02-01', DATE '1900-03-01') as a2,\n" +
                "timestampdiff(SQL_TSI_DAY, DATE '1996-02-01', DATE '1996-03-01') as a3,\n" +
                "timestampdiff(SQL_TSI_DAY, DATE '2000-02-01', DATE '2000-03-01') as a4;\n" +
                " a1 | a2 | a3 | a4\n" +
                "-----\n" +
                " 28 | 28 | 29 | 29\n" +
                "(1 row)");
    }

    @Test
    public void testTimestampDiff() {
        this.qs("select timestampdiff(SQL_TSI_MINUTE, TIMESTAMP '2001-02-01 12:59:59', TIMESTAMP '2001-05-01 12:58:59') as a;\n" +
                "a\n" +
                "-----\n" +
                "128159\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(SQL_TSI_SECOND, TIMESTAMP '2001-02-01 12:59:59', TIMESTAMP '2001-05-01 12:58:58') as a;\n" +
                "a\n" +
                "-----\n" +
                "7689539\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:27');\n" +
                "TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:27')\n" +
                "-----\n" +
                "0\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:28');\n" +
                "TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:28')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:29');\n" +
                "TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:29')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:27');\n" +
                "TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:27')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:28');\n" +
                "TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:28')\n" +
                "-----\n" +
                "2\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:29');\n" +
                "TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:29')\n" +
                "-----\n" +
                "2\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:27');\n" +
                "TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:27')\n" +
                "-----\n" +
                "0\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:28');\n" +
                "TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:28')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:29');\n" +
                "TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:29')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:27');\n" +
                "TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:27')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:28');\n" +
                "TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:28')\n" +
                "-----\n" +
                "2\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:29');\n" +
                "TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:29')\n" +
                "-----\n" +
                "2\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:27');\n" +
                "TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:27')\n" +
                "-----\n" +
                "0\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:28');\n" +
                "TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:28')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:29');\n" +
                "TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:29')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:27');\n" +
                "TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:27')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:28');\n" +
                "TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:28')\n" +
                "-----\n" +
                "2\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:29');\n" +
                "TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:29')\n" +
                "-----\n" +
                "2\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:27');\n" +
                "TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:27')\n" +
                "-----\n" +
                "0\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:28');\n" +
                "TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:28')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:29');\n" +
                "TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:29')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:27');\n" +
                "TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:27')\n" +
                "-----\n" +
                "1\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:28');\n" +
                "TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:28')\n" +
                "-----\n" +
                "2\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:29');\n" +
                "TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:29')\n" +
                "-----\n" +
                "2\n" +
                "(1 row)");
    }

    @Test
    public void testMonthDiff() {
        this.qs("select timestampdiff(month, DATE '2004-09-11', DATE '2004-09-11');\n" +
                "timestampdiff(month, DATE '2004-09-11', DATE '2004-09-11')\n" +
                "-----\n" +
                "0\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '2004-09-11', DATE '2005-09-11');\n" +
                "timestampdiff(month, DATE '2004-09-11', DATE '2005-09-11')\n" +
                "-----\n" +
                "12\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '2004-09-11', DATE '2006-09-11');\n" +
                "timestampdiff(month, DATE '2004-09-11', DATE '2006-09-11')\n" +
                "-----\n" +
                "24\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '2004-09-11', DATE '2007-09-11');\n" +
                "timestampdiff(month, DATE '2004-09-11', DATE '2007-09-11')\n" +
                "-----\n" +
                "36\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '2005-09-11', DATE '2004-09-11');\n" +
                "timestampdiff(month, DATE '2005-09-11', DATE '2004-09-11')\n" +
                "-----\n" +
                "-12\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '2005-09-11', DATE '2003-09-11');\n" +
                "timestampdiff(month, DATE '2005-09-11', DATE '2003-09-11')\n" +
                "-----\n" +
                "-24\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '2004-02-28', DATE '2005-02-28');\n" +
                "timestampdiff(month, DATE '2004-02-28', DATE '2005-02-28')\n" +
                "-----\n" +
                "12\n" +
                "(1 row)\n" +
                "\n" +
                // TODO: enable this test when https://issues.apache.org/jira/browse/CALCITE-5981
                // is fixed.  The bug is closed, but the fix is in Avatica, and Avatica
                // hasn't released a new version.
                //"select timestampdiff(month, DATE '2004-02-29', DATE '2005-02-28');\n" +
                //"timestampdiff(month, DATE '2004-02-29', DATE '2005-02-28')\n" +
                //"-----\n" +
                //"11\n" +
                //"(1 row)\n" +
                //"\n" +
                "select timestampdiff(month, DATE '2004-02-28', DATE '2005-02-28');\n" +
                "timestampdiff(month, DATE '2004-02-28', DATE '2005-02-28')\n" +
                "-----\n" +
                "12\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '2004-03-29', DATE '2005-03-28');\n" +
                "timestampdiff(month, DATE '2004-03-29', DATE '2005-03-28')\n" +
                "-----\n" +
                "11\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '2003-02-28', DATE '2004-02-29');\n" +
                "timestampdiff(month, DATE '2003-02-28', DATE '2004-02-29')\n" +
                "-----\n" +
                "12\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '2003-02-28', DATE '2005-02-28');\n" +
                "timestampdiff(month, DATE '2003-02-28', DATE '2005-02-28')\n" +
                "-----\n" +
                "24\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '1999-09-11', DATE '2001-10-10');\n" +
                "timestampdiff(month, DATE '1999-09-11', DATE '2001-10-10')\n" +
                "-----\n" +
                "24\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(month, DATE '1999-09-11', DATE '2001-9-11');\n" +
                "timestampdiff(month, DATE '1999-09-11', DATE '2001-9-11')\n" +
                "-----\n" +
                "24\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(year, DATE '1999-09-11', DATE '2001-9-11');\n" +
                "timestampdiff(year, DATE '1999-09-11', DATE '2001-9-11')\n" +
                "-----\n" +
                "2\n" +
                "(1 row)\n" +
                "\n" +
                "select timestampdiff(year, DATE '2004-02-28', DATE '2005-02-28');\n" +
                "timestampdiff(year, DATE '2004-02-28', DATE '2005-02-28')\n" +
                "-----\n" +
                "1\n" +
                // TODO: enable this test when https://issues.apache.org/jira/browse/CALCITE-5981
                // is fixed.
                //"(1 row)\n" +
                //"\n" +
                //"select timestampdiff(year, DATE '2004-02-29', DATE '2005-02-28');\n" +
                //"timestampdiff(year, DATE '2004-02-29', DATE '2005-02-28')\n" +
                //"-----\n" +
                //"0\n" +
                "(1 row)");
    }
}
