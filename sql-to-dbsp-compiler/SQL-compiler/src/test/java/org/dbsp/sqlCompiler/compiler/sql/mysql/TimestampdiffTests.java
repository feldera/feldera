package org.dbsp.sqlCompiler.compiler.sql.mysql;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

public class TimestampdiffTests extends SqlIoTest {
    // Test data obtained from
    // https://github.com/mysql/mysql-server/blob/ea1efa9822d81044b726aab20c857d5e1b7e046a/mysql-test/r/func_time.result#L715
    @Test
    public void testDateDiff() {
        this.qs("""
                select timestampdiff(MONTH, DATE '2001-02-01', DATE '2001-05-01') as a;
                a
                -----
                 3
                (1 row)

                select timestampdiff(YEAR, DATE '2002-05-01', DATE '2001-01-01') as a;
                a
                -----
                 -1
                (1 row)

                select timestampdiff(QUARTER, DATE '2002-05-01', DATE '2001-01-01') as a;
                a
                -----
                 -5
                (1 row)

                select timestampdiff(MONTH, DATE '2000-03-28', DATE '2000-02-29') as a;
                a
                -----
                 0
                (1 row)

                select timestampdiff(MONTH, DATE '1991-03-28', DATE '2000-02-29') as a;
                a
                -----
                 107
                (1 row)

                select timestampdiff(SQL_TSI_WEEK, DATE '2001-02-01', DATE '2001-05-01') as a;
                a
                -----
                12
                (1 row)

                select timestampdiff(SQL_TSI_HOUR, DATE '2001-02-01', DATE '2001-05-01') as a;
                a
                -----
                2136
                (1 row)

                select timestampdiff(SQL_TSI_DAY, DATE '2001-02-01', DATE '2001-05-01') as a;
                a
                -----
                89
                (1 row)""");
    }
    
    @Test
    public void testDateDayDiff() {
        this.qs("""
                select timestampdiff(SQL_TSI_DAY, DATE '1986-02-01', DATE '1986-03-01') as a1,
                timestampdiff(SQL_TSI_DAY, DATE '1900-02-01', DATE '1900-03-01') as a2,
                timestampdiff(SQL_TSI_DAY, DATE '1996-02-01', DATE '1996-03-01') as a3,
                timestampdiff(SQL_TSI_DAY, DATE '2000-02-01', DATE '2000-03-01') as a4;
                 a1 | a2 | a3 | a4
                -----
                 28 | 28 | 29 | 29
                (1 row)""");
    }

    @Test
    public void testTimestampDiff() {
        this.qs("""
                select timestampdiff(SQL_TSI_MINUTE, TIMESTAMP '2001-02-01 12:59:59', TIMESTAMP '2001-05-01 12:58:59') as a;
                a
                -----
                128159
                (1 row)

                select timestampdiff(SQL_TSI_SECOND, TIMESTAMP '2001-02-01 12:59:59', TIMESTAMP '2001-05-01 12:58:58') as a;
                a
                -----
                7689539
                (1 row)

                SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:27');
                TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:27')
                -----
                0
                (1 row)

                SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:28');
                TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:28')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:29');
                TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-11 14:30:29')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:27');
                TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:27')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:28');
                TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:28')
                -----
                2
                (1 row)

                SELECT TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:29');
                TIMESTAMPDIFF(day, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-12 14:30:29')
                -----
                2
                (1 row)

                SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:27');
                TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:27')
                -----
                0
                (1 row)

                SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:28');
                TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:28')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:29');
                TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-17 14:30:29')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:27');
                TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:27')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:28');
                TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:28')
                -----
                2
                (1 row)

                SELECT TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:29');
                TIMESTAMPDIFF(week, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-01-24 14:30:29')
                -----
                2
                (1 row)

                SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:27');
                TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:27')
                -----
                0
                (1 row)

                SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:28');
                TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:28')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:29');
                TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-02-10 14:30:29')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:27');
                TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:27')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:28');
                TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:28')
                -----
                2
                (1 row)

                SELECT TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:29');
                TIMESTAMPDIFF(month, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2006-03-10 14:30:29')
                -----
                2
                (1 row)

                SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:27');
                TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:27')
                -----
                0
                (1 row)

                SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:28');
                TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:28')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:29');
                TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2007-01-10 14:30:29')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:27');
                TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:27')
                -----
                1
                (1 row)

                SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:28');
                TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:28')
                -----
                2
                (1 row)

                SELECT TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:29');
                TIMESTAMPDIFF(year, TIMESTAMP '2006-01-10 14:30:28', TIMESTAMP '2008-01-10 14:30:29')
                -----
                2
                (1 row)""");
    }

    @Test
    public void testMonthDiff() {
        this.qs("""
                select timestampdiff(month, DATE '2004-09-11', DATE '2004-09-11');
                timestampdiff(month, DATE '2004-09-11', DATE '2004-09-11')
                -----
                0
                (1 row)

                select timestampdiff(month, DATE '2004-09-11', DATE '2005-09-11');
                timestampdiff(month, DATE '2004-09-11', DATE '2005-09-11')
                -----
                12
                (1 row)

                select timestampdiff(month, DATE '2004-09-11', DATE '2006-09-11');
                timestampdiff(month, DATE '2004-09-11', DATE '2006-09-11')
                -----
                24
                (1 row)

                select timestampdiff(month, DATE '2004-09-11', DATE '2007-09-11');
                timestampdiff(month, DATE '2004-09-11', DATE '2007-09-11')
                -----
                36
                (1 row)

                select timestampdiff(month, DATE '2005-09-11', DATE '2004-09-11');
                timestampdiff(month, DATE '2005-09-11', DATE '2004-09-11')
                -----
                -12
                (1 row)

                select timestampdiff(month, DATE '2005-09-11', DATE '2003-09-11');
                timestampdiff(month, DATE '2005-09-11', DATE '2003-09-11')
                -----
                -24
                (1 row)

                select timestampdiff(month, DATE '2004-02-28', DATE '2005-02-28');
                timestampdiff(month, DATE '2004-02-28', DATE '2005-02-28')
                -----
                12
                (1 row)

                select timestampdiff(month, DATE '2004-02-29', DATE '2005-02-28');
                timestampdiff(month, DATE '2004-02-29', DATE '2005-02-28')
                -----
                11
                (1 row)

                select timestampdiff(month, DATE '2004-02-28', DATE '2005-02-28');
                timestampdiff(month, DATE '2004-02-28', DATE '2005-02-28')
                -----
                12
                (1 row)

                select timestampdiff(month, DATE '2004-03-29', DATE '2005-03-28');
                timestampdiff(month, DATE '2004-03-29', DATE '2005-03-28')
                -----
                11
                (1 row)

                select timestampdiff(month, DATE '2003-02-28', DATE '2004-02-29');
                timestampdiff(month, DATE '2003-02-28', DATE '2004-02-29')
                -----
                12
                (1 row)

                select timestampdiff(month, DATE '2003-02-28', DATE '2005-02-28');
                timestampdiff(month, DATE '2003-02-28', DATE '2005-02-28')
                -----
                24
                (1 row)

                select timestampdiff(month, DATE '1999-09-11', DATE '2001-10-10');
                timestampdiff(month, DATE '1999-09-11', DATE '2001-10-10')
                -----
                24
                (1 row)

                select timestampdiff(month, DATE '1999-09-11', DATE '2001-9-11');
                timestampdiff(month, DATE '1999-09-11', DATE '2001-9-11')
                -----
                24
                (1 row)

                select timestampdiff(year, DATE '1999-09-11', DATE '2001-9-11');
                timestampdiff(year, DATE '1999-09-11', DATE '2001-9-11')
                -----
                2
                (1 row)

                select timestampdiff(year, DATE '2004-02-28', DATE '2005-02-28');
                timestampdiff(year, DATE '2004-02-28', DATE '2005-02-28')
                -----
                1
                (1 row)

                select timestampdiff(year, DATE '2004-02-29', DATE '2005-02-28');
                timestampdiff(year, DATE '2004-02-29', DATE '2005-02-28')
                -----
                 0
                (1 row)"""
                );
    }
}
