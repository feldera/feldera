package org.dbsp.sqlCompiler.compiler.sql.mysql;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

// https://github.com/mysql/mysql-server/blob/trunk/mysql-test/r/date_formats.result
public class DateFormatsTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                -- initially named T1
                create table t2 (d date);
                insert into t2 values ('2004-07-14'),('2005-07-14');

                -- initially named T1
                create table t3 (f1 date);
                insert into t3 (f1) values ('2005-01-01');
                insert into t3 (f1) values ('2005-02-01');
                """);
    }

    @Test
    public void testFormat() {
        // date_format(a, b) -> format_date(b, a)
        this.q("""
                select format_date('%d', d) from t2;
                format_date(d,"%d")
                -------------------
                 14
                 14""");
        this.q("""
                select format_date('%m', f1) as d1, format_date('%B', f1) as d2 from t3;
                d1\td2
                 02\t February
                 01\t January""");
    }

    @Test
    public void testParseDate() {
        this.qs("""
                SELECT PARSE_DATE(' %Y-%m-%d', '   2020-10-01');
                 d
                ---
                 2020-10-01
                (1 row)
                
                SELECT PARSE_DATE('%Y-%m-%d', '2020-10-01');
                 d
                ---
                2020-10-01
                (1 row)
               
                -- This works because elements on both sides match.
                SELECT PARSE_DATE('%A %b %e %Y', 'Thursday Dec 25 2008');
                 d
                ---
                2008-12-25
                (1 row)
                
                -- This works because %F can find all matching elements in date_string.
                SELECT PARSE_DATE('%F', '2000-12-30');
                 d
                ---
                2000-12-30
                (1 row)
                
                -- This works because %F can find all matching elements in date_string.
                SELECT PARSE_DATE(' %F ', '   2000-12-30   ');
                 d
                ---
                2000-12-30
                (1 row)
       
                -- Parse error because one of the year is in the wrong place.
                SELECT PARSE_DATE('%Y %A %b %e', 'Thursday Dec 25 2008');
                 d
                ---
                NULL
                (1 row)
                
                -- Parse error because one of the year elements is missing.
                SELECT PARSE_DATE('%A %b %e', 'Thursday Dec 25 2008');
                 d
                ---
                NULL
                (1 row)""");
    }

    @Test
    public void testIncorrectOrder() {
        // Returns NULL in MySQL
        this.runtimeConstantFail("SELECT format_date(1151414896, '%Y-%m-%d %H:%i:%s')",
                "input contains invalid characters");
    }

    @Test
    public void testFormat2() {
        // MySql seems to have different format specifiers!
        // %W in MySql is %A
        // %M in MySql is %B
        this.q("""
                select format_date('%A (%a), %e %B (%b) %Y', '2004-01-01');
                format_date('%A (%a), %e %B (%b) %Y', '2004-01-01',)
                ------------------
                 Thursday (Thu),  1 January (Jan) 2004""");
    }

    @Test
    public void testCorners() {
        // Year 0 is not legal, replaced with year 1
        // %Y in MySql is %y
        this.qs("""
                SELECT format_date('%Y-%m', DATE '2020-10-10');
                valid_date
                ------------
                 2020-10
                (1 row)
                
                SELECT format_date('%A %d %B %Y', '0001-01-01') as valid_date;
                valid_date
                ------------
                 Monday 01 January 0001
                (1 row)

                SELECT format_date('%A %d %B %Y', '0001-02-28') as valid_date;
                valid_date
                ------------
                 Wednesday 28 February 0001
                (1 row)

                SELECT format_date('%A %d %B %Y', '2009-01-01') as valid_date;
                valid_date
                -------------
                 Thursday 01 January 2009
                (1 row)""");
    }

    @Test
    public void testAllFormat() {
        // Test all format flags that can be applied to a DATE.
        // not testing %n, difficult using the q function
        this.q("""
                SELECT format_date('%A,%a,%B,%b,%C,%D,%d,%e,%F,%G,%g,%h,%j,%m,%U,%u,%V,%W,%w,%x,%Y,%y,%t,%%','2024-02-07');
                result
                ------
                 Wednesday,Wed,February,Feb,20,02/07/24,07, 7,2024-02-07,2024,24,Feb,038,02,05,3,06,06,3,02/07/24,2024,24,\t,%""");
    }
}
