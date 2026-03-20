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

    record R(int year, int month, int day, String format, String expected) {
        @Override
        public String toString() {
            return this.year + "-" + String.format("%02d", this.month) + "-" + String.format("%02d", this.day);
        }
    }

    @Test
    public void testFormatDate() {
        R[] tests = {
                new R(2024, 3, 5, "%Y", "2024"),
                new R(2024, 3, 5, "%C", "20"),
                new R(2024, 3, 5, "%y", "24"),
                new R(2024, 3, 5, "%m", "03"),
                new R(2024, 3, 5, "%b", "Mar"),
                new R(2024, 3, 5, "%B", "March"),
                new R(2024, 3, 5, "%h", "Mar"),
                new R(2024, 3, 5, "%d", "05"),
                new R(2024, 3, 5, "%e", " 5"),
                new R(2024, 3, 5, "%a", "Tue"),
                new R(2024, 3, 5, "%A", "Tuesday"),
                new R(2024, 3, 5, "%u", "2"),
                new R(2024, 3, 5, "%w", "2"),
                new R(2024, 1, 1, "%U", "00"),
                new R(2024, 1, 1, "%W", "01"),
                new R(2024, 1, 1, "%G", "2024"),
                new R(2024, 1, 1, "%g", "24"),
                new R(2024, 1, 1, "%V", "01"),
                new R(2024, 12, 31, "%j", "366"),
                new R(2024, 3, 5, "%D", "03/05/24"),
                new R(2024, 3, 5, "%F", "2024-03-05"),
                new R(2024, 3, 5, "%x", "03/05/24"), // Chrono uses US locale
                new R(2024, 3, 5, "%%", "%"),
                new R(2024, 3, 5, "%q", "1")
        };

        StringBuilder builder = new StringBuilder();
        for (R r: tests) {
            builder.append("SELECT format_date('")
                    .append(r.format)
                    .append("', '")
                    .append(r).append("');\n")
                    .append(" r\n")
                    .append("------\n")
                    .append(" ").append(r.expected).append("\n")
                    .append("(1 row)\n\n");
        }
        this.qs(builder.toString());
    }

    record T(int hour, int minute, int second, int microsec) {
        @Override
        public String toString() {
            return String.format("%02d", this.hour) + ":" + String.format("%02d", this.minute) + ":" + String.format("%02d", this.second)
                    + "." + String.format("%06d", this.microsec);
        }
    }

    record FE(String format, String expected) {}

    @Test
    public void testFormatTime() {
        FE[] tests = {
                new FE("%H", "13"),
                new FE("%k", "13"),
                new FE("%-H", "13"),
                new FE("%_H", "13"),
                new FE("%I", "01"),
                new FE("%l", " 1"),
                new FE("%-I", "1"),
                new FE("%M", "05"),
                new FE("%-M", "5"),
                new FE("%S", "09"),
                new FE("%-S", "9"),
                new FE("%.f", ".123456"),
                new FE("%.3f", ".123"),
                new FE("%.6f", ".123456"),
                new FE("%.9f", ".123456000"),
                new FE("%p", "PM"),
                new FE("%P", "pm"),
                new FE("%T", "13:05:09"),
                new FE("%R", "13:05"),
                new FE("%X", "13:05:09"),
                new FE("%r", "01:05:09 PM"),
                new FE("%%", "%"),
                new FE("%H%%%M", "13%05"),
        };
    
        StringBuilder builder = new StringBuilder();
        T time = new T(13, 5, 9, 123456);
        for (FE r: tests) {
            builder.append("SELECT format_time('")
                    .append(r.format)
                    .append("', '")
                    .append(time).append("');\n")
                    .append(" r\n")
                    .append("------\n")
                    .append(" ").append(r.expected).append("\n")
                    .append("(1 row)\n\n");
        }
        this.qs(builder.toString());
    }

    @Test
    public void testFormatTimestamp() {
        FE[] tests = {
                new FE("%Y", "2024"),
                new FE("%y", "24"),
                new FE("%C", "20"),
                // --- Month ---
                new FE("%m", "03"),
                new FE("%-m", "3"),
                new FE("%b", "Mar"),
                new FE("%B", "March"),
                // --- Day ---
                new FE("%d", "05"),
                new FE("%-d", "5"),
                new FE("%e", " 5"),
                new FE("%j", "065"), // ordinal day
                // --- Weekday ---
                new FE("%a", "Tue"),
                new FE("%A", "Tuesday"),
                new FE("%u", "2"), // ISO weekday (Mon=1)
                new FE("%w", "2"), // Sunday=0
                // --- ISO week date ---
                new FE("%G", "2024"),
                new FE("%g", "24"),
                new FE("%V", "10"),
                // --- Hour (24h) ---
                new FE("%H", "13"),
                new FE("%k", "13"),
                new FE("%-H", "13"),
                new FE("%_H", "13"),
                // --- Hour (12h) ---
                new FE("%I", "01"),
                new FE("%l", " 1"),
                new FE("%-I", "1"),
                // --- Minute ---
                new FE("%M", "05"),
                new FE("%-M", "5"),
                // --- Second ---
                new FE("%S", "09"),
                new FE("%-S", "9"),
                new FE("%f", "123456000"),
                new FE("%.f", ".123456"),
                new FE("%.3f", ".123"),
                new FE("%.6f", ".123456"),
                new FE("%.9f", ".123456000"),
                // --- AM/PM ---
                new FE("%p", "PM"),
                new FE("%P", "pm"),
                // --- Unix timestamp ---
                // 2024‑03‑05 13:05:09 UTC → 1709643909
                new FE("%s", "1709643909"),
                // --- Composite formats ---
                new FE("%F", "2024-03-05"),
                new FE("%D", "03/05/24"),
                new FE("%x", "03/05/24"),
                new FE("%T", "13:05:09"),
                new FE("%R", "13:05"),
                new FE("%X", "13:05:09"),
                new FE("%r", "01:05:09 PM"),
                // --- Literal percent ---
                new FE("%%", "%"),
                new FE("%Y%%%m", "2024%03"),
                new FE("%H%%%M", "13%05"),
                new FE("%H%%M", "13%M"),
        };

        StringBuilder builder = new StringBuilder();
        for (FE r: tests) {
            builder.append("SELECT format_timestamp('")
                    .append(r.format)
                    .append("', TIMESTAMP '2024-03-05 13:05:09.123456');\n")
                    .append(" r\n")
                    .append("------\n")
                    .append(" ").append(r.expected).append("\n")
                    .append("(1 row)\n\n");
        }
        this.qs(builder.toString());
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
