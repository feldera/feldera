package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

public class TimeArithmeticTests extends SqlIoTest {
    @Test
    public void testTimeAddInterval() {
        this.qs("""
                SELECT '23:00:00'::time + INTERVAL '10' MINUTES;
                   time
                ----------
                 23:10:00
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '10' MINUTES;
                   time
                --------
                 00:10:00
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '-10' MINUTES;
                   time
                --------
                 23:50:00
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '2' DAYS;
                 time
                --------
                 00:00:00
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '-2' DAYS;
                 time
                --------
                 00:00:00
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '2' HOURS;
                 time
                --------
                 02:00:00
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '-2' HOURS;
                 time
                --------
                 22:00:00
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '2' SECONDS;
                 time
                --------
                 00:00:02
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '-2' SECONDS;
                 time
                --------
                 23:59:58
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '2' HOURS + INTERVAL '10' MINUTES + INTERVAL '10' SECONDS;
                 time
                --------
                 02:10:10
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '-2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '-10' SECONDS;
                 time
                --------
                 21:49:50
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '2' HOURS + INTERVAL '10' MINUTES + INTERVAL '-10' SECONDS;
                 time
                --------
                 02:09:50
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '-2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '10' SECONDS;
                 time
                --------
                 21:50:10
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '10' SECONDS;
                 time
                --------
                 01:50:10
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '-2' HOURS + INTERVAL '10' MINUTES + INTERVAL '-10' SECONDS;
                 time
                --------
                 22:09:50
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '-10' SECONDS;
                 time
                --------
                 01:49:50
                (1 row)
                
                SELECT '00:00:00'::time + INTERVAL '-2' HOURS + INTERVAL '10' MINUTES + INTERVAL '10' SECONDS;
                 time
                --------
                 22:10:10
                (1 row)
                
                SELECT '23:59:59'::time + INTERVAL '1' SECONDS;
                   time
                ----------
                 00:00:00
                (1 row)
                
                SELECT '23:59:59'::time + INTERVAL '1' HOURS;
                   time
                --------
                 00:59:59
                (1 row)
                
                SELECT '23:59:59'::time + INTERVAL '1' MINUTES;
                   time
                --------
                 00:00:59
                (1 row)
                
                SELECT '23:59:59'::time + INTERVAL '1' DAYS;
                   time
                --------
                 23:59:59
                (1 row)
                
                SELECT '23:59:59'::time + INTERVAL '1' HOURS + INTERVAL '1' MINUTES + INTERVAL '1' SECONDS;
                   time
                --------
                 01:01:00
                (1 row)
                
                SELECT '23:59:59'::time + INTERVAL '1' HOURS + INTERVAL '1' MINUTES + INTERVAL '1' SECONDS + INTERVAL '1' DAYS;
                   time
                --------
                 01:01:00
                (1 row)
                
                SELECT '23:59:59'::time + INTERVAL '1' HOURS + INTERVAL '1' MINUTES + INTERVAL '1' SECONDS + INTERVAL '1' DAYS + INTERVAL '1' HOURS;
                   time
                --------
                 02:01:00
                (1 row)
                
                SELECT INTERVAL '2' HOURS + '15:12:24'::time;
                 time
                ------
                 17:12:24
                (1 row)
                
                SELECT '12:34:56'::time + INTERVAL '1' YEAR;
                 time
                ------
                 12:34:56
                (1 row)
                
                SELECT '12:34:56'::time + INTERVAL '2' YEARS;
                 time
                ------
                 12:34:56
                (1 row)
                
                SELECT '12:34:56'::time + INTERVAL '2' MONTHS;
                 time
                ------
                 12:34:56
                (1 row)
                
                SELECT '12:34:56'::time + INTERVAL '2' MONTH;
                 time
                ------
                 12:34:56
                (1 row)
                
                SELECT '12:34:56'::time + INTERVAL '10 10:30' DAY TO MINUTE;
                 time
                ------
                 23:04:56
                (1 row)
                
                SELECT '00:00:01'::time + INTERVAL '-10' MINUTES;
                     time
                --------------
                 23:50:01
                (1 row)
                
                SELECT '00:00:01'::time + INTERVAL '-48:10' HOURS TO MINUTE;
                     time
                --------------
                 23:50:01
                (1 row)
                """
        );
    }

    @Test
    public void testTimeSubInterval() {
        this.qs("""
                SELECT '00:00:00'::time - INTERVAL '10' MINUTES;
                   time
                --------
                 23:50:00
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '-10' MINUTES;
                   time
                --------
                 00:10:00
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '2' DAYS;
                 time
                --------
                 00:00:00
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '-2' DAYS;
                 time
                --------
                 00:00:00
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '2' HOURS;
                 time
                --------
                 22:00:00
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '-2' HOURS;
                 time
                --------
                 02:00:00
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '2' SECONDS;
                 time
                --------
                 23:59:58
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '-2' SECONDS;
                 time
                --------
                 00:00:02
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '2' HOURS - INTERVAL '10' MINUTES - INTERVAL '10' SECONDS;
                 time
                --------
                 21:49:50
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '-2' HOURS - INTERVAL '-10' MINUTES - INTERVAL '-10' SECONDS;
                 time
                --------
                 02:10:10
                (1 row)
                
                SELECT '00:00:00'::time - INTERVAL '2' HOURS - INTERVAL '10' MINUTES - INTERVAL '-10' SECONDS;
                 time
                --------
                 21:50:10
                (1 row)
                
                SELECT '12:34:56'::time - INTERVAL '1' YEAR;
                 time
                ------
                 12:34:56
                (1 row)
                
                SELECT '12:34:56'::time - INTERVAL '2' YEARS;
                 time
                ------
                 12:34:56
                (1 row)
                
                SELECT '12:34:56'::time - INTERVAL '2' MONTHS;
                 time
                ------
                 12:34:56
                (1 row)
                
                SELECT '12:34:56'::time - INTERVAL '2' MONTH;
                 time
                ------
                 12:34:56
                (1 row)
                
                SELECT '12:34:56'::time - INTERVAL '10 10:30' DAY TO MINUTE;
                 time
                ------
                 02:04:56
                (1 row)
                """);
    }

    @Test @Ignore("https://github.com/feldera/feldera/issues/1658")
    public void testTimeAddInterval1() {
        this.qs("""
                SELECT '12:34:56'::time + INTERVAL '25' DAYS;
                   time
                ---------
                 12:34:56
                (1 row)
                """
        );
    }
}
