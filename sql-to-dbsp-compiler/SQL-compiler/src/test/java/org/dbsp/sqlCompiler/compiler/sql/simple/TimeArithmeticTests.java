package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

public class TimeArithmeticTests extends SqlIoTest {
    @Test
    public void testTimestampAddLongInterval() {
        this.qs("""
                SELECT TIMESTAMP '2024-01-01 10:23:45' + INTERVAL 10 MONTHS;
                        ts
                ---------------------
                 2024-11-01 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-01 10:23:45' + INTERVAL -10 MONTHS;
                        ts
                ---------------------
                 2023-03-01 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-01 10:23:45' - INTERVAL 10 MONTHS;
                        ts
                ---------------------
                 2023-03-01 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-01 10:23:45' - INTERVAL -10 MONTHS;
                        ts
                ---------------------
                 2024-11-01 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL 10 MONTHS;
                        ts
                ---------------------
                 2024-11-30 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL -10 MONTHS;
                        ts
                ---------------------
                 2023-03-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL -10 MONTHS;
                        ts
                ---------------------
                 2024-11-30 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL 1 MONTHS;
                        ts
                ---------------------
                 2024-02-29 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL 100 MONTHS;
                        ts
                ---------------------
                 2032-05-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL '8-4' YEARS TO MONTHS;
                        ts
                ---------------------
                 2032-05-31 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL -100 MONTHS;
                        ts
                ---------------------
                 2032-05-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL '-8-4' YEARS TO MONTHS;
                        ts
                ---------------------
                 2032-05-31 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL 100 MONTHS;
                        ts
                ---------------------
                 2015-09-30 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL '8-4' YEARS TO MONTHS;
                        ts
                ---------------------
                 2015-09-30 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL 10 YEARS;
                        ts
                ---------------------
                 2034-01-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL 10 YEARS;
                        ts
                ---------------------
                 2014-01-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL -10 YEARS;
                        ts
                ---------------------
                 2014-01-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL -10 YEARS;
                        ts
                ---------------------
                 2034-01-31 10:23:45
                (1 row)""");
    }

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

    @Test
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
