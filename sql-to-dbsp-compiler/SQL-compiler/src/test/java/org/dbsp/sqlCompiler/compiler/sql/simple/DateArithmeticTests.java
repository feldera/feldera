package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

public class DateArithmeticTests extends SqlIoTest {
    @Test
    public void testTimeAddInterval() {
        this.qs("""
                SELECT DATE '2024-01-01' + INTERVAL '10' MINUTES;
                   date
                ----------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-10' MINUTES;
                   date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2' DAYS;
                 date
                --------
                 2024-01-03
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-2' DAYS;
                 date
                --------
                 2023-12-30
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2' HOURS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-2' HOURS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-2' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2' HOURS + INTERVAL '10' MINUTES + INTERVAL '10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '-10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2' HOURS + INTERVAL '10' MINUTES + INTERVAL '-10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-2' HOURS + INTERVAL '10' MINUTES + INTERVAL '-10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '-10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-2' HOURS + INTERVAL '10' MINUTES + INTERVAL '10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '86400' SECONDS;
                   date
                ----------
                 2024-01-02
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '24' HOURS;
                   date
                --------
                 2024-01-02
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '1' DAYS;
                   date
                --------
                 2024-01-02
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '1' DAYS + INTERVAL '1' MINUTES + INTERVAL '1' SECONDS;
                   date
                --------
                 2024-01-02
                (1 row)
                
                SELECT INTERVAL '2' HOURS + DATE '2024-01-01';
                 date
                ------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '1' YEAR;
                 date
                ------
                 2025-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2' YEARS;
                 date
                ------
                 2026-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2-10' YEARS TO MONTHS;
                 date
                ------
                 2026-11-01
                (1 row)

                SELECT DATE '2024-01-01' + INTERVAL '2' MONTHS;
                 date
                ------
                 2024-03-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '2' MONTH;
                 date
                ------
                 2024-03-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '10 10:30' DAY TO MINUTE;
                 date
                ------
                 2024-01-11
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-10' MINUTES;
                     date
                --------------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' + INTERVAL '-48:10' HOURS TO MINUTE;
                     date
                --------------
                 2023-12-30
                (1 row)
                """
        );
    }

    @Test
    public void testTimeSubInterval() {
        this.qs("""
                SELECT DATE '2024-01-01' - INTERVAL '10' MINUTES;
                   date
                ----------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-10' MINUTES;
                   date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2' DAYS;
                 date
                --------
                 2023-12-30
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-2' DAYS;
                 date
                --------
                 2024-01-03
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2' HOURS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-2' HOURS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-2' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2' HOURS - INTERVAL '10' MINUTES - INTERVAL '10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-2' HOURS - INTERVAL '-10' MINUTES - INTERVAL '-10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2' HOURS - INTERVAL '10' MINUTES - INTERVAL '-10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-2' HOURS - INTERVAL '-10' MINUTES - INTERVAL '10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2' HOURS - INTERVAL '-10' MINUTES - INTERVAL '10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-2' HOURS - INTERVAL '10' MINUTES - INTERVAL '-10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2' HOURS - INTERVAL '-10' MINUTES - INTERVAL '-10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-2' HOURS - INTERVAL '10' MINUTES - INTERVAL '10' SECONDS;
                 date
                --------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '86400' SECONDS;
                   date
                ----------
                 2023-12-31
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '24' HOURS;
                   date
                --------
                 2023-12-31
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '1' DAYS;
                   date
                --------
                 2023-12-31
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '1' DAYS - INTERVAL '1' MINUTES - INTERVAL '1' SECONDS;
                   date
                --------
                 2023-12-31
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '1' YEAR;
                 date
                ------
                 2023-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2' YEARS;
                 date
                ------
                 2022-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2-10' YEARS TO MONTHS;
                 date
                ------
                 2021-03-01
                (1 row)

                SELECT DATE '2024-01-01' - INTERVAL '2' MONTHS;
                 date
                ------
                 2023-11-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '2' MONTH;
                 date
                ------
                 2023-11-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '10 10:30' DAY TO MINUTE;
                 date
                ------
                 2023-12-22
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-10' MINUTES;
                     date
                --------------
                 2024-01-01
                (1 row)
                
                SELECT DATE '2024-01-01' - INTERVAL '-48:10' HOURS TO MINUTE;
                     date
                --------------
                 2024-01-03
                (1 row)""");
    }
}
