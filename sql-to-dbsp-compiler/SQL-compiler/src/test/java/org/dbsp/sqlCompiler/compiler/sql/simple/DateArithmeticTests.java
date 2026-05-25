package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
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

    @Test
    public void testDateSub() {
        this.qs("""
                SELECT (date '2023-12-01' - date '2022-12-01') days;
                 diff
                ------
                 365 days
                (1 row)

                SELECT (date '2023-12-01' - date '2022-12-01') month;
                 diff
                ------
                 1 year
                (1 row)

                SELECT (date '2022-12-01' - date '2023-12-01') month;
                 diff
                ------
                 1 year ago
                (1 row)

                SELECT (date '2023-12-01' - date '2023-01-01') month;
                 diff
                ------
                 11 months
                (1 row)

                SELECT (date '2023-01-01' - date '2023-12-01') month;
                 diff
                ------
                 11 months ago
                (1 row)

                SELECT (date '2023-01-01' - date '2023-12-01') days;
                 diff
                ------
                 334 days ago
                (1 row)""");
        this.qs("""
                SELECT (TIMESTAMP '2023-01-01 10:00:00' - TIMESTAMP '2023-12-01 10:00:00') month;
                 diff
                ------
                 11 months ago
                (1 row)""");
    }

    @Test
    public void makeDateTests() {
        this.qs("""
                SELECT MAKE_DATE(2020, 1, 1);
                 r
                ---
                 2020-01-01
                (1 row)
                
                SELECT MAKE_DATE(CAST(2020 AS UNSIGNED), 1, 1);
                 r
                ---
                 2020-01-01
                (1 row)
                
                SELECT MAKE_DATE(1, 1, 1);
                 r
                ---
                 0001-01-01
                (1 row)
                
                SELECT MAKE_DATE(NULL, 1, 1);
                 r
                ---
                NULL
                (1 row)
                
                SELECT MAKE_DATE(2020, 0, 1);
                 r
                ---
                NULL
                (1 row)
                
                SELECT MAKE_DATE(0, 1, 1);
                 r
                ---
                NULL
                (1 row)
                
                SELECT MAKE_DATE(20202, 10, 10);
                 r
                ---
                NULL
                (1 row)
                
                SELECT MAKE_DATE(2023, 2, 29);
                 r
                ---
                NULL
                (1 row)
                
                SELECT MAKE_DATE(9999, 12, 31);
                 r
                ---
                 9999-12-31
                (1 row)""");
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT MAKE_DATE('a', 1.2, 3);",
                "Cannot apply 'make_date' to arguments of type 'make_date(<CHAR(1)>, <DECIMAL(2, 1)>, <INTEGER>)'. " +
                        "Supported form(s): MAKE_DATE(<INTEGER>, <INTEGER>, <INTEGER>)");
    }

    @Test
    public void makeTimestampTests() {
        this.qs("""
                SELECT MAKE_TIMESTAMP(2020, 1, 1, 10, 0, 0);
                 r
                ---
                 2020-01-01 10:00:00
                (1 row)
                
                SELECT MAKE_TIMESTAMP(2020, 1, 1, 22, 58, 59);
                 r
                ---
                 2020-01-01 22:58:59
                (1 row)
                
                SELECT MAKE_TIMESTAMP(2020, 1, 1, 22, 58, 59.99999);
                 r
                ---
                 2020-01-01 22:58:59.99999
                (1 row)
                
                SELECT MAKE_TIMESTAMP(2020, 0, 1, 10, 0, 0);
                 r
                ---
                NULL
                (1 row)
                
                SELECT MAKE_TIMESTAMP(2020, 1, 1, 10, 0, -1);
                 r
                ---
                NULL
                (1 row)

                SELECT MAKE_TIMESTAMP(2023, 2, 29, 10, 1, 1);
                 r
                ---
                NULL
                (1 row)
                
                SELECT MAKE_TIMESTAMP(2023, 2, 29, 10, 1, NULL);
                 r
                ---
                NULL
                (1 row)""");
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT MAKE_TIMESTAMP('a', 1.2, 3, 1, 2, 3);",
                "Cannot apply 'make_timestamp' to arguments of type 'make_timestamp(<CHAR(1)>, <DECIMAL(2, 1)>, <INTEGER>, <INTEGER>, <INTEGER>, <INTEGER>)'. " +
                        "Supported form(s): MAKE_TIMESTAMP(<INTEGER>, <INTEGER>, <INTEGER>, <INTEGER>, <INTEGER>, <NUMERIC>)");
    }
}
