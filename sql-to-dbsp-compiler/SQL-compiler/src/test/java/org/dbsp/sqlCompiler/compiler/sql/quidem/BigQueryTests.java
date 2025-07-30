package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests from babel/src/test/resources/sql/big-query.iq */
public class BigQueryTests extends SqlIoTest {
    @Test
    public void testSafeCast() {
        this.qs("""
                -- SAFE_CAST(x AS type)
                -- Identical to CAST(), except it returns NULL instead of raising an error.
                WITH Casted AS (
                  SELECT SAFE_CAST('a' as int) as casted, 'a' as input, 'int' as as UNION ALL
                  SELECT SAFE_CAST('a' as varchar(1)), 'a', 'varchar(1)' UNION ALL
                  SELECT SAFE_CAST('2023-03-07' as DATE), DATE('2023-03-07'), 'date' UNION ALL
                  SELECT SAFE_CAST('2023-03-07a' as DATE), '2023-03-07a', 'date' UNION ALL
                  SELECT SAFE_CAST(0 as BOOLEAN), 0, 'boolean'
                )
                SELECT
                  *
                FROM Casted;
                +------------+-------------+------------+
                | casted     | input       | as         |
                +------------+-------------+------------+
                |NULL        | a| int|
                | a| a| varchar(1)|
                | 2023-03-07| 2023-03-07| date|
                |NULL        | 2023-03-07a| date|
                | FALSE| 0| boolean|
                +------------+-------------+------------+
                (5 rows)
                
                WITH Casted AS (
                  SELECT SAFE_CAST('12:12:11' as TIME) as casted,
                   '12:12:11' as input, 'time' as as UNION ALL
                  SELECT SAFE_CAST('12:12:11a' as TIME), '12:12:11a', 'time'
                )
                SELECT
                  *
                FROM Casted;
                +----------+-----------+-----+
                | casted   | input     | as  |
                +----------+-----------+-----+
                | 12:12:11 | 12:12:11| time|
                |          | 12:12:11a| time|
                +----------+-----------+-----+
                (2 rows)
                
                WITH Casted AS (
                  SELECT SAFE_CAST(TRUE as BOOLEAN) as casted, 'true' as input,
                   'boolean' as as UNION ALL
                  SELECT SAFE_CAST(FALSE as BOOLEAN) as casted, 'false' as input,
                   'boolean' as as
                )
                SELECT
                  *
                FROM Casted;
                +--------+-------+--------+
                | casted | input | as     |
                +--------+-------+--------+
                | true   | true| boolean|
                | false  | false| boolean|
                +--------+-------+--------+
                (2 rows)
                
                WITH Casted AS (
                  SELECT SAFE_CAST(interval '12' month as interval year) as casted,
                   'interval 1 month' as input,
                   'interval year' as as UNION ALL
                   SELECT SAFE_CAST('a' as interval year), 'a',
                     'interval year' UNION ALL
                   SELECT SAFE_CAST(null as interval year), 'null', 'interval year'
                )
                SELECT
                  *
                FROM Casted;
                +--------+------------------+--------------+
                | casted | input            | as           |
                +--------+------------------+--------------+
                | 1 year | interval 1 month| interval year|
                |        | a| interval year|
                |        | null| interval year|
                +--------+------------------+--------------+
                (3 rows)
                
                WITH Casted AS (
                  SELECT SAFE_CAST(interval '1:1' hour to minute as interval minute to second)
                   as casted, 'interval 1:1 hour to minute' as input,
                    'interval minute to second' as as UNION ALL
                  SELECT SAFE_CAST('a' as interval minute to second),
                   'a', 'interval minute to second'
                )
                SELECT
                  *
                FROM Casted;
                +---------------+-----------------------------+--------------------------+
                | casted        | input                       | as                       |
                +---------------+-----------------------------+--------------------------+
                | 61 mins       | interval 1:1 hour to minute| interval minute to second|
                |               | a| interval minute to second|
                +---------------+-----------------------------+--------------------------+
                (2 rows)
                
                WITH Casted AS (
                  SELECT SAFE_CAST('true' as BIGINT) as casted, 'true' as input,
                    'bigint' as as UNION ALL
                  SELECT SAFE_CAST(1.0 as BIGINT), '1.0', 'bigint' UNION ALL
                  SELECT SAFE_CAST(1 as BIGINT), '1', 'bigint' -- UNION ALL
                  -- SELECT SAFE_CAST(SAFE_CAST(TRUE AS BOOLEAN) AS BIGINT), 'TRUE', 'bigint'
                  -- deleted, since our type system does not seem to support cast of bool to bigint
                )
                SELECT
                  *
                FROM Casted;
                +--------+-------+-------+
                | casted | input | as    |
                +--------+-------+-------+
                |        | true| bigint|
                |      1 | 1.0| bigint|
                |      1 | 1| bigint|
                +--------+-------+-------+
                (3 rows)""");
    }

    @Test
    public void testRegexpReplace() {
        this.qs("""
                -- REGEXP_REPLACE(value, regexp, replacement)
                --
                -- Returns a STRING where all substrings of value that match regexp are replaced with replacement.
                -- Supports backslashed-escaped digits in replacement argument for corresponding capturing groups
                -- in regexp. Returns an exception if regex is invalid.
                SELECT REGEXP_REPLACE('qw1e1rt1y', '1', 'X');
                +----------+
                | EXPR$0   |
                +----------+
                | qwXeXrtXy|
                +----------+
                (1 row)
                
                SELECT REGEXP_REPLACE('a0b1c2d3', 'a|d', 'X');
                +---------+
                | EXPR$0  |
                +---------+
                | X0b1c2X3|
                +---------+
                (1 row)
                
                SELECT REGEXP_REPLACE('1=00--20=0', '(-)', '#');
                +-----------+
                | EXPR$0    |
                +-----------+
                | 1=00##20=0|
                +-----------+
                (1 row)""");
    }

    @Test
    public void negativeTestParseDate() {
        // These have been adapted, since these functions are not compatible with BigQuery.

        // pattern insufficient to build a time
        this.qf("SELECT PARSE_TIME('%S:%I:%M', '07:01:00')",
            "Invalid format in PARSE_TIME");
        // pattern insufficient for hour
        this.qf("SELECT PARSE_TIMESTAMP('%a %b %e %I:%M:%S %Y', 'Thu Dec 25 07:30:00 2008')",
            "Invalid format in PARSE_TIMESTAMP");
        this.qs("""
                -- 30 hour is out of range
                SELECT PARSE_TIME('%S:%I:%M', '07:30:00');
                 r
                ---
                 NULL
                (1 row)
                
                -- 2008 is not parsed
                SELECT PARSE_DATE('%A %b %e', 'Thursday Dec 25 2008');
                 r
                ---
                 NULL
                (1 row)
                
                -- %Y is not parsing a year
                SELECT PARSE_DATE('%Y %A %b %e', 'Thursday Dec 25 2008');
                 r
                ---
                 NULL
                (1 row)
                
                -- not parsing everything
                SELECT PARSE_TIME('%I:%M', '07:30:00');
                 r
                ---
                 NULL
                (1 row)
                
                -- string parsed is too short (after cast from integer)
                SELECT PARSE_TIMESTAMP('%a %b %e %I:%M:%S', 20);
                 r
                ---
                 NULL
                (1 row)""");
    }

    @Test
    public void testParseDate() {
        // These have been adapted, since these functions are not compatible with BigQuery.
        this.qs("""
                SELECT PARSE_DATE('%A %b %e %Y', 'Thursday Dec 25 2008');
                +------------+
                | EXPR$0     |
                +------------+
                | 2008-12-25 |
                +------------+
                (1 row)
                
                SELECT PARSE_DATE('%F', '2000-12-30');
                +------------+
                | EXPR$0     |
                +------------+
                | 2000-12-30 |
                +------------+
                (1 row)
                
                SELECT PARSE_DATE('%x', '12/25/08') AS parsed;
                +------------+
                | parsed     |
                +------------+
                | 2008-12-25 |
                +------------+
                (1 row)
                
                SELECT PARSE_DATE('%Y%m%d', '20081225') AS parsed;
                +------------+
                | parsed     |
                +------------+
                | 2008-12-25 |
                +------------+
                (1 row)
                
                SELECT PARSE_TIME('%I:%M:%S %p', '07:30:00 am');
                +----------+
                | EXPR$0   |
                +----------+
                | 07:30:00 |
                +----------+
                (1 row)
                
                SELECT PARSE_TIME('%T', '07:30:00');
                +----------+
                | EXPR$0   |
                +----------+
                | 07:30:00 |
                +----------+
                (1 row)
                
                SELECT PARSE_TIME('%H:%M:%S', '15:00:00') as parsed_time;
                +-------------+
                | parsed_time |
                +-------------+
                | 15:00:00    |
                +-------------+
                (1 row)
                
                SELECT PARSE_TIME('%I:%M:%S %p', '2:23:38 pm') AS parsed_time;
                +-------------+
                | parsed_time |
                +-------------+
                | 14:23:38    |
                +-------------+
                (1 row)
                
                SELECT PARSE_TIME(NULL, '2:23');
                +-------+
                |     p |
                +-------+
                |       |
                +-------+
                (1 row)
                
                SELECT PARSE_TIME('%T', NULL);
                +-------+
                |     p |
                +-------+
                |       |
                +-------+
                (1 row)
                
                SELECT PARSE_TIMESTAMP('%a %b %e %H:%M:%S %Y', 'Thu Dec 25 07:30:00 2008');
                +---------------------+
                | EXPR$0              |
                +---------------------+
                | 2008-12-25 07:30:00 |
                +---------------------+
                (1 row)
                
                SELECT PARSE_TIMESTAMP('%c', 'Thu Dec 25 07:30:00 2008');
                +---------------------+
                | EXPR$0              |
                +---------------------+
                | 2008-12-25 07:30:00 |
                +---------------------+
                (1 row)
                
                SELECT PARSE_TIMESTAMP('%c', 'Thu Dec 25 07:30:00 2008') AS parsed;
                +---------------------+
                | parsed              |
                +---------------------+
                | 2008-12-25 07:30:00 |
                +---------------------+
                (1 row)""");
    }
}
