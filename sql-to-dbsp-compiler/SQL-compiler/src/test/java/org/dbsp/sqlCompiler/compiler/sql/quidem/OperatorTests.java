package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests from Calcite quidem test operator.iq */
public class OperatorTests extends SqlIoTest {
    @Test
    public void testRow() {
        this.qs("""
                select T.X[1] as x1 from (VALUES (ROW(ROW(3, 7), ROW(4, 8)))) as T(x, y);
                +----+
                | X1 |
                +----+
                |  3 |
                +----+
                (1 row)
                
                select T.X[CAST(2 AS BIGINT)] as x2 from (VALUES (ROW(ROW(3, 7), ROW(4, 8)))) as T(x, y);
                +----+
                | X2 |
                +----+
                |  7 |
                +----+
                (1 row)
                
                select T.Y[CAST(1 AS TINYINT)] as y1 from (VALUES (ROW(ROW(3, 7), ROW(4, 8)))) as T(x, y);
                +----+
                | Y1 |
                +----+
                |  4 |
                +----+
                (1 row)
                
                select T.Y[CAST(2 AS SMALLINT)] as y2 from (VALUES (ROW(ROW(3, 7), ROW(4, 8)))) as T(x, y);
                +----+
                | Y2 |
                +----+
                |  8 |
                +----+
                (1 row)""");
    }

    @Test
    public void testCeilFloor() {
        // From Calcite operator.iq
        this.qs("""
                select v,
                  case when b then 'ceil' else 'floor' end as op,
                  case when b then ceil(v to year) else floor(v to year) end as y,
                  case when b then ceil(v to quarter) else floor(v to quarter) end as q,
                  case when b then ceil(v to month) else floor(v to month) end as m,
                  case when b then ceil(v to week) else floor(v to week) end as w,
                  case when b then ceil(v to day) else floor(v to day) end as d
                from (values (date '2019-07-05')) as t(v),
                 (values false, true) as u(b);
                +------------+-------+------------+------------+------------+------------+------------+
                | V          | OP    | Y          | Q          | M          | W          | D          |
                +------------+-------+------------+------------+------------+------------+------------+
                | 2019-07-05 | ceil|   2020-01-01 | 2019-10-01 | 2019-08-01 | 2019-07-07 | 2019-07-05 |
                | 2019-07-05 | floor|  2019-01-01 | 2019-07-01 | 2019-07-01 | 2019-06-30 | 2019-07-05 |
                +------------+-------+------------+------------+------------+------------+------------+
                (2 rows)

                select v,
                  case when b then 'ceil' else 'floor' end as op,
                  case when b then ceil(v to year) else floor(v to year) end as y,
                  case when b then ceil(v to quarter) else floor(v to quarter) end as q,
                  case when b then ceil(v to month) else floor(v to month) end as m,
                  case when b then ceil(v to week) else floor(v to week) end as w,
                  case when b then ceil(v to day) else floor(v to day) end as d,
                  case when b then ceil(v to hour) else floor(v to hour) end as h,
                  case when b then ceil(v to minute) else floor(v to minute) end as mi,
                  case when b then ceil(v to second) else floor(v to second) end as s
                from (values (timestamp '2019-07-05 12:34:56')) as t(v),
                  (values false, true) as u(b);
                +---------------------+-------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+
                | V                   | OP    | Y                   | Q                   | M                   | W                   | D                   | H                   | MI                  | S                   |
                +---------------------+-------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+
                | 2019-07-05 12:34:56 | ceil|   2020-01-01 00:00:00 | 2019-10-01 00:00:00 | 2019-08-01 00:00:00 | 2019-07-07 00:00:00 | 2019-07-06 00:00:00 | 2019-07-05 13:00:00 | 2019-07-05 12:35:00 | 2019-07-05 12:34:56 |
                | 2019-07-05 12:34:56 | floor|  2019-01-01 00:00:00 | 2019-07-01 00:00:00 | 2019-07-01 00:00:00 | 2019-06-30 00:00:00 | 2019-07-05 00:00:00 | 2019-07-05 12:00:00 | 2019-07-05 12:34:00 | 2019-07-05 12:34:56 |
                +---------------------+-------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+
                (2 rows)
                
                select v,
                  case when b then 'ceil' else 'floor' end as op,
                  case when b then ceil(v to hour) else floor(v to hour) end as h,
                  case when b then ceil(v to minute) else floor(v to minute) end as mi,
                  case when b then ceil(v to second) else floor(v to second) end as s
                from (values (time '12:34:56.7')) as t(v),
                  (values false, true) as u(b);
                +------------+-------+------------+------------+------------+
                | V          | OP    | H          | MI         | S          |
                +------------+-------+------------+------------+------------+
                | 12:34:56.7 | ceil|   13:00:00.0 | 12:35:00.0 | 12:34:57.0 |
                | 12:34:56.7 | floor|  12:00:00.0 | 12:34:00.0 | 12:34:56.0 |
                +------------+-------+------------+------------+------------+
                (2 rows)""");
    }
}
