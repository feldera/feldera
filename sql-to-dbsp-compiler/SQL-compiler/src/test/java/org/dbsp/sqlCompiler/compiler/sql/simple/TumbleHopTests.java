package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** TUMBLE and HOP windows for timestamps before 1970 and before the window offset.
 * Expected window starts validated with Postgres `date_bin`. */
public class TumbleHopTests extends SqlIoTest {
    @Test
    public void tumbleBefore1970() {
        var ccs = this.getCCS("""
                CREATE TABLE T (ts TIMESTAMP NOT NULL);
                CREATE VIEW V AS SELECT window_start FROM TABLE(
                  TUMBLE(TABLE T, DESCRIPTOR(ts), INTERVAL '1' MINUTE));""");
        ccs.step("INSERT INTO T VALUES ('1969-12-31 23:59:30');", """
                 window_start        | weight
                ------------------------------
                 1969-12-31 23:59:00 | 1""");
    }

    @Test
    public void tumbleBeforeOffset() {
        var ccs = this.getCCS("""
                CREATE TABLE T (ts TIMESTAMP NOT NULL);
                CREATE VIEW V AS SELECT window_start FROM TABLE(
                  TUMBLE(TABLE T, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '45' SECOND));""");
        ccs.step("INSERT INTO T VALUES ('1970-01-01 00:00:30');", """
                 window_start        | weight
                ------------------------------
                 1969-12-31 23:59:45 | 1""");
    }

    @Test
    public void tumbleGroupBefore1970() {
        var ccs = this.getCCS("""
                CREATE TABLE T (ts TIMESTAMP NOT NULL);
                CREATE VIEW V AS SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE, TIME '00:00:45') AS s
                FROM T GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE, TIME '00:00:45');""");
        ccs.step("INSERT INTO T VALUES ('1970-01-01 00:00:30');", """
                 s                   | weight
                ------------------------------
                 1969-12-31 23:59:45 | 1""");
    }

    @Test
    public void hopBefore1970() {
        var ccs = this.getCCS("""
                CREATE TABLE T (ts TIMESTAMP NOT NULL);
                CREATE VIEW V AS SELECT window_start FROM TABLE(
                  HOP(TABLE T, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '2' MINUTE));""");
        ccs.step("INSERT INTO T VALUES ('1969-12-31 23:59:30');", """
                 window_start        | weight
                ------------------------------
                 1969-12-31 23:58:00 | 1
                 1969-12-31 23:59:00 | 1""");
    }
}
