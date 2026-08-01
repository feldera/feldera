package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

// Based on stream.iq from Calcite
public class SessionTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String sql = """
                CREATE TABLE orders(
                   rowtime TIMESTAMP NOT NULL,
                   id      INTEGER,
                   product VARCHAR,
                   units   INTEGER
                );

                INSERT INTO orders VALUES
                ('2015-02-15 10:15:00', 1, 'paint', 10),
                ('2015-02-15 10:24:15', 2, 'paper', 5),
                ('2015-02-15 10:24:45', 3, 'brush', 12),
                ('2015-02-15 10:58:00', 4, 'paint', 3),
                ('2015-02-15 11:10:00', 5, 'paint', 3);

                CREATE TABLE events(
                   ts  TIMESTAMP,
                   uid VARCHAR
                );

                INSERT INTO events VALUES
                (NULL,                  'a'),
                ('2020-01-01 10:00:00', 'a'),
                ('2020-01-01 10:05:00', 'a'),
                ('2020-01-01 10:30:00', 'a'),
                ('2020-01-01 10:00:00', 'b'),
                ('2020-01-01 10:15:00', 'b'),
                ('2020-01-01 10:00:00', NULL),
                ('2020-01-01 10:14:59', NULL),
                ('2020-01-01 10:35:00', NULL);

                -- Columns before, between and after the two columns that
                -- SESSION uses, to check that the rewrite keeps them in place
                CREATE TABLE surrounded(
                   before  INTEGER,
                   ts      TIMESTAMP NOT NULL,
                   between VARCHAR,
                   k       VARCHAR,
                   after   INTEGER
                );

                INSERT INTO surrounded VALUES
                (1, '2020-01-01 10:00:00', 'x', 'k1', 100),
                (2, '2020-01-01 10:05:00', 'y', 'k1', 200),
                (3, '2020-01-01 10:30:00', 'z', 'k1', 300),
                (4, '2020-01-01 10:00:00', 'w', 'k2', 400);""";
        compiler.submitStatementsForCompilation(sql);
    }

    // Expected output taken from Calcite's stream.iq and validated with an
    // external Python implementation of the SESSION semantics
    @Test
    public void testSession() {
        this.qst("""
                SELECT * FROM TABLE(SESSION(TABLE ORDERS, DESCRIPTOR(ROWTIME), DESCRIPTOR(PRODUCT), INTERVAL '20' MINUTE));
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | ROWTIME             | ID | PRODUCT | UNITS | window_start            | window_end              |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | 2015-02-15 10:15:00 |  1 | paint   |    10 | 2015-02-15 10:15:00.000 | 2015-02-15 10:35:00.000 |
                | 2015-02-15 10:24:15 |  2 | paper   |     5 | 2015-02-15 10:24:15.000 | 2015-02-15 10:44:15.000 |
                | 2015-02-15 10:24:45 |  3 | brush   |    12 | 2015-02-15 10:24:45.000 | 2015-02-15 10:44:45.000 |
                | 2015-02-15 10:58:00 |  4 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                | 2015-02-15 11:10:00 |  5 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                (5 rows)

                SELECT * FROM TABLE(
                  SESSION(
                    DATA => TABLE ORDERS,
                    TIMECOL => DESCRIPTOR(ROWTIME),
                    KEY => DESCRIPTOR(PRODUCT),
                    SIZE => INTERVAL '20' MINUTE));
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | ROWTIME             | ID | PRODUCT | UNITS | window_start            | window_end              |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | 2015-02-15 10:15:00 |  1 | paint   |    10 | 2015-02-15 10:15:00.000 | 2015-02-15 10:35:00.000 |
                | 2015-02-15 10:24:15 |  2 | paper   |     5 | 2015-02-15 10:24:15.000 | 2015-02-15 10:44:15.000 |
                | 2015-02-15 10:24:45 |  3 | brush   |    12 | 2015-02-15 10:24:45.000 | 2015-02-15 10:44:45.000 |
                | 2015-02-15 10:58:00 |  4 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                | 2015-02-15 11:10:00 |  5 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                (5 rows)

                SELECT * FROM TABLE(SESSION((SELECT * FROM ORDERS), DESCRIPTOR(ROWTIME), DESCRIPTOR(PRODUCT), INTERVAL '20' MINUTE));
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | ROWTIME             | ID | PRODUCT | UNITS | window_start            | window_end              |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | 2015-02-15 10:15:00 |  1 | paint   |    10 | 2015-02-15 10:15:00.000 | 2015-02-15 10:35:00.000 |
                | 2015-02-15 10:24:15 |  2 | paper   |     5 | 2015-02-15 10:24:15.000 | 2015-02-15 10:44:15.000 |
                | 2015-02-15 10:24:45 |  3 | brush   |    12 | 2015-02-15 10:24:45.000 | 2015-02-15 10:44:45.000 |
                | 2015-02-15 10:58:00 |  4 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                | 2015-02-15 11:10:00 |  5 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                (5 rows)""");
    }

    // Without a key descriptor all rows share one session timeline.
    // Expected output validated with an external Python implementation of the
    // SESSION semantics; Calcite cannot execute the keyless form
    @Test
    public void testSessionNoKey() {
        this.qst("""
                SELECT * FROM TABLE(SESSION(TABLE ORDERS, DESCRIPTOR(ROWTIME), INTERVAL '20' MINUTE));
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | ROWTIME             | ID | PRODUCT | UNITS | window_start            | window_end              |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | 2015-02-15 10:15:00 |  1 | paint   |    10 | 2015-02-15 10:15:00.000 | 2015-02-15 10:44:45.000 |
                | 2015-02-15 10:24:15 |  2 | paper   |     5 | 2015-02-15 10:15:00.000 | 2015-02-15 10:44:45.000 |
                | 2015-02-15 10:24:45 |  3 | brush   |    12 | 2015-02-15 10:15:00.000 | 2015-02-15 10:44:45.000 |
                | 2015-02-15 10:58:00 |  4 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                | 2015-02-15 11:10:00 |  5 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                (5 rows)

                SELECT * FROM TABLE(
                  SESSION(
                    DATA => TABLE ORDERS,
                    TIMECOL => DESCRIPTOR(ROWTIME),
                    SIZE => INTERVAL '20' MINUTE));
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | ROWTIME             | ID | PRODUCT | UNITS | window_start            | window_end              |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                | 2015-02-15 10:15:00 |  1 | paint   |    10 | 2015-02-15 10:15:00.000 | 2015-02-15 10:44:45.000 |
                | 2015-02-15 10:24:15 |  2 | paper   |     5 | 2015-02-15 10:15:00.000 | 2015-02-15 10:44:45.000 |
                | 2015-02-15 10:24:45 |  3 | brush   |    12 | 2015-02-15 10:15:00.000 | 2015-02-15 10:44:45.000 |
                | 2015-02-15 10:58:00 |  4 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                | 2015-02-15 11:10:00 |  5 | paint   |     3 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                +---------------------+----+---------+-------+-------------------------+-------------------------+
                (5 rows)""");
    }

    // Rows with a NULL timestamp are dropped; NULL keys sessionize like any
    // other key value; rows exactly gap apart belong to different sessions.
    // Expected output validated with an external Python implementation of the
    // SESSION semantics
    @Test
    public void testSessionNulls() {
        this.qst("""
                SELECT * FROM TABLE(SESSION(TABLE EVENTS, DESCRIPTOR(TS), DESCRIPTOR(UID), INTERVAL '15' MINUTE));
                +---------------------+------+-------------------------+-------------------------+
                | TS                  | UID  | window_start            | window_end              |
                +---------------------+------+-------------------------+-------------------------+
                | 2020-01-01 10:00:00 | a    | 2020-01-01 10:00:00.000 | 2020-01-01 10:20:00.000 |
                | 2020-01-01 10:05:00 | a    | 2020-01-01 10:00:00.000 | 2020-01-01 10:20:00.000 |
                | 2020-01-01 10:30:00 | a    | 2020-01-01 10:30:00.000 | 2020-01-01 10:45:00.000 |
                | 2020-01-01 10:00:00 | b    | 2020-01-01 10:00:00.000 | 2020-01-01 10:15:00.000 |
                | 2020-01-01 10:15:00 | b    | 2020-01-01 10:15:00.000 | 2020-01-01 10:30:00.000 |
                | 2020-01-01 10:00:00 |NULL  | 2020-01-01 10:00:00.000 | 2020-01-01 10:29:59.000 |
                | 2020-01-01 10:14:59 |NULL  | 2020-01-01 10:00:00.000 | 2020-01-01 10:29:59.000 |
                | 2020-01-01 10:35:00 |NULL  | 2020-01-01 10:35:00.000 | 2020-01-01 10:50:00.000 |
                +---------------------+------+-------------------------+-------------------------+
                (8 rows)""");
    }

    // The timestamp and the key are neither the first nor the last column, so
    // the rewrite has to carry the columns around them through unchanged.
    // Expected output validated with an external Python implementation of the
    // SESSION semantics
    @Test
    public void testSessionSurroundedColumns() {
        this.qst("""
                SELECT * FROM TABLE(SESSION(TABLE SURROUNDED, DESCRIPTOR(TS), DESCRIPTOR(K), INTERVAL '15' MINUTE));
                +--------+---------------------+---------+----+-------+-------------------------+-------------------------+
                | BEFORE | TS                  | BETWEEN | K  | AFTER | window_start            | window_end              |
                +--------+---------------------+---------+----+-------+-------------------------+-------------------------+
                |      1 | 2020-01-01 10:00:00 | x       | k1 |   100 | 2020-01-01 10:00:00.000 | 2020-01-01 10:20:00.000 |
                |      2 | 2020-01-01 10:05:00 | y       | k1 |   200 | 2020-01-01 10:00:00.000 | 2020-01-01 10:20:00.000 |
                |      3 | 2020-01-01 10:30:00 | z       | k1 |   300 | 2020-01-01 10:30:00.000 | 2020-01-01 10:45:00.000 |
                |      4 | 2020-01-01 10:00:00 | w       | k2 |   400 | 2020-01-01 10:00:00.000 | 2020-01-01 10:15:00.000 |
                +--------+---------------------+---------+----+-------+-------------------------+-------------------------+
                (4 rows)""");
    }

    @Test
    public void testSessionNegative() {
        // The gap must be an interval
        this.statementsFailingInCompilation("""
                CREATE VIEW V AS SELECT * FROM TABLE(
                  SESSION(TABLE ORDERS, DESCRIPTOR(ROWTIME), DESCRIPTOR(PRODUCT), 10))""",
                "Cannot apply 'SESSION' to arguments");
        // The time column must have a timestamp type
        this.statementsFailingInCompilation("""
                CREATE VIEW V AS SELECT * FROM TABLE(
                  SESSION(TABLE ORDERS, DESCRIPTOR(PRODUCT), DESCRIPTOR(ID), INTERVAL '20' MINUTE))""",
                "Cannot apply 'SESSION' to arguments");
        // The descriptors must name existing columns
        this.statementsFailingInCompilation("""
                CREATE VIEW V AS SELECT * FROM TABLE(
                  SESSION(TABLE ORDERS, DESCRIPTOR(NO_SUCH_COLUMN), DESCRIPTOR(PRODUCT), INTERVAL '20' MINUTE))""",
                "Unknown identifier");
        // Missing gap argument
        this.statementsFailingInCompilation("""
                CREATE VIEW V AS SELECT * FROM TABLE(
                  SESSION(TABLE ORDERS, DESCRIPTOR(ROWTIME)))""",
                "Invalid number of arguments");
    }

    // Aggregation on top of SESSION, the typical use of the table function.
    // Expected output validated with an external Python implementation of the
    // SESSION semantics
    @Test
    public void testSessionAggregate() {
        this.qst("""
                SELECT PRODUCT, COUNT(*) AS event_count, window_start, window_end
                FROM TABLE(SESSION(TABLE ORDERS, DESCRIPTOR(ROWTIME), DESCRIPTOR(PRODUCT), INTERVAL '20' MINUTE))
                GROUP BY PRODUCT, window_start, window_end;
                +---------+-------------+-------------------------+-------------------------+
                | PRODUCT | event_count | window_start            | window_end              |
                +---------+-------------+-------------------------+-------------------------+
                | brush   |           1 | 2015-02-15 10:24:45.000 | 2015-02-15 10:44:45.000 |
                | paint   |           1 | 2015-02-15 10:15:00.000 | 2015-02-15 10:35:00.000 |
                | paint   |           2 | 2015-02-15 10:58:00.000 | 2015-02-15 11:30:00.000 |
                | paper   |           1 | 2015-02-15 10:24:15.000 | 2015-02-15 10:44:15.000 |
                +---------+-------------+-------------------------+-------------------------+
                (4 rows)""");
    }
}
