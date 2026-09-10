package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Issue 7093: waterlines for MIN and ARG_MIN, both for append_only=true and append_only=false. */
public class MinWaterlineIncrementalTests extends StreamingTestBase {
    /** Events whose timestamps arrive at most one hour out of order. */
    static final String EVENTS_COLUMNS = """
            CREATE TABLE events (
                id INT NOT NULL,
                payload VARCHAR,
                ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
            )""";
    /** Events that may be deleted. */
    static final String EVENTS = EVENTS_COLUMNS + ";\n";
    /** Events that are never deleted. */
    static final String APPEND_ONLY_EVENTS = EVENTS_COLUMNS + " WITH ('append_only' = 'true');\n";

    /** Append-only events with two timestamp columns, each with its own waterline. */
    static final String APPEND_ONLY_TWO_TIMESTAMPS = """
            CREATE TABLE events (
                id INT NOT NULL,
                ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
                ts2 TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
            ) WITH ('append_only' = 'true');
            """;

    /** Asserts that the circuit contains {@code count} integrate_trace_retain_keys operators,
     * none of them attached to a chain aggregate. */
    static void checkDownstreamRetainKeys(CompilerCircuit cc, int count) {
        cc.visit(new CircuitVisitor(cc.compiler) {
            final List<String> sources = new ArrayList<>();

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                Assert.assertFalse(operator.left().operator.is(DBSPChainAggregateOperator.class));
                this.sources.add(operator.left().operator.getClass().getSimpleName() + " " + operator.left().operator.getIdString());
            }

            @Override
            public void endVisit() {
                Assert.assertEquals("retain_keys on " + this.sources, count, this.sources.size());
            }
        });
    }

    /** The MIN of a column with a waterline, computed over an append-only input;
     * the output inherits the input waterline. */
    @Test
    public void issue7093Chain() {
        CompilerCircuitStream ccs = this.getCCS(APPEND_ONLY_EVENTS + """
                CREATE LOCAL VIEW first_event AS
                SELECT id, ARG_MIN(payload, ts) AS payload, MIN(ts) AS ts FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM first_event GROUP BY TIMESTAMP_TRUNC(ts, HOUR);""")
                .compactAfterEachStep();
        // The hourly aggregate garbage-collects its input and its output by hour.
        checkDownstreamRetainKeys(ccs, 2);
        ccs.step("""
                INSERT INTO events VALUES (1, 'a', '2020-01-01 10:30:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 10:00:00 | 1      | 1""");
        // Waterline = 10:30 - 1 hour = 09:30, so hour 09:00 is live.
        // The earlier row moves group 1 from hour 10 to hour 9.
        ccs.step("""
                INSERT INTO events VALUES (1, 'b', '2020-01-01 09:45:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 10:00:00 | 1      | -1
                 2020-01-01 09:00:00 | 1      | 1""");
        // Waterline = 11:00 after this step; hours 9 and 10 fall below it
        // and are garbage-collected.
        ccs.step("""
                INSERT INTO events VALUES (2, 'c', '2020-01-01 12:00:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 12:00:00 | 2      | 1""");
        // 11:30 passes the filter and moves group 2 to hour 11, the hour at the waterline.
        ccs.step("""
                INSERT INTO events VALUES (2, 'd', '2020-01-01 11:30:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 12:00:00 | 2      | -1
                 2020-01-01 11:00:00 | 2      | 1""");
        // 10:59 is late and dropped; 11:00 is later than group 1's minimum.
        ccs.step("""
                INSERT INTO events VALUES
                    (3, 'e', '2020-01-01 10:59:00'),
                    (1, 'f', '2020-01-01 11:00:00');""", """
                 hour                | max_id | weight
                ---------------------------------------""");
    }

    /** MAX chains and mixed aggregates retract rows whose compared value may lie below the
     * waterline, so their output does not have a waterline. */
    @Test
    public void issue7093ChainNoWaterline() {
        // The retracted old maximum may be below the waterline.
        checkDownstreamRetainKeys(this.getCC(APPEND_ONLY_EVENTS + """
                CREATE LOCAL VIEW per_id AS SELECT id, MAX(ts) AS ts FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM per_id GROUP BY TIMESTAMP_TRUNC(ts, HOUR);"""), 0);
        // A MAX in the same chain changes rows whose minimum may be below the waterline.
        checkDownstreamRetainKeys(this.getCC(APPEND_ONLY_EVENTS + """
                CREATE LOCAL VIEW per_id AS SELECT id, MIN(ts) AS ts, MAX(ts) AS last FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(last) AS last
                FROM per_id GROUP BY TIMESTAMP_TRUNC(ts, HOUR);"""), 0);
        // A change driven by one column retracts a row whose other minimum may be
        // below that column's waterline.
        checkDownstreamRetainKeys(this.getCC(APPEND_ONLY_TWO_TIMESTAMPS + """
                CREATE LOCAL VIEW per_id AS SELECT id, MIN(ts) AS ts, MIN(ts2) AS ts2 FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(ts2) AS last
                FROM per_id GROUP BY TIMESTAMP_TRUNC(ts, HOUR);"""), 0);
        // The minimized column has no waterline, although another column of the input does.
        checkDownstreamRetainKeys(this.getCC("""
                CREATE TABLE events (
                    id INT NOT NULL,
                    amount INT NOT NULL,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
                ) WITH ('append_only' = 'true');
                CREATE LOCAL VIEW per_id AS SELECT id, MIN(amount) AS amount FROM events GROUP BY id;
                CREATE VIEW by_amount AS
                SELECT amount / 100 AS bucket, MAX(id) AS max_id FROM per_id GROUP BY amount / 100;"""), 0);
    }

    /** NULL timestamps pass the lateness filter and lose to any timestamp in a MIN, so a
     * group's NULL minimum is retracted when its first timestamped row arrives.  The hourly
     * view keeps its NULL group, since a NULL counts as at or above the waterline. */
    @Test
    public void issue7093NullTimestamps() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TABLE events (
                    id INT NOT NULL,
                    payload VARCHAR,
                    ts TIMESTAMP LATENESS INTERVAL 1 HOUR
                ) WITH ('append_only' = 'true');
                CREATE LOCAL VIEW first_event AS
                SELECT id, ARG_MIN(payload, ts) AS payload, MIN(ts) AS ts FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM first_event GROUP BY TIMESTAMP_TRUNC(ts, HOUR);""")
                .compactAfterEachStep();
        checkDownstreamRetainKeys(ccs, 2);
        // Group 1 has only a NULL timestamp, so its minimum is NULL.
        ccs.step("""
                INSERT INTO events VALUES
                    (1, 'a', NULL),
                    (2, 'b', '2020-01-01 10:30:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 NULL                | 1      | 1
                 2020-01-01 10:00:00 | 2      | 1""");
        // Waterline = 09:30.  The first timestamp of group 1 replaces its NULL minimum.
        ccs.step("""
                INSERT INTO events VALUES (1, 'c', '2020-01-01 09:45:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 NULL                | 1      | -1
                 2020-01-01 09:00:00 | 1      | 1""");
        // A NULL timestamp never lowers a minimum; a new all-NULL group appears.
        ccs.step("""
                INSERT INTO events VALUES
                    (1, 'd', NULL),
                    (3, 'e', NULL);""", """
                 hour                | max_id | weight
                ---------------------------------------
                 NULL                | 3      | 1""");
        // Waterline = 11:00 after this step; hours 9 and 10 are garbage-collected,
        // the NULL group is not.
        ccs.step("""
                INSERT INTO events VALUES (4, 'f', '2020-01-01 12:00:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 12:00:00 | 4      | 1""");
        // 11:30 passes the filter and replaces group 3's NULL minimum; 10:59 is late.
        ccs.step("""
                INSERT INTO events VALUES
                    (3, 'g', '2020-01-01 11:30:00'),
                    (5, 'h', '2020-01-01 10:59:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 NULL                | 3      | -1
                 2020-01-01 11:00:00 | 3      | 1""");
    }

    /** Two independent waterlines drive garbage collection at the same time.  The final
     * view unions the two hourly views so that one output shows both; `ts` advances to the
     * afternoon while `ts2` stays in the morning, so hours are pruned on one side only. */
    @Test
    public void issue7093TwoColumnsData() {
        CompilerCircuitStream ccs = this.getCCS(APPEND_ONLY_TWO_TIMESTAMPS + """
                CREATE LOCAL VIEW a AS SELECT id, MIN(ts) AS ts FROM events GROUP BY id;
                CREATE LOCAL VIEW b AS SELECT id, MIN(ts2) AS ts2 FROM events GROUP BY id;
                CREATE LOCAL VIEW h1 AS SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM a GROUP BY TIMESTAMP_TRUNC(ts, HOUR);
                CREATE LOCAL VIEW h2 AS SELECT TIMESTAMP_TRUNC(ts2, HOUR) AS hour, MAX(id) AS max_id
                FROM b GROUP BY TIMESTAMP_TRUNC(ts2, HOUR);
                CREATE VIEW hourly AS
                SELECT 'a' AS src, hour, max_id FROM h1
                UNION ALL
                SELECT 'b' AS src, hour, max_id FROM h2;""")
                .withStringTrim().compactAfterEachStep();
        checkDownstreamRetainKeys(ccs, 4);
        ccs.step("""
                INSERT INTO events VALUES (1, '2020-01-01 10:30:00', '2020-01-01 10:30:00');""", """
                 src | hour                | max_id | weight
                ---------------------------------------------
                 a   | 2020-01-01 10:00:00 | 1      | 1
                 b   | 2020-01-01 10:00:00 | 1      | 1""");
        // Waterlines: ts 13:00, ts2 09:45.  Hour 10 of h1 falls below its waterline.
        ccs.step("""
                INSERT INTO events VALUES (2, '2020-01-01 14:00:00', '2020-01-01 10:45:00');""", """
                 src | hour                | max_id | weight
                ---------------------------------------------
                 a   | 2020-01-01 14:00:00 | 2      | 1
                 b   | 2020-01-01 10:00:00 | 1      | -1
                 b   | 2020-01-01 10:00:00 | 2      | 1""");
        // 09:50 is above the ts2 waterline although far below the ts one: group 1 moves
        // to hour 9 in h2 while h1 is untouched.
        ccs.step("""
                INSERT INTO events VALUES (1, '2020-01-01 13:30:00', '2020-01-01 09:50:00');""", """
                 src | hour                | max_id | weight
                ---------------------------------------------
                 b   | 2020-01-01 09:00:00 | 1      | 1""");
        // A row late in ts alone is dropped as a whole.
        ccs.step("""
                INSERT INTO events VALUES (3, '2020-01-01 09:59:00', '2020-01-01 10:10:00');""", """
                 src | hour                | max_id | weight
                ---------------------------------------------""");
        // Both timestamps exactly at their waterlines: a new group in both views.
        ccs.step("""
                INSERT INTO events VALUES (3, '2020-01-01 13:00:00', '2020-01-01 09:45:00');""", """
                 src | hour                | max_id | weight
                ---------------------------------------------
                 a   | 2020-01-01 13:00:00 | 3      | 1
                 b   | 2020-01-01 09:00:00 | 1      | -1
                 b   | 2020-01-01 09:00:00 | 3      | 1""");
    }

    /** MIN over a nullable column on an input that allows deletions: deleting a group's only
     * timestamped row restores its NULL minimum. */
    @Test
    public void issue7093NullableAggregate() {
        CompilerCircuitStream ccs = this.getCCS("""
                CREATE TABLE events (
                    id INT NOT NULL,
                    payload VARCHAR,
                    ts TIMESTAMP LATENESS INTERVAL 1 HOUR
                );
                CREATE LOCAL VIEW first_event AS SELECT id, MIN(ts) AS ts FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM first_event GROUP BY TIMESTAMP_TRUNC(ts, HOUR);""")
                .compactAfterEachStep();
        checkDownstreamRetainKeys(ccs, 2);
        ccs.step("""
                INSERT INTO events VALUES
                    (1, 'a', NULL),
                    (2, 'b', '2020-01-01 10:30:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 NULL                | 1      | 1
                 2020-01-01 10:00:00 | 2      | 1""");
        // Waterline = 09:30.  Group 1 gets a timestamp, then loses it again.
        ccs.step("""
                INSERT INTO events VALUES (1, 'c', '2020-01-01 09:45:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 NULL                | 1      | -1
                 2020-01-01 09:00:00 | 1      | 1""");
        ccs.step("""
                REMOVE FROM events VALUES (1, 'c', '2020-01-01 09:45:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 09:00:00 | 1      | -1
                 NULL                | 1      | 1""");
        // Waterline = 11:00 after this step; hour 10 is garbage-collected.
        ccs.step("""
                INSERT INTO events VALUES (3, 'd', '2020-01-01 12:00:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 12:00:00 | 3      | 1""");
        // Deleting group 2's row is late and dropped; group 1 leaves the NULL hour.
        ccs.step("""
                REMOVE FROM events VALUES (2, 'b', '2020-01-01 10:30:00');
                INSERT INTO events VALUES (1, 'e', '2020-01-01 11:00:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 NULL                | 1      | -1
                 2020-01-01 11:00:00 | 1      | 1""");
    }

    /** A FILTER clause makes the aggregate a fold rather than a MIN; no waterline on either path. */
    @Test
    public void issue7093Filter() {
        String views = """
                CREATE LOCAL VIEW first_event AS
                SELECT id, MIN(ts) FILTER (WHERE payload IS NOT NULL) AS ts FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM first_event GROUP BY TIMESTAMP_TRUNC(ts, HOUR);""";
        checkDownstreamRetainKeys(this.getCC(APPEND_ONLY_EVENTS + views), 0);
        checkDownstreamRetainKeys(this.getCC(EVENTS + views), 0);
    }

    /** Two chains minimizing different columns each carry their own column's waterline, and
     * consumers of each use it independently. */
    @Test
    public void issue7093TwoColumns() {
        checkDownstreamRetainKeys(this.getCC(APPEND_ONLY_TWO_TIMESTAMPS + """
                CREATE LOCAL VIEW a AS SELECT id, MIN(ts) AS ts FROM events GROUP BY id;
                CREATE LOCAL VIEW b AS SELECT id, MIN(ts2) AS ts2 FROM events GROUP BY id;
                CREATE VIEW h1 AS SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM a GROUP BY TIMESTAMP_TRUNC(ts, HOUR);
                CREATE VIEW h2 AS SELECT TIMESTAMP_TRUNC(ts2, HOUR) AS hour, MAX(id) AS max_id
                FROM b GROUP BY TIMESTAMP_TRUNC(ts2, HOUR);"""), 4);
    }

    /** The same two minimums computed by one GROUP BY share one chain, whose row changes
     * whenever either minimum moves and then carries the other column's old minimum,
     * possibly below that column's waterline: neither output column has a waterline. */
    @Test
    public void issue7093TwoColumnsOneAggregate() {
        checkDownstreamRetainKeys(this.getCC(APPEND_ONLY_TWO_TIMESTAMPS + """
                CREATE LOCAL VIEW ab AS SELECT id, MIN(ts) AS ts, MIN(ts2) AS ts2 FROM events GROUP BY id;
                CREATE VIEW h1 AS SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM ab GROUP BY TIMESTAMP_TRUNC(ts, HOUR);
                CREATE VIEW h2 AS SELECT TIMESTAMP_TRUNC(ts2, HOUR) AS hour, MAX(id) AS max_id
                FROM ab GROUP BY TIMESTAMP_TRUNC(ts2, HOUR);"""), 0);
    }

    @Test
    public void issue7093Aggregate() {
        CompilerCircuitStream ccs = this.getCCS(EVENTS + """
                CREATE LOCAL VIEW first_event AS
                SELECT id, MIN(ts) AS ts FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM first_event GROUP BY TIMESTAMP_TRUNC(ts, HOUR);""")
                .compactAfterEachStep();
        // The hourly aggregate garbage-collects its input and its output by hour.
        checkDownstreamRetainKeys(ccs, 2);
        ccs.step("""
                INSERT INTO events VALUES (1, 'a', '2020-01-01 10:30:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 10:00:00 | 1      | 1""");
        // Waterline = 09:30.  An earlier row moves group 1 to hour 9.
        ccs.step("""
                INSERT INTO events VALUES (1, 'b', '2020-01-01 09:45:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 10:00:00 | 1      | -1
                 2020-01-01 09:00:00 | 1      | 1""");
        // Deleting the minimum row (09:45, above the waterline) moves group 1 back to hour 10.
        ccs.step("""
                REMOVE FROM events VALUES (1, 'b', '2020-01-01 09:45:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 09:00:00 | 1      | -1
                 2020-01-01 10:00:00 | 1      | 1""");
        // Waterline = 11:00 after this step; hours 9 and 10 are garbage-collected.
        ccs.step("""
                INSERT INTO events VALUES (2, 'c', '2020-01-01 12:00:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 12:00:00 | 2      | 1""");
        // Deleting group 1's remaining row is late (10:30 < 11:00) and dropped;
        // the new group lands in the hour at the waterline.
        ccs.step("""
                REMOVE FROM events VALUES (1, 'a', '2020-01-01 10:30:00');
                INSERT INTO events VALUES (3, 'd', '2020-01-01 11:00:00');""", """
                 hour                | max_id | weight
                ---------------------------------------
                 2020-01-01 11:00:00 | 3      | 1""");
    }

    @Test
    public void issue7093AggregateNoWaterline() {
        // MAX: no waterline
        checkDownstreamRetainKeys(this.getCC(EVENTS + """
                CREATE LOCAL VIEW last_event AS SELECT id, MAX(ts) AS ts FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, MAX(id) AS max_id
                FROM last_event GROUP BY TIMESTAMP_TRUNC(ts, HOUR);"""), 0);
        // seen has no waterline
        checkDownstreamRetainKeys(this.getCC("""
                CREATE TABLE events (
                    id INT NOT NULL,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
                    seen TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
                );
                CREATE LOCAL VIEW first_seen AS SELECT id, ARG_MIN(seen, ts) AS seen FROM events GROUP BY id;
                CREATE VIEW hourly AS
                SELECT TIMESTAMP_TRUNC(seen, HOUR) AS hour, MAX(id) AS max_id
                FROM first_seen GROUP BY TIMESTAMP_TRUNC(seen, HOUR);"""), 0);
        // amount has no waterline
        checkDownstreamRetainKeys(this.getCC("""
                CREATE TABLE events (
                    id INT NOT NULL,
                    amount INT NOT NULL,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
                );
                CREATE LOCAL VIEW per_id AS SELECT id, MIN(amount) AS amount FROM events GROUP BY id;
                CREATE VIEW by_amount AS
                SELECT amount / 100 AS bucket, MAX(id) AS max_id FROM per_id GROUP BY amount / 100;"""), 0);
    }
}
