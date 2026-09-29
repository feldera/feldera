package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Issue 7086: deduplication of a CDC stream in bounded state. */
public class DeduplicationIncrementalTests extends StreamingTestBase {
    /** A CDC stream of orders, and the deduplication of issue 7086.  Record times arrive
     * up to a day late, Kafka timestamps in order.  MIN computes the smallest
     * processing time of each change.  The join finds the message with that time. */
    static final String DEDUP = """
            CREATE TABLE orders_cdc (
                -- The row before the change, NULL for an insert.  Same columns as after
                before ROW(
                    order_id VARCHAR NULL,
                    amount DECIMAL(12, 2) NULL,
                    status VARCHAR NULL
                    -- additional columns of the orders table omitted
                ) NULL,
                -- The row after the change, NULL for a delete
                after ROW(
                    -- The primary key of the orders table (but not a PK of the CDC stream)
                    order_id VARCHAR NULL,
                    amount DECIMAL(12, 2) NULL,
                    status VARCHAR NULL
                    -- additional columns of the orders table omitted
                ) NULL,
                source ROW(
                    -- When the database recorded the change
                    ts_ms TIMESTAMP NULL,
                    -- Position of the change in the write-ahead log of the database
                    lsn BIGINT NULL
                    -- additional fields of the Debezium source block omitted
                ) NULL,
                -- 'c' for an insert, 'u' for an update, 'd' for a delete, 'r' for a snapshot read
                op VARCHAR NULL,
                kafka_timestamp TIMESTAMP DEFAULT CAST(CONNECTOR_METADATA()['kafka_timestamp'] AS TIMESTAMP),
                kafka_offset BIGINT DEFAULT CAST(CONNECTOR_METADATA()['kafka_offset'] AS BIGINT)
            ) WITH (
                'append_only' = 'true',
                'connectors' = '[{
                    "name": "orders",
                    "transport": {
                        "name": "kafka_input",
                        "config": {
                            "topic": "orders",
                            "bootstrap.servers": "broker:9092",
                            "include_timestamp": true,
                            "include_offset": true,
                            "synchronize_partitions": true
                        }
                    },
                    "format": {
                        "name": "json",
                        "config": { "update_format": "raw" }
                    }
                }]'
            );

            CREATE LOCAL VIEW changes AS
            SELECT
                -- Flatten the messages into top-level columns
                source.ts_ms AS recorded_at,
                kafka_timestamp,
                -- A delete describes the row removed in 'before', other changes are in 'after'
                (CASE WHEN op = 'd' THEN before ELSE after END).order_id AS order_id,
                (CASE WHEN op = 'd' THEN before ELSE after END).amount AS amount,
                (CASE WHEN op = 'd' THEN before ELSE after END).status AS status,
                op,
                -- A snapshot read carries no log position
                COALESCE(source.lsn, 0) AS lsn,
                kafka_offset
            FROM orders_cdc
            WHERE
              -- drop rows without a key
              (CASE WHEN op = 'd' THEN before ELSE after END).order_id IS NOT NULL
              -- drop rows without timestamps
              AND source.ts_ms IS NOT NULL
              AND kafka_timestamp IS NOT NULL;

            LATENESS changes.recorded_at INTERVAL 1 DAY;
            LATENESS changes.kafka_timestamp INTERVAL 0 SECONDS;

            -- The processing time of the first copy of each change
            CREATE LOCAL VIEW first_arrival AS
            SELECT order_id, recorded_at, lsn, MIN(kafka_timestamp) AS kafka_timestamp
            FROM changes
            GROUP BY order_id, recorded_at, lsn;

            -- The first copy of each change, with its values
            CREATE VIEW deduplicated AS
            SELECT changes.order_id, changes.amount, changes.status, changes.op,
                changes.recorded_at, changes.kafka_timestamp, changes.kafka_offset
            FROM first_arrival JOIN changes
              ON changes.order_id = first_arrival.order_id
             AND changes.recorded_at = first_arrival.recorded_at
             AND changes.lsn = first_arrival.lsn
             AND changes.kafka_timestamp = first_arrival.kafka_timestamp;
            """;

    /** One message about order {@code id}.  The database records the change at
     * {@code recorded}, at log position {@code lsn}.  Kafka stores the record at
     * {@code produced}, at offset {@code offset}.  {@code op} is 'c', 'u' or 'd'. */
    static String message(String op, String id, String recorded, long lsn,
                          String produced, int offset) {
        String values = "ROW('%s', 10.00, 'approved')".formatted(id);
        return """
                INSERT INTO orders_cdc VALUES(ROW(
                    %1$s,
                    %2$s,
                    ROW(TIMESTAMP '%3$s', %4$d),
                    '%5$s', TIMESTAMP '%6$s', %7$d));"""
                .formatted(op.equals("d") ? values : "NULL",
                        op.equals("d") ? "NULL" : values,
                        recorded, lsn, op, produced, offset);
    }

    /** Checks which integrals have garbage collection by keys.  Each entry of
     * {@code expected} is the class name of an operator that feeds one integral. */
    static void checkRetainKeys(CompilerCircuit cc, String... expected) {
        cc.visit(new CircuitVisitor(cc.compiler) {
            final List<String> retained = new ArrayList<>();

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.retained.add(operator.left().operator.getClass().getSimpleName());
            }

            @Override
            public void endVisit() {
                this.retained.sort(String::compareTo);
                List<String> sorted = new ArrayList<>(Arrays.asList(expected));
                sorted.sort(String::compareTo);
                Assert.assertEquals(sorted, this.retained);
            }
        });
    }

    /** All three integrals have garbage collection.  The aggregate uses the record time.
     * Each side of the join uses the first of the two waterlines the key falls below. */
    @Test
    public void dedupStateIsBounded() {
        checkRetainKeys(this.getCC(DEDUP),
                // MIN(kafka_timestamp) for each (order_id, recorded_at)
                "DBSPChainAggregateOperator",
                // first_arrival, indexed for the join
                "DBSPFlatMapIndexOperator",
                // changes, indexed for the join
                "DBSPFlatMapIndexOperator");
    }

    /** The view keeps the first copy of each change and drops the later copies. */
    @Test
    public void dedupOutput() {
        CompilerCircuitStream ccs = this.getCCS(DEDUP).withStringTrim().compactAfterEachStep();
        // Two copies of one change arrive together; the one produced earlier wins.
        ccs.step(message("c", "order-1", "2026-09-08 09:00:00", 1000, "2026-09-08 10:00:00", 100)
                + message("c", "order-1", "2026-09-08 09:00:00", 1000, "2026-09-08 10:00:30", 101)
                + message("c", "order-2", "2026-09-08 09:00:00", 1001, "2026-09-08 10:00:10", 102), """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------
                 order-1  | 10.00  | approved | c | 2026-09-08 09:00:00 | 2026-09-08 10:00:00 | 100 | 1
                 order-2  | 10.00  | approved | c | 2026-09-08 09:00:00 | 2026-09-08 10:00:10 | 102 | 1""");
        // A copy produced later gives no output.
        ccs.step(message("c", "order-1", "2026-09-08 09:00:00", 1000, "2026-09-08 10:00:45", 103), """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------""");
        // Lateness 0 on kafka_timestamp rules out a correction: this copy was produced
        // before the winner, so it is below the waterline and Feldera drops it.
        ccs.step(message("c", "order-2", "2026-09-08 09:00:00", 1001, "2026-09-08 10:00:05", 104), """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------""");
        // A change the database recorded at another time is a different change.
        ccs.step(message("c", "order-1", "2026-09-08 09:05:00", 1002, "2026-09-08 10:01:00", 105), """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------
                 order-1 | 10.00 | approved | c | 2026-09-08 09:05:00 | 2026-09-08 10:01:00 | 105 | 1""");
        // The processing time waterline is now 10:00:00; this copy is late and is dropped.
        ccs.step(message("c", "order-2", "2026-09-08 09:00:00", 1001, "2026-09-08 09:59:00", 106), """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------""");
    }

    /** Lateness 0 on kafka_timestamp drops a change whose record enters Kafka behind the
     * newest one, even when the change itself is new.  The topic must therefore stamp
     * records in a monotone order. */
    @Test
    public void dedupFirstCopySlightlyOutOfOrder() {
        CompilerCircuitStream ccs = this.getCCS(DEDUP).withStringTrim().compactAfterEachStep();
        ccs.step(message("c", "order-1", "2026-09-08 09:00:00", 1000, "2026-09-08 10:00:10", 100), """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------
                 order-1 | 10.00 | approved | c | 2026-09-08 09:00:00 | 2026-09-08 10:00:10 | 100 | 1""");
        // A new change, written to Kafka one second behind the newest record: lost.
        ccs.step(message("c", "order-2", "2026-09-08 09:00:01", 1001, "2026-09-08 10:00:09", 101), """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------""");
    }

    /** Two identical messages both reach the output.  The join cannot tell them apart. */
    @Test
    public void dedupRepeatedRow() {
        CompilerCircuitStream ccs = this.getCCS(DEDUP).withStringTrim().compactAfterEachStep();
        String row = message("c", "order-1", "2026-09-08 09:00:00", 1000, "2026-09-08 10:00:00", 100);
        String expected = """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------
                 order-1  | 10.00  | approved | c | 2026-09-08 09:00:00 | 2026-09-08 10:00:00 | 100 | 1""";
        ccs.step(row, expected);
        ccs.step(row, expected);
    }

    /** A transaction can change one row twice.  The two changes share a record time and
     * differ in the log position, so the view reports both. */
    @Test
    public void dedupTwoChangesInOneTransaction() {
        CompilerCircuitStream ccs = this.getCCS(DEDUP).withStringTrim().compactAfterEachStep();
        ccs.step(message("c", "order-1", "2026-09-08 09:00:00", 1000, "2026-09-08 10:00:00", 100)
                + message("u", "order-1", "2026-09-08 09:00:00", 1001, "2026-09-08 10:00:01", 101), """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------
                 order-1 | 10.00 | approved | c | 2026-09-08 09:00:00 | 2026-09-08 10:00:00 | 100 | 1
                 order-1 | 10.00 | approved | u | 2026-09-08 09:00:00 | 2026-09-08 10:00:01 | 101 | 1""");
    }

    /** A delete carries its values in before, and the view reports them. */
    @Test
    public void dedupDelete() {
        CompilerCircuitStream ccs = this.getCCS(DEDUP).withStringTrim().compactAfterEachStep();
        ccs.step(message("d", "order-1", "2026-09-08 09:00:00", 1000, "2026-09-08 10:00:00", 100)
                + message("d", "order-1", "2026-09-08 09:00:00", 1000, "2026-09-08 10:00:30", 101), """
                 order_id | amount | status | op | recorded_at | kafka_timestamp | kafka_offset | weight
                ------------------------------------------------------------------------------------------------
                 order-1 | 10.00 | approved | d | 2026-09-08 09:00:00 | 2026-09-08 10:00:00 | 100 | 1""");
    }
}
