package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.util.HashString;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Tests that run circuits decoded from their JSON serialization.
 * Each test serializes the compiled circuit to JSON, decodes the JSON back into a circuit,
 * generates the Rust code from the decoded circuit, and checks the outputs that the Rust
 * code computes. */
public class SerializationTests extends StreamingTestBase {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        // Every optimization pass must leave a circuit that round-trips through JSON
        options.ioOptions.checkSerialization = true;
        return options;
    }

    @Test
    public void testSmallProgram() {
        String sql = """
                CREATE TABLE T(id INT NOT NULL, name VARCHAR, v DECIMAL(6, 2));
                CREATE VIEW V AS SELECT id, UPPER(name) AS name, v * 2 AS v2 FROM T WHERE v > 1;""";
        CompilerCircuitStream ccs = this.getCCS(sql).setRoundTripThroughJson().withStringTrim();
        // Expected output validated with Postgres
        ccs.stepWeightOne("""
                INSERT INTO T VALUES (1, 'a', 1.50), (2, NULL, 0.50), (3, 'b', 2.00);""", """
                 id | name | v2
                ----------------
                 1  | A    | 3.00
                 3  | B    | 4.00""");
    }

    @Test
    public void testLargeProgram() {
        String sql = """
                CREATE TABLE customers(
                    name VARCHAR NOT NULL PRIMARY KEY,
                    region VARCHAR,
                    credit INT NOT NULL);
                CREATE TABLE orders(
                    id INT NOT NULL PRIMARY KEY,
                    customer VARCHAR NOT NULL,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
                    amount DECIMAL(10, 2) NOT NULL,
                    tags VARCHAR ARRAY);
                CREATE TABLE edges(src INT NOT NULL, dst INT NOT NULL);
                CREATE FUNCTION discounted(amount DECIMAL(10, 2), pct INT) RETURNS DECIMAL(10, 2)
                    AS CAST(amount - amount * pct / 100 AS DECIMAL(10, 2));

                CREATE LOCAL VIEW hourly AS
                SELECT TUMBLE_START(ts, INTERVAL 1 HOUR) AS hour, customer,
                       COUNT(*) AS cnt, SUM(amount) AS total, MAX(amount) AS largest
                FROM orders
                GROUP BY TUMBLE(ts, INTERVAL 1 HOUR), customer;

                CREATE LOCAL VIEW priced AS
                SELECT o.id, c.region, discounted(o.amount, c.credit) AS price
                FROM orders o JOIN customers c ON o.customer = c.name;

                CREATE LOCAL VIEW top2 AS
                SELECT id, customer, amount FROM (
                    SELECT id, customer, amount,
                           ROW_NUMBER() OVER (PARTITION BY customer ORDER BY amount DESC, id) AS rn
                    FROM orders)
                WHERE rn <= 2;

                CREATE LOCAL VIEW rolling AS
                SELECT id, SUM(amount) OVER (
                    PARTITION BY customer ORDER BY ts
                    RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS hour_total
                FROM orders;

                CREATE LOCAL VIEW tagged AS
                SELECT o.id, t.tag FROM orders o, UNNEST(o.tags) AS t(tag);

                DECLARE RECURSIVE VIEW reach(src INT NOT NULL, dst INT NOT NULL);
                CREATE LOCAL VIEW reach AS
                SELECT src, dst FROM edges
                UNION
                SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src;

                CREATE VIEW summary AS
                SELECT 'hourly' AS src, CAST(hour AS VARCHAR) || ' ' || customer AS k,
                       CAST(cnt AS VARCHAR) || ' ' || CAST(total AS VARCHAR) || ' ' || CAST(largest AS VARCHAR) AS v
                FROM hourly
                UNION ALL SELECT 'priced', CAST(id AS VARCHAR), COALESCE(region, '?') || ' ' || CAST(price AS VARCHAR) FROM priced
                UNION ALL SELECT 'top2', customer, CAST(id AS VARCHAR) || ' ' || CAST(amount AS VARCHAR) FROM top2
                UNION ALL SELECT 'rolling', CAST(id AS VARCHAR), CAST(hour_total AS VARCHAR) FROM rolling
                UNION ALL SELECT 'tagged', CAST(id AS VARCHAR), tag FROM tagged
                UNION ALL SELECT 'reach', CAST(src AS VARCHAR), CAST(dst AS VARCHAR) FROM reach
                UNION ALL SELECT 'regions', region, CAST(COUNT(*) AS VARCHAR) FROM customers
                          WHERE region IS NOT NULL GROUP BY region
                UNION ALL SELECT 'count', 'orders', CAST(COUNT(*) AS VARCHAR) FROM orders;""";
        CompilerCircuitStream ccs = this.getCCS(sql).setRoundTripThroughJson().withStringTrim();
        // Expected outputs validated with Postgres, with TUMBLE written as date_trunc,
        // the SQL function inlined, and the late order 6 left out of the data.
        // CAST(DECIMAL AS VARCHAR) drops trailing zeros here, while Postgres keeps them.
        ccs.step("""
                INSERT INTO customers VALUES ('ann', 'west', 20), ('bob', 'east', 0), ('cid', NULL, 50);
                INSERT INTO orders VALUES
                    (1, 'ann', '2024-01-01 10:05:00', 100.00, ARRAY['gift', 'rush']),
                    (2, 'bob', '2024-01-01 10:20:00', 20.50, ARRAY['rush']),
                    (3, 'ann', '2024-01-01 10:50:00', 7.25, NULL),
                    (4, 'cid', '2024-01-01 11:40:00', 45.00, ARRAY['gift']);
                INSERT INTO edges VALUES (1, 2), (2, 3), (3, 4);""", """
                 src     | k                       | v               | weight
                -------------------------------------------------------------
                 count   | orders                  | 4               | 1
                 hourly  | 2024-01-01 10:00:00 ann | 2 107.25 100    | 1
                 hourly  | 2024-01-01 10:00:00 bob | 1 20.5 20.5     | 1
                 hourly  | 2024-01-01 11:00:00 cid | 1 45 45         | 1
                 priced  | 1                       | west 80         | 1
                 priced  | 2                       | east 20.5       | 1
                 priced  | 3                       | west 5.8        | 1
                 priced  | 4                       | ? 22.5          | 1
                 reach   | 1                       | 2               | 1
                 reach   | 1                       | 3               | 1
                 reach   | 1                       | 4               | 1
                 reach   | 2                       | 3               | 1
                 reach   | 2                       | 4               | 1
                 reach   | 3                       | 4               | 1
                 regions | east                    | 1               | 1
                 regions | west                    | 1               | 1
                 rolling | 1                       | 100             | 1
                 rolling | 2                       | 20.5            | 1
                 rolling | 3                       | 107.25          | 1
                 rolling | 4                       | 45              | 1
                 tagged  | 1                       | gift            | 1
                 tagged  | 1                       | rush            | 1
                 tagged  | 2                       | rush            | 1
                 tagged  | 4                       | gift            | 1
                 top2    | ann                     | 1 100           | 1
                 top2    | ann                     | 3 7.25          | 1
                 top2    | bob                     | 2 20.5          | 1
                 top2    | cid                     | 4 45            | 1""");
        // Order 6 is older than the waterline (11:40 minus the 1 hour lateness): dropped
        ccs.step("""
                INSERT INTO orders VALUES
                    (5, 'ann', '2024-01-01 11:30:00', 60.00, ARRAY['bulk']),
                    (6, 'bob', '2024-01-01 09:00:00', 1.00, NULL);
                REMOVE FROM orders VALUES (4, 'cid', '2024-01-01 11:40:00', 45.00, ARRAY['gift']);
                INSERT INTO edges VALUES (0, 1);""", """
                 src     | k                       | v               | weight
                -------------------------------------------------------------
                 hourly  | 2024-01-01 11:00:00 cid | 1 45 45         | -1
                 hourly  | 2024-01-01 11:00:00 ann | 1 60 60         | 1
                 priced  | 4                       | ? 22.5          | -1
                 priced  | 5                       | west 48         | 1
                 reach   | 0                       | 1               | 1
                 reach   | 0                       | 2               | 1
                 reach   | 0                       | 3               | 1
                 reach   | 0                       | 4               | 1
                 rolling | 4                       | 45              | -1
                 rolling | 5                       | 67.25           | 1
                 tagged  | 4                       | gift            | -1
                 tagged  | 5                       | bulk            | 1
                 top2    | ann                     | 3 7.25          | -1
                 top2    | ann                     | 5 60            | 1
                 top2    | cid                     | 4 45            | -1""");
    }

    /** Operators the other programs do not produce: RANK, an ASOF join with its retain-N-values
     * garbage collection, ARG_MAX and ARG_MIN with post-processing, a view column with LATENESS,
     * and a VARIANT column. */
    @Test
    public void testStreamingOperators() {
        String sql = """
                CREATE TABLE events(
                    id INT NOT NULL,
                    grp INT NOT NULL,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 10 MINUTES,
                    v INT NOT NULL,
                    payload VARIANT);
                CREATE TABLE prices(
                    grp INT NOT NULL,
                    since TIMESTAMP NOT NULL LATENESS INTERVAL 10 MINUTES,
                    price DECIMAL(8, 2) NOT NULL);

                CREATE LOCAL VIEW ranked AS
                SELECT id, grp, v, RANK() OVER (PARTITION BY grp ORDER BY v) AS r FROM events;

                CREATE LOCAL VIEW priced AS
                SELECT e.id, p.price
                FROM events e LEFT ASOF JOIN prices p
                MATCH_CONDITION (p.since <= e.ts)
                ON e.grp = p.grp;

                CREATE LOCAL VIEW extremes AS
                SELECT grp, ARG_MAX(id, v) AS id_of_max, ARG_MIN(id, v) AS id_of_min,
                       MIN(v) AS lo, MAX(v) AS hi
                FROM events GROUP BY grp;

                CREATE LOCAL VIEW tagged AS
                SELECT id, CAST(payload['tag'] AS VARCHAR) AS tag FROM events;

                CREATE LOCAL VIEW per_minute AS
                SELECT TUMBLE_START(ts, INTERVAL 1 MINUTE) AS minute, grp, COUNT(*) AS cnt
                FROM events GROUP BY TUMBLE(ts, INTERVAL 1 MINUTE), grp;
                LATENESS per_minute.minute INTERVAL 10 MINUTES;

                CREATE VIEW summary AS
                SELECT 'ranked' AS src, CAST(id AS VARCHAR) AS k,
                       CAST(grp AS VARCHAR) || ' ' || CAST(v AS VARCHAR) || ' ' || CAST(r AS VARCHAR) AS val
                FROM ranked
                UNION ALL SELECT 'priced', CAST(id AS VARCHAR), COALESCE(CAST(price AS VARCHAR), '?') FROM priced
                UNION ALL SELECT 'extremes', CAST(grp AS VARCHAR),
                          CAST(id_of_max AS VARCHAR) || ' ' || CAST(id_of_min AS VARCHAR) || ' ' ||
                          CAST(lo AS VARCHAR) || ' ' || CAST(hi AS VARCHAR) FROM extremes
                UNION ALL SELECT 'tagged', CAST(id AS VARCHAR), COALESCE(tag, '?') FROM tagged
                UNION ALL SELECT 'per_minute', CAST(minute AS VARCHAR) || ' ' || CAST(grp AS VARCHAR),
                          CAST(cnt AS VARCHAR) FROM per_minute;""";
        CompilerCircuitStream ccs = this.getCCS(sql).setRoundTripThroughJson().withStringTrim();
        // Expected outputs validated with Postgres, with the ASOF join written as a correlated
        // subquery, ARG_MAX/ARG_MIN as ORDER BY ... LIMIT 1, and payload->>'tag' for the VARIANT
        ccs.step("""
                INSERT INTO prices VALUES
                    (1, '2024-01-01 10:00:00', 5.25),
                    (1, '2024-01-01 10:30:00', 6.75),
                    (2, '2024-01-01 10:00:00', 9.99);
                INSERT INTO events VALUES
                    (1, 1, '2024-01-01 10:05:00', 10, PARSE_JSON('{"tag": "a"}')),
                    (2, 1, '2024-01-01 10:35:00', 30, PARSE_JSON('{"tag": "b"}')),
                    (3, 1, '2024-01-01 10:36:00', 20, NULL),
                    (4, 2, '2024-01-01 10:10:00', 7, PARSE_JSON('{"tag": "c", "n": 1}')),
                    (5, 3, '2024-01-01 09:59:00', 1, PARSE_JSON('{"tag": "d"}'));""", """
                 src        | k                     | val       | weight
                --------------------------------------------------------
                 extremes   | 1                     | 2 1 10 30 | 1
                 extremes   | 2                     | 4 4 7 7   | 1
                 extremes   | 3                     | 5 5 1 1   | 1
                 per_minute | 2024-01-01 09:59:00 3 | 1         | 1
                 per_minute | 2024-01-01 10:05:00 1 | 1         | 1
                 per_minute | 2024-01-01 10:10:00 2 | 1         | 1
                 per_minute | 2024-01-01 10:35:00 1 | 1         | 1
                 per_minute | 2024-01-01 10:36:00 1 | 1         | 1
                 priced     | 1                     | 5.25      | 1
                 priced     | 2                     | 6.75      | 1
                 priced     | 3                     | 6.75      | 1
                 priced     | 4                     | 9.99      | 1
                 priced     | 5                     | ?         | 1
                 ranked     | 1                     | 1 10 1    | 1
                 ranked     | 2                     | 1 30 3    | 1
                 ranked     | 3                     | 1 20 2    | 1
                 ranked     | 4                     | 2 7 1     | 1
                 ranked     | 5                     | 3 1 1     | 1
                 tagged     | 1                     | a         | 1
                 tagged     | 2                     | b         | 1
                 tagged     | 3                     | ?         | 1
                 tagged     | 4                     | c         | 1
                 tagged     | 5                     | d         | 1""");
        // Event 7 is older than the waterline (10:36 minus the 10 minutes lateness): dropped
        ccs.step("""
                INSERT INTO events VALUES
                    (6, 1, '2024-01-01 10:40:00', 25, PARSE_JSON('{"tag": "e"}')),
                    (7, 2, '2024-01-01 10:20:00', 8, NULL);""", """
                 src        | k                     | val       | weight
                --------------------------------------------------------
                 per_minute | 2024-01-01 10:40:00 1 | 1         | 1
                 priced     | 6                     | 6.75      | 1
                 ranked     | 2                     | 1 30 3    | -1
                 ranked     | 2                     | 1 30 4    | 1
                 ranked     | 6                     | 1 25 3    | 1
                 tagged     | 6                     | e         | 1""");
    }

    /** A small recursive program: the nested operator, the view declaration and the delayed
     * integral of a transitive closure, with a change that retracts derived facts. */
    @Test
    public void testRecursiveProgram() {
        String sql = """
                CREATE TABLE edges(src INT NOT NULL, dst INT NOT NULL);
                DECLARE RECURSIVE VIEW reach(src INT NOT NULL, dst INT NOT NULL);
                CREATE VIEW reach AS
                SELECT src, dst FROM edges
                UNION
                SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src;""";
        CompilerCircuitStream ccs = this.getCCS(sql).setRoundTripThroughJson();
        // Expected outputs validated with Postgres; the recursion is linear and monotone, so
        // WITH RECURSIVE r AS (SELECT src, dst FROM edges UNION
        //                      SELECT r.src, e.dst FROM r JOIN edges e ON r.dst = e.src)
        // computes the same closure
        ccs.stepWeightOne("""
                INSERT INTO edges VALUES (1, 2), (2, 3), (3, 4);""", """
                 src | dst
                -----------
                 1   | 2
                 1   | 3
                 1   | 4
                 2   | 3
                 2   | 4
                 3   | 4""");
        ccs.step("""
                REMOVE FROM edges VALUES (2, 3);
                INSERT INTO edges VALUES (4, 1);""", """
                 src | dst | weight
                --------------------
                 1   | 3   | -1
                 1   | 4   | -1
                 2   | 3   | -1
                 2   | 4   | -1
                 3   | 1   | 1
                 3   | 2   | 1
                 4   | 1   | 1
                 4   | 2   | 1""");
    }

    /** Persistent ids of the operators of a program, keyed by operator id. */
    Map<Long, String> persistentIds(String sql) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Map<Long, String> result = new HashMap<>();
        for (DBSPOperator operator: circuit.allOperators) {
            HashString hash = OperatorHash.getHash(operator, true);
            if (hash != null)
                result.put(operator.id, hash.toString());
        }
        return result;
    }

    /** The legacy hash of a view's persistent id leaves out the column metadata, so that
     * deployed pipelines keep their ids and do not bootstrap.  A change of a view column's
     * LATENESS must then show up in the ids of the operators that use the lateness: the
     * waterline, the garbage collection of the consumer, and the consumer's sink. */
    @Test
    public void testViewLatenessChangesDownstreamIds() {
        String program = """
                CREATE TABLE events(id INT NOT NULL, grp INT NOT NULL,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 10 MINUTES, v INT NOT NULL);
                CREATE LOCAL VIEW per_minute AS
                SELECT TUMBLE_START(ts, INTERVAL 1 MINUTE) AS minute, grp, COUNT(*) AS cnt
                FROM events GROUP BY TUMBLE(ts, INTERVAL 1 MINUTE), grp;
                LATENESS per_minute.minute INTERVAL %s;
                CREATE VIEW hourly AS
                SELECT TUMBLE_START(minute, INTERVAL 1 HOUR) AS hour, grp, SUM(cnt) AS total
                FROM per_minute GROUP BY TUMBLE(minute, INTERVAL 1 HOUR), grp;""";
        Set<String> tenMinutes = new HashSet<>(this.persistentIds(String.format(program, "10 MINUTES")).values());
        Set<String> twentyMinutes = new HashSet<>(this.persistentIds(String.format(program, "20 MINUTES")).values());
        Set<String> changed = new HashSet<>(tenMinutes);
        changed.removeAll(twentyMinutes);
        Set<String> unchanged = new HashSet<>(tenMinutes);
        unchanged.retainAll(twentyMinutes);
        // The sources and the operators computing per_minute keep their ids
        Assert.assertFalse(unchanged.isEmpty());
        // The waterline, the garbage collection and the sink of hourly do not
        Assert.assertFalse(changed.isEmpty());
    }
}
