package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.junit.Test;

import java.util.Map;

/** Issue 7530: late updates of a table whose primary key has a LATENESS column. */
public class UpsertLatenessIncrementalTests extends StreamingTestBase {
    /** The same table twice; only 't' garbage-collects its rows. */
    static final String TABLES = """
            CREATE TABLE t (
              id INT NOT NULL,
              ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
              v  VARCHAR,
              PRIMARY KEY (id, ts)
            );
            CREATE TABLE m (
              id INT NOT NULL,
              ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
              v  VARCHAR,
              PRIMARY KEY (id, ts)
            ) WITH ('materialized' = 'true');
            CREATE VIEW contents AS
            SELECT 't' AS name, * FROM t UNION ALL SELECT 'm' AS name, * FROM m;
            CREATE VIEW errors AS SELECT * FROM ERROR_VIEW;""";

    static final String NO_ERRORS = """
             table_or_view_name | message | metadata | weight
            --------------------------------------------------""";

    static final String NO_CONTENTS = """
             name | id | ts | v | weight
            ------------------------------""";

    /** The errors for a late update do not depend on whether the table
     * garbage-collects its rows, nor on whether compaction removed them. */
    static void lateUpdates(CompilerCircuitStream ccs) {
        ccs.stepMultiView("""
                INSERT INTO t VALUES (1, '2024-01-01 00:00:00', 'a');
                INSERT INTO m VALUES (1, '2024-01-01 00:00:00', 'a');""", Map.of(
                "contents", """
                         name | id | ts                  | v | weight
                        ---------------------------------------------
                         t    | 1  | 2024-01-01 00:00:00 | a | 1
                         m    | 1  | 2024-01-01 00:00:00 | a | 1""",
                "errors", NO_ERRORS));
        // The waterline moves to 02:00
        ccs.stepMultiView("""
                INSERT INTO t VALUES (2, '2024-01-01 03:00:00', 'b');
                INSERT INTO m VALUES (2, '2024-01-01 03:00:00', 'b');""", Map.of(
                "contents", """
                         name | id | ts                  | v | weight
                        ---------------------------------------------
                         t    | 2  | 2024-01-01 03:00:00 | b | 1
                         m    | 2  | 2024-01-01 03:00:00 | b | 1""",
                "errors", NO_ERRORS));
        // A late upsert reports the rejected row
        ccs.stepMultiView("""
                INSERT INTO t VALUES (1, '2024-01-01 00:00:00', 'c');
                INSERT INTO m VALUES (1, '2024-01-01 00:00:00', 'c');""", Map.of(
                "contents", NO_CONTENTS,
                "errors", """
                         table_or_view_name | message  | metadata                                       | weight
                        --------------------------------------------------------------------------------------------
                         t                  | Late row | (1, 2024-01-01 00:00:00, Some(SqlString("c"))) | 1
                         m                  | Late row | (1, 2024-01-01 00:00:00, Some(SqlString("c"))) | 1"""));
        // A late deletion reports the key
        ccs.stepMultiView("""
                REMOVE FROM t VALUES (1, '2024-01-01 00:00:00', 'a');
                REMOVE FROM m VALUES (1, '2024-01-01 00:00:00', 'a');""", Map.of(
                "contents", NO_CONTENTS,
                "errors", """
                         table_or_view_name | message  | metadata                 | weight
                        ----------------------------------------------------------------
                         t                  | Late row | (1, 2024-01-01 00:00:00) | 1
                         m                  | Late row | (1, 2024-01-01 00:00:00) | 1"""));
    }

    @Test
    public void issue7530() {
        lateUpdates(this.getCCS(TABLES).withStringTrim());
    }

    @Test
    public void issue7530a() {
        lateUpdates(this.getCCS(TABLES).withStringTrim().compactAfterEachStep());
    }
}
