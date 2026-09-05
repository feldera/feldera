package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.junit.Assert;
import org.junit.Test;

public class TopKTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String sql = """
                create table DocumentStatusLog (
                    ID int,
                    DocumentId int,
                    Status VARCHAR,
                    DateCreated DATE NOT NULL
                );
                INSERT INTO DocumentStatusLog VALUES(2, 1, 'S1', '2011-07-29');
                INSERT INTO DocumentStatusLog VALUES(3, 1, 'S2', '2011-07-30');
                INSERT INTO DocumentStatusLog VALUES(6, 1, 'S1', '2011-09-02');
                INSERT INTO DocumentStatusLog VALUES(1, 2, 'S1', '2011-07-28');
                INSERT INTO DocumentStatusLog VALUES(4, 2, 'S2', '2011-07-30');
                INSERT INTO DocumentStatusLog VALUES(5, 2, 'S3', '2011-08-01');
                INSERT INTO DocumentStatusLog VALUES(6, 3, 'S1', '2011-08-02');""";
        compiler.submitStatementsForCompilation(sql);
    }

    @Test
    public void testTopK() {
        // below ? is a parameter
        String paramQuery = """
                WITH cte AS
                (
                   SELECT *,
                         ?() OVER (PARTITION BY DocumentID ORDER BY DateCreated DESC) AS rn
                   FROM DocumentStatusLog
                )
                SELECT DocumentId, Status, DateCreated
                FROM cte
                WHERE rn <= 1;
                 DocumentID | Status | DateCreated
                -----------------------------------
                 1          | S1     | 2011-09-02
                 2          | S3     | 2011-08-01
                 3          | S1     | 2011-08-02
                (3 rows)

                WITH cte AS
                (
                   SELECT *,
                         ?() OVER (PARTITION BY DocumentID ORDER BY DateCreated) AS rn
                   FROM DocumentStatusLog
                )
                SELECT DocumentId, Status, DateCreated
                FROM cte
                WHERE rn <= 1;
                 DocumentID | Status | DateCreated
                -----------------------------------
                 1          | S1     | 2011-07-29
                 2          | S1     | 2011-07-28
                 3          | S1     | 2011-08-02
                (3 rows)

                WITH cte AS
                (
                   SELECT *,
                         ?() OVER (PARTITION BY DocumentID ORDER BY DateCreated DESC) AS rn
                   FROM DocumentStatusLog
                )
                SELECT DocumentId, Status, DateCreated
                FROM cte
                WHERE rn <= 1;
                 DocumentID | Status | DateCreated
                -----------------------------------
                 1          | S1     | 2011-09-02
                 3          | S1     | 2011-08-02
                 2          | S3     | 2011-08-01
                (3 rows)""";
        for (String function : new String[]{"RANK", "DENSE_RANK", "ROW_NUMBER"}) {
            String q = paramQuery.replace("?", function);
            // Same result for all 3 functions
            this.qst(q);
        }
    }

    @Test
    public void issue1174() {
        String sql = """
                CREATE TABLE event_t (
                id BIGINT NOT NULL PRIMARY KEY,
                site_id BIGINT NOT NULL,
                event_type_id BIGINT NOT NULL,
                event_date BIGINT NOT NULL, -- epoch
                event_clear_date BIGINT -- epoch
                );

                CREATE VIEW EVENT_DURATION_V AS
                SELECT (event_date - event_clear_date) AS duration
                ,      event_type_id
                ,      site_id
                FROM   event_t
                WHERE  event_clear_date IS NOT NULL;

                CREATE VIEW TOP_EVENT_DURATIONS_V AS
                SELECT (duration * -1) as duration
                ,      event_type_id
                FROM   (SELECT duration
                        ,      event_type_id
                        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id
                                                  ORDER BY duration ASC) AS rnum
                        FROM   EVENT_DURATION_V)
                WHERE   rnum <= 3;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        Assert.assertEquals(0, compiler.messages.errorCount());
    }

    @Test
    public void issue1184() {
        String sql = """
                CREATE TABLE event_t (
                id BIGINT NOT NULL PRIMARY KEY,
                site_id BIGINT NOT NULL,
                event_type_id BIGINT NOT NULL,
                event_date BIGINT NOT NULL, -- epoch
                event_clear_date BIGINT -- epoch
                );

                CREATE VIEW EVENT_DURATION_V AS
                SELECT (event_date - event_clear_date) AS duration
                ,      event_type_id
                ,      site_id
                FROM   event_t
                WHERE  event_clear_date IS NOT NULL
                ;

                CREATE VIEW TOP_EVENT_DURATIONS_V AS
                SELECT duration
                ,      event_type_id
                FROM   (SELECT duration
                        ,      event_type_id
                        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id
                                                  ORDER BY duration DESC) AS rnum
                        FROM   EVENT_DURATION_V)
                WHERE   rnum = 1
                ORDER BY 1 DESC;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        Assert.assertEquals(0, compiler.messages.errorCount());
    }

    @Test
    public void issue1185() {
        String sql = """
                CREATE TABLE event_t (
                id BIGINT NOT NULL PRIMARY KEY,
                site_id BIGINT NOT NULL,
                event_type_id BIGINT NOT NULL,
                event_date BIGINT NOT NULL, -- epoch
                event_clear_date BIGINT -- epoch
                );

                CREATE VIEW EVENT_DURATION_V AS
                SELECT (event_date - event_clear_date) AS duration
                ,      event_type_id
                ,      site_id
                FROM   event_t
                WHERE  event_clear_date IS NOT NULL;

                CREATE VIEW TOP_EVENT_DURATIONS_V AS
                SELECT duration
                ,      site_id
                FROM   (SELECT duration
                        ,      site_id
                        ,      ROW_NUMBER() OVER (PARTITION BY site_id
                                                  ORDER BY duration ASC) AS rnum
                        FROM   EVENT_DURATION_V)
                WHERE   rnum = 1;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        Assert.assertEquals(0, compiler.messages.errorCount());
    }

    @Test
    public void issue1175() {
        String sql = """
                CREATE TABLE event_t (
                id BIGINT NOT NULL PRIMARY KEY,
                site_id BIGINT NOT NULL,
                event_type_id BIGINT NOT NULL,
                event_date BIGINT NOT NULL, -- epoch
                event_clear_date BIGINT -- epoch
                );

                CREATE VIEW EVENT_DURATION_V AS
                SELECT (event_date - event_clear_date) AS duration
                ,      event_type_id
                ,      site_id
                FROM   event_t
                WHERE  event_clear_date IS NOT NULL
                ;

                CREATE VIEW TOP_EVENT_DURATIONS_V AS
                SELECT (duration * -1) as duration
                ,      event_type_id
                FROM   (SELECT duration
                        ,      event_type_id
                        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id
                                                  ORDER BY duration ASC) AS rnum
                        FROM   EVENT_DURATION_V)
                WHERE   rnum <= 3;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        Assert.assertEquals(0, compiler.messages.errorCount());
    }

    @Test
    public void issue3707() {
        this.getCC("""
                CREATE TABLE event_duration (
                   duration int,
                   event_type_id int,
                   site_id int
                );

                CREATE VIEW V0 AS
                SELECT (duration * -1) as duration
                ,      event_type_id
                FROM   (SELECT duration
                        ,      event_type_id
                        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id
                                                  ORDER BY duration ASC) AS rnum
                        FROM   EVENT_DURATION)
                WHERE   rnum <= 3 AND event_type_id = 1;

                CREATE VIEW V1 AS
                SELECT (duration * -1) as duration
                ,      event_type_id
                FROM   (SELECT duration
                        ,      event_type_id
                        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id
                                                  ORDER BY duration ASC) AS rnum
                        FROM   EVENT_DURATION)
                WHERE   event_type_id = 1 AND rnum <= 3;

                CREATE VIEW V2 AS
                SELECT (duration * -1) as duration
                ,      event_type_id
                FROM   (SELECT duration
                        ,      event_type_id
                        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id
                                                  ORDER BY duration ASC) AS rnum
                        FROM   EVENT_DURATION)
                WHERE   rnum % 2 = 1 AND rnum <= 3;""");
    }

    @Test
    public void top3() {
        // Validated on Postgres
        this.qst("""
                WITH cte AS (
                SELECT *,
                   RANK() OVER (PARTITION BY DocumentID ORDER BY DateCreated) AS rn
                   FROM DocumentStatusLog
                )
                SELECT DocumentId, Status, DateCreated, rn
                FROM cte
                WHERE rn < 3;
                 DocumentID | Status | DateCreated | rn
                ---------------------------------------
                 1          | S1     | 2011-07-29  | 1
                 1          | S2     | 2011-07-30  | 2
                 2          | S1     | 2011-07-28  | 1
                 2          | S2     | 2011-07-30  | 2
                 3          | S1     | 2011-08-02  | 1
                (5 rows)""");
    }

    /** Compile a query with one TopK operator and check the arities of its output key and value. */
    void checkTopKShape(String query, int keyArity, int valueArity) {
        var cc = this.getCC("CREATE VIEW V AS " + query);
        cc.visit(new CircuitVisitor(cc.compiler) {
            int topKs = 0;

            @Override
            public void postorder(DBSPIndexedTopKOperator node) {
                DBSPTypeIndexedZSet output = node.getOutputIndexedZSetType();
                Assert.assertEquals(keyArity, output.keyType.to(DBSPTypeTuple.class).size());
                Assert.assertEquals(valueArity, output.elementType.to(DBSPTypeTuple.class).size());
                this.topKs++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.topKs);
                super.endVisit();
            }
        });
    }

    /** The partition fields live in the TopK key, so the value holds only the other fields and the rank.
     * Every query selects all columns, so the optimizer trims nothing. */
    @Test
    public void topKValueOmitsPartitionFields() {
        String select = "SELECT ID, DocumentId, Status, DateCreated, rn FROM (SELECT *, ROW_NUMBER() OVER (";
        String from = ") AS rn FROM DocumentStatusLog) WHERE rn <= 2;";
        // Partition fields in the middle of the row
        this.checkTopKShape(select + "PARTITION BY Status, DocumentId ORDER BY DateCreated DESC" + from, 2, 3);
        // No partition: the whole row and the rank are in the value
        this.checkTopKShape(select + "ORDER BY DateCreated" + from, 0, 5);
        // Partition on the first field
        this.checkTopKShape(select + "PARTITION BY ID ORDER BY DateCreated" + from, 1, 4);
        // Partition on every field: only the rank is in the value
        this.checkTopKShape(select + "PARTITION BY ID, DocumentId, Status, DateCreated ORDER BY ID" + from, 4, 1);
        // Partition field that is also an ORDER BY field
        this.checkTopKShape(select + "PARTITION BY DocumentId ORDER BY DocumentId, DateCreated" + from, 1, 4);
        // TopK after another window aggregate over the same partition; its result is one more input field
        this.checkTopKShape("""
                SELECT ID, DocumentId, Status, DateCreated, r, rn FROM (
                    SELECT *, RANK() OVER (PARTITION BY DocumentId ORDER BY ID) AS r,
                              ROW_NUMBER() OVER (PARTITION BY DocumentId ORDER BY DateCreated) AS rn
                    FROM DocumentStatusLog)
                WHERE rn <= 1;""", 1, 5);
        // TopK after a window aggregate over a different partition, so the TopK builds its own index
        this.checkTopKShape("""
                SELECT ID, DocumentId, Status, DateCreated, r, rn FROM (
                    SELECT *, RANK() OVER (PARTITION BY Status ORDER BY ID) AS r,
                              ROW_NUMBER() OVER (PARTITION BY DocumentId ORDER BY DateCreated) AS rn
                    FROM DocumentStatusLog)
                WHERE rn <= 1;""", 1, 5);
    }

    /** NULL partition keys form one partition and come back as NULL once the fields are reassembled. */
    @Test
    public void nullPartitionKey() {
        var ccs = this.getCCS("""
                CREATE TABLE T(a INT, b INT NOT NULL);
                CREATE VIEW V AS
                SELECT a, b, rn FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn FROM T)
                WHERE rn <= 1;""");
        // Validated on Postgres
        ccs.step("INSERT INTO T VALUES (NULL, 1), (NULL, 2), (1, 3), (1, 4), (2, 5);", """
                 a | b | rn | weight
                --------------------
                   | 1 | 1  | 1
                 1 | 3 | 1  | 1
                 2 | 5 | 1  | 1""");
    }

    /** Partition fields in the middle and at the end of the row, with every column and the rank selected. */
    @Test
    public void partitionFieldsInsideRow() {
        // Validated on Postgres
        String paramQuery = """
                SELECT ID, DocumentId, Status, DateCreated, rn FROM (
                    SELECT *, ?() OVER (PARTITION BY Status, DocumentId ORDER BY DateCreated DESC) AS rn
                    FROM DocumentStatusLog)
                WHERE rn <= 2;
                 ID | DocumentId | Status | DateCreated | rn
                --------------------------------------------
                 6  | 1          | S1     | 2011-09-02  | 1
                 2  | 1          | S1     | 2011-07-29  | 2
                 3  | 1          | S2     | 2011-07-30  | 1
                 1  | 2          | S1     | 2011-07-28  | 1
                 4  | 2          | S2     | 2011-07-30  | 1
                 5  | 2          | S3     | 2011-08-01  | 1
                 6  | 3          | S1     | 2011-08-02  | 1
                (7 rows)

                SELECT ID, DocumentId, Status, DateCreated, rn FROM (
                    SELECT *, ?() OVER (PARTITION BY DateCreated ORDER BY ID) AS rn
                    FROM DocumentStatusLog)
                WHERE rn <= 1;
                 ID | DocumentId | Status | DateCreated | rn
                --------------------------------------------
                 1  | 2          | S1     | 2011-07-28  | 1
                 2  | 1          | S1     | 2011-07-29  | 1
                 3  | 1          | S2     | 2011-07-30  | 1
                 5  | 2          | S3     | 2011-08-01  | 1
                 6  | 3          | S1     | 2011-08-02  | 1
                 6  | 1          | S1     | 2011-09-02  | 1
                (6 rows)""";
        for (String function : new String[]{"RANK", "DENSE_RANK", "ROW_NUMBER"}) {
            // No ties within any partition, so the three functions agree
            this.qst(paramQuery.replace("?", function));
        }
    }
}
