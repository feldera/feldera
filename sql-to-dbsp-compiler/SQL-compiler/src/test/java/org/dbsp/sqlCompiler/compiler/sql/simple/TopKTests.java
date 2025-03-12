package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Assert;
import org.junit.Ignore;
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
                 1          | S1| 2011-09-02
                 2          | S3| 2011-08-01
                 3          | S1| 2011-08-02
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
                 1          | S1| 2011-07-29
                 2          | S1| 2011-07-28
                 3          | S1| 2011-08-02
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
                 1          | S1| 2011-09-02
                 3          | S1| 2011-08-02
                 2          | S3| 2011-08-01
                (3 rows)""";
        for (String function : new String[]{"RANK", "DENSE_RANK", "ROW_NUMBER"}) {
            String q = paramQuery.replace("?", function);
            // Same result for all 3 functions
            this.qs(q);
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

    @Test @Ignore("RANK aggregate not implemented without TopK")
    public void testRank() {
        this.qs("""
                WITH cte AS
                (
                SELECT *,
                         RANK() OVER (PARTITION BY DocumentID ORDER BY DateCreated) AS rn
                   FROM DocumentStatusLog
                )
                SELECT DocumentId, Status, DateCreated, rn
                FROM cte;
                 DocumentID | Status | DateCreated | rn
                ---------------------------------------
                 1          | S1| 2011-09-02       | 1
                 2          | S3| 2011-08-01       | 2
                 3          | S1| 2011-08-02       | 3
                (3 rows)""");
    }
}
