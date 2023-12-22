package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TopKTests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        String sql = "create table DocumentStatusLog (\n" +
                "    ID int,\n" +
                "    DocumentId int,\n" +
                "    Status VARCHAR,\n" +
                "    DateCreated DATE NOT NULL\n" +
                ");\n" +
                "INSERT INTO DocumentStatusLog VALUES(2, 1, 'S1', '2011-07-29')\n;" +
                "INSERT INTO DocumentStatusLog VALUES(3, 1, 'S2', '2011-07-30')\n;" +
                "INSERT INTO DocumentStatusLog VALUES(6, 1, 'S1', '2011-09-02')\n;" +
                "INSERT INTO DocumentStatusLog VALUES(1, 2, 'S1', '2011-07-28')\n;" +
                "INSERT INTO DocumentStatusLog VALUES(4, 2, 'S2', '2011-07-30')\n;" +
                "INSERT INTO DocumentStatusLog VALUES(5, 2, 'S3', '2011-08-01')\n;" +
                "INSERT INTO DocumentStatusLog VALUES(6, 3, 'S1', '2011-08-02')\n";
        compiler.compileStatements(sql);
    }

    @Test
    public void testTopK() {
        String paramQuery = "WITH cte AS\n" +
                "(\n" +
                "   SELECT *,\n" +
                "         ?() OVER (PARTITION BY DocumentID ORDER BY DateCreated DESC) AS rn\n" +
                "   FROM DocumentStatusLog\n" +
                ")\n" +
                "SELECT DocumentId, Status, DateCreated\n" +
                "FROM cte\n" +
                "WHERE rn <= 1;\n" +
                " DocumentID | Status | DateCreated \n" +
                "-----------------------------------\n" +
                " 1          | S1| 2011-09-02  \n" +
                " 2          | S3| 2011-08-01  \n" +
                " 3          | S1| 2011-08-02  \n" +
                "(3 rows)\n" +
                "\n" +
                "WITH cte AS\n" +
                "(\n" +
                "   SELECT *,\n" +
                "         ?() OVER (PARTITION BY DocumentID ORDER BY DateCreated) AS rn\n" +  // ? is a parameter
                "   FROM DocumentStatusLog\n" +
                ")\n" +
                "SELECT DocumentId, Status, DateCreated\n" +
                "FROM cte\n" +
                "WHERE rn <= 1;\n" +
                " DocumentID | Status | DateCreated \n" +
                "-----------------------------------\n" +
                " 1          | S1| 2011-07-29  \n" +
                " 2          | S1| 2011-07-28  \n" +
                " 3          | S1| 2011-08-02  \n" +
                "(3 rows)\n" +
                "\n" +
                "WITH cte AS\n" +
                "(\n" +
                "   SELECT *,\n" +
                "         ?() OVER (PARTITION BY DocumentID ORDER BY DateCreated DESC) AS rn\n" +  // ? is a parameter
                "   FROM DocumentStatusLog\n" +
                ")\n" +
                "SELECT DocumentId, Status, DateCreated\n" +
                "FROM cte\n" +
                "WHERE rn <= 1;\n" +
                " DocumentID | Status | DateCreated \n" +
                "-----------------------------------\n" +
                " 1          | S1| 2011-09-02  \n" +
                " 3          | S1| 2011-08-02  \n" +
                " 2          | S3| 2011-08-01  \n" +
                "(3 rows)";
        for (String function : new String[]{"RANK", "DENSE_RANK", "ROW_NUMBER"}) {
            String q = paramQuery.replace("?", function);
            // Same result for all 3 functions
            this.qs(q, false);
        }
    }

    @Test
    public void issue1174() {
        String sql = "CREATE TABLE event_t (\n" +
                "id BIGINT NOT NULL PRIMARY KEY,\n" +
                "site_id BIGINT NOT NULL,\n" +
                "event_type_id BIGINT NOT NULL,\n" +
                "event_date BIGINT NOT NULL, -- epoch\n" +
                "event_clear_date BIGINT -- epoch\n" +
                ");\n" +
                "\n" +
                "CREATE VIEW EVENT_DURATION_V AS\n" +
                "SELECT (event_date - event_clear_date) AS duration\n" +
                ",      event_type_id\n" +
                ",      site_id\n" +
                "FROM   event_t\n" +
                "WHERE  event_clear_date IS NOT NULL\n" +
                ";\n" +
                "\n" +
                "CREATE VIEW TOP_EVENT_DURATIONS_V AS\n" +
                "SELECT (duration * -1) as duration\n" +
                ",      event_type_id\n" +
                "FROM   (SELECT duration\n" +
                "        ,      event_type_id\n" +
                "        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id\n" +
                "                                  ORDER BY duration ASC) AS rnum\n" +
                "        FROM   EVENT_DURATION_V)\n" +
                "WHERE   rnum <= 3\n" +
                ";";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        Assert.assertEquals(0, compiler.messages.errorCount());
    }

    @Test
    public void issue1184() {
        String sql = "CREATE TABLE event_t (\n" +
                "id BIGINT NOT NULL PRIMARY KEY,\n" +
                "site_id BIGINT NOT NULL,\n" +
                "event_type_id BIGINT NOT NULL,\n" +
                "event_date BIGINT NOT NULL, -- epoch\n" +
                "event_clear_date BIGINT -- epoch\n" +
                ");\n" +
                "\n" +
                "CREATE VIEW EVENT_DURATION_V AS\n" +
                "SELECT (event_date - event_clear_date) AS duration\n" +
                ",      event_type_id\n" +
                ",      site_id\n" +
                "FROM   event_t\n" +
                "WHERE  event_clear_date IS NOT NULL\n" +
                ";\n" +
                "\n" +
                "CREATE VIEW TOP_EVENT_DURATIONS_V AS\n" +
                "SELECT duration\n" +
                ",      event_type_id\n" +
                "FROM   (SELECT duration\n" +
                "        ,      event_type_id\n" +
                "        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id\n" +
                "                                  ORDER BY duration DESC) AS rnum\n" +
                "        FROM   EVENT_DURATION_V)\n" +
                "WHERE   rnum = 1\n" +
                "ORDER BY 1 DESC\n" +
                ";";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        Assert.assertEquals(0, compiler.messages.errorCount());
    }

    @Test
    public void issue1185() {
        String sql = "CREATE TABLE event_t (\n" +
                "id BIGINT NOT NULL PRIMARY KEY,\n" +
                "site_id BIGINT NOT NULL,\n" +
                "event_type_id BIGINT NOT NULL,\n" +
                "event_date BIGINT NOT NULL, -- epoch\n" +
                "event_clear_date BIGINT -- epoch\n" +
                ");\n" +
                "\n" +
                "CREATE VIEW EVENT_DURATION_V AS\n" +
                "SELECT (event_date - event_clear_date) AS duration\n" +
                ",      event_type_id\n" +
                ",      site_id\n" +
                "FROM   event_t\n" +
                "WHERE  event_clear_date IS NOT NULL\n" +
                ";\n" +
                "\n" +
                "CREATE VIEW TOP_EVENT_DURATIONS_V AS\n" +
                "SELECT duration\n" +
                ",      site_id\n" +
                "FROM   (SELECT duration\n" +
                "        ,      site_id\n" +
                "        ,      ROW_NUMBER() OVER (PARTITION BY site_id\n" +
                "                                  ORDER BY duration ASC) AS rnum\n" +
                "        FROM   EVENT_DURATION_V)\n" +
                "WHERE   rnum = 1\n" +
                ";";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        Assert.assertEquals(0, compiler.messages.errorCount());
    }

    @Test
    public void issue1175() {
        String sql = "CREATE TABLE event_t (\n" +
                "id BIGINT NOT NULL PRIMARY KEY,\n" +
                "site_id BIGINT NOT NULL,\n" +
                "event_type_id BIGINT NOT NULL,\n" +
                "event_date BIGINT NOT NULL, -- epoch\n" +
                "event_clear_date BIGINT -- epoch\n" +
                ");\n" +
                "\n" +
                "CREATE VIEW EVENT_DURATION_V AS\n" +
                "SELECT (event_date - event_clear_date) AS duration\n" +
                ",      event_type_id\n" +
                ",      site_id\n" +
                "FROM   event_t\n" +
                "WHERE  event_clear_date IS NOT NULL\n" +
                ";\n" +
                "\n" +
                "CREATE VIEW TOP_EVENT_DURATIONS_V AS\n" +
                "SELECT (duration * -1) as duration\n" +
                ",      event_type_id\n" +
                "FROM   (SELECT duration\n" +
                "        ,      event_type_id\n" +
                "        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id\n" +
                "                                  ORDER BY duration ASC) AS rnum\n" +
                "        FROM   EVENT_DURATION_V)\n" +
                "WHERE   rnum <= 3\n" +
                ";";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        Assert.assertEquals(0, compiler.messages.errorCount());
    }

    @Test @Ignore("RANK aggregate not implemented without TopK")
    public void testRank() {
        this.qs("WITH cte AS\n" +
                "(\n" +"SELECT *,\n" +
                "         RANK() OVER (PARTITION BY DocumentID ORDER BY DateCreated) AS rn\n" +
                "   FROM DocumentStatusLog\n" +
                ")\n" +
                "SELECT DocumentId, Status, DateCreated, rn\n" +
                "FROM cte;\n" +
                " DocumentID | Status | DateCreated | rn\n" +
                "---------------------------------------\n" +
                " 1          | S1| 2011-09-02       | 1 \n" +
                " 2          | S3| 2011-08-01       | 2 \n" +
                " 3          | S1| 2011-08-02       | 3 \n" +
                "(3 rows)", false);
    }
}
