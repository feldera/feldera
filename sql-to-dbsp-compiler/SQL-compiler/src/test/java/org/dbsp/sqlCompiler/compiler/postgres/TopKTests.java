package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.junit.Test;

public class TopKTests extends PostgresBaseTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        String sql = "create table DocumentStatusLog (\n" +
                "    ID int,\n" +
                "    DocumentId int,\n" +
                "    Status VARCHAR,\n" +
                "    DateCreated DATE\n" +
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
        this.qs("WITH cte AS\n" +
                "(\n" +
                "   SELECT *,\n" +
                "         RANK() OVER (PARTITION BY DocumentID ORDER BY DateCreated DESC) AS rn\n" +
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
                "         RANK() OVER (PARTITION BY DocumentID ORDER BY DateCreated) AS rn\n" +
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
                "(3 rows)", false);
    }
}
