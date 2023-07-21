package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.junit.Test;

/**
 * From <a href="https://www.geeksforgeeks.org/pivot-and-unpivot-in-sql/">
 * Geeks for Geeks</a> and <a href="https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-pivot.html">
 * The SPARK documentation</a>
 */
public class PivotTests extends PostgresBaseTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        compiler.compileStatements("Create Table GG (\n" +
                "CourseName varchar,\n" +
                "CourseCategory varchar,\n" +
                "Price int\n" +
                "); \n" +
                "\n" +
                "Insert into GG  values('C', 'PROGRAMMING', 5000);\n" +
                "Insert into GG  values('JAVA', 'PROGRAMMING', 6000);\n" +
                "Insert into GG  values('PYTHON', 'PROGRAMMING', 8000);\n" +
                "Insert into GG  values('PLACEMENT 100', 'INTERVIEWPREPARATION', 5000);\n" +

                "CREATE TABLE person (id INT, name STRING, age INT, class INT, address STRING);\n" +
                "INSERT INTO person VALUES\n" +
                "    (100, 'John', 30, 1, 'Street 1'),\n" +
                "    (200, 'Mary', NULL, 1, 'Street 2'),\n" +
                "    (300, 'Mike', 80, 3, 'Street 3'),\n" +
                "    (400, 'Dan', 50, 4, 'Street 4');");
    }

    @Test
    public void testGroupby() {
        this.q("SELECT CourseName, Sum(Price)\n" +
                "FROM GG \n" +
                "GROUP BY CourseName;\n" +
                "CourseName | Price\n" +
                "-----------------\n" +
                "C| 5000\n" +
                "JAVA| 6000\n" +
                "PLACEMENT 100| 5000\n" +
                "PYTHON| 8000");
    }

    @Test
    public void testGGPivot() {
        this.q("SELECT CourseName, \"PG\", \"IV\"\n" +
                "FROM GG \n" +
                "PIVOT (\n" +
                "  SUM(Price) FOR CourseCategory IN (" +
                "         'PROGRAMMING' AS PG, " +
                "         'INTERVIEWPREPARATION' AS IV) \n" +
                ") AS PivotTable;\n" +
                "CourseName | PG | IV\n" +
                "-----------------\n" +
                "C| 5000 | NULL\n" +
                "JAVA| 6000 | NULL\n" +
                "PLACEMENT 100| NULL | 5000\n" +
                "PYTHON| 8000 | NULL");
    }

    @Test
    public void testSparkPivot() {
        this.q("SELECT * FROM person\n" +
                "    PIVOT (\n" +
                "        SUM(age) AS a, AVG(class) AS cc\n" +
                "        FOR name IN ('John' AS john, 'Mike' AS mike)\n" +
                "    );\n" +
                "+------+-----------+---------+---------+---------+---------+\n" +
                "|  id  |  address  | john_a  | john_cc | mike_a  | mike_cc |\n" +
                "+------+-----------+---------+---------+---------+---------+\n" +
                "| 200  |Street 2| NULL    | NULL    | NULL    | NULL    |\n" +
                "| 100  |Street 1| 30      | 1       | NULL    | NULL    |\n" +
                "| 300  |Street 3| NULL    | NULL    | 80      | 3       |\n" +
                "| 400  |Street 4| NULL    | NULL    | NULL    | NULL    |");
        this.q("SELECT * FROM person\n" +
                "    PIVOT (\n" +
                "        SUM(age) AS a, AVG(class) AS cc\n" +
                "        FOR (name, age) IN (('John', 30) AS c1, ('Mike', 40) AS c2)\n" +
                "    );\n" +
                "+------+-----------+-------+-------+-------+-------+\n" +
                "|  id  |  address  | c1_a  | c1_cc | c2_a  | c2_cc |\n" +
                "+------+-----------+-------+-------+-------+-------+\n" +
                "| 200  |Street 2| NULL  | NULL  | NULL  | NULL  |\n" +
                "| 100  |Street 1| 30    | 1     | NULL  | NULL  |\n" +
                "| 300  |Street 3| NULL  | NULL  | NULL  | NULL  |\n" +
                "| 400  |Street 4| NULL  | NULL  | NULL  | NULL  |");
    }
}
