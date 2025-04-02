package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/**
 * From <a href="https://www.geeksforgeeks.org/pivot-and-unpivot-in-sql/">
 * Geeks for Geeks</a> and <a href="https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-pivot.html">
 * The SPARK documentation</a>
 */
public class PivotTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                Create Table GG (
                    CourseName varchar,
                    CourseCategory varchar,
                    Price int
                );

                Insert into GG  values('C', 'PROGRAMMING', 5000);
                Insert into GG  values('JAVA', 'PROGRAMMING', 6000);
                Insert into GG  values('PYTHON', 'PROGRAMMING', 8000);
                Insert into GG  values('PLACEMENT 100', 'INTERVIEWPREPARATION', 5000);
                CREATE TABLE person (id INT, name STRING, age INT, class INT, address STRING);
                INSERT INTO person VALUES
                    (100, 'John', 30, 1, 'Street 1'),
                    (200, 'Mary', NULL, 1, 'Street 2'),
                    (300, 'Mike', 80, 3, 'Street 3'),
                    (400, 'Dan', 50, 4, 'Street 4');

                CREATE TABLE FURNITURE(   type VARCHAR,   year INTEGER,   count INTEGER);
                INSERT INTO FURNITURE VALUES
                ('chair', 2020, 4),('table', 2021, 3),('chair', 2021, 4),('desk', 2023, 1),('table', 2023, 2)""");
                // "('bed', 2020, 5);");
    }

    @Test
    public void testGroupby() {
        this.q("""
                SELECT CourseName, Sum(Price)
                FROM GG
                GROUP BY CourseName;
                CourseName | Price
                -----------------
                 C| 5000
                 JAVA| 6000
                 PLACEMENT 100| 5000
                 PYTHON| 8000""");
    }

    @Test
    public void testGGPivot() {
        this.q("""
                SELECT CourseName, PG, IV
                FROM GG
                PIVOT (
                   SUM(Price) FOR CourseCategory IN (
                     'PROGRAMMING' AS PG,
                     'INTERVIEWPREPARATION' AS IV
                   )
                ) AS PivotTable;
                CourseName | PG | IV
                -----------------
                 C| 5000 | NULL
                 JAVA| 6000 | NULL
                 PLACEMENT 100| NULL | 5000
                 PYTHON| 8000 | NULL""");
    }

    @Test
    public void testSparkPivot() {
        this.q("""
                SELECT * FROM person
                    PIVOT (
                        SUM(age) AS a, AVG(class) AS cc
                        FOR name IN ('John' AS john, 'Mike' AS mike)
                    );
                +------+-----------+---------+---------+---------+---------+
                |  id  |  address  | john_a  | john_cc | mike_a  | mike_cc |
                +------+-----------+---------+---------+---------+---------+
                | 200  | Street 2|   NULL    | NULL    | NULL    | NULL    |
                | 100  | Street 1|   30      | 1       | NULL    | NULL    |
                | 300  | Street 3|   NULL    | NULL    | 80      | 3       |
                | 400  | Street 4|   NULL    | NULL    | NULL    | NULL    |
                +------+-----------+---------+---------+---------+---------+
                """);
        this.q("""
                SELECT * FROM person
                    PIVOT (
                        SUM(age) AS a, AVG(class) AS cc
                        FOR (name, age) IN (('John', 30) AS c1, ('Mike', 40) AS c2)
                    );
                +------+-----------+-------+-------+-------+-------+
                |  id  |  address  | c1_a  | c1_cc | c2_a  | c2_cc |
                +------+-----------+-------+-------+-------+-------+
                | 200  | Street 2|   NULL  | NULL  | NULL  | NULL  |
                | 100  | Street 1|   30    | 1     | NULL  | NULL  |
                | 300  | Street 3|   NULL  | NULL  | NULL  | NULL  |
                | 400  | Street 4|   NULL  | NULL  | NULL  | NULL  |
                +------+-----------+-------+-------+-------+-------+""");
    }

    @Test
    public void testPivotDoc() {
        this.q("""
                SELECT year, type, SUM(count) FROM FURNITURE GROUP BY year, type;
                year | type  | sum
                -----------------
                2020 | chair| 4
                2021 | table| 3
                2021 | chair| 4
                2023 | desk| 1
                2023 | table| 2""");
        this.q("""
                SELECT * FROM FURNITURE
                PIVOT (
                    SUM(count) AS ct
                    FOR type IN ('desk' AS desks, 'table' AS tables, 'chair' as chairs)
                );
                year | desks | tables | chairs
                ------------------------------
                2020 |       |        |    4
                2021 |       |     3  |    4
                2023 |     1 |     2  |
                """);
    }
}
