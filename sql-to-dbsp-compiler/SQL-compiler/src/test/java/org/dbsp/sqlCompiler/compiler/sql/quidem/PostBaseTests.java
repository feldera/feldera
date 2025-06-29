package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;

// Tests based on the POST database
// https://github.com/apache/calcite/blob/main/testkit/src/main/java/org/apache/calcite/test/CalciteAssert.java
public class PostBaseTests extends SqlIoTest {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.ignoreOrderBy = true;
        options.languageOptions.lenient = true;
        return options;
    }

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE EMP(
                   ename VARCHAR,
                   deptno INTEGER,
                   gender CHAR(1)
                );
                CREATE TABLE DEPT(
                   deptno INTEGER,
                   dname VARCHAR
                );
                CREATE TABLE EMPS(
                   empno  INTEGER,
                   name   VARCHAR,
                   deptno INTEGER,
                   gender CHAR(1),
                   city   VARCHAR,
                   empid  INTEGER,
                   age    INTEGER,
                   slacker BOOLEAN,
                   manager BOOLEAN,
                   joinedat DATE
                );
                CREATE TABLE TICKER(
                   symbol VARCHAR,
                   tstamp DATE,
                   price  INTEGER
                );
                INSERT INTO EMP VALUES
                   ('Jane', 10, 'F'),
                   ('Bob', 10, 'M'),
                   ('Eric', 20, 'M'),
                   ('Susan', 30, 'F'),
                   ('Alice', 30, 'F'),
                   ('Adam', 50, 'M'),
                   ('Eve', 50, 'F'),
                   ('Grace', 60, 'F'),
                   ('Wilma', cast(null as integer), 'F');
                INSERT INTO DEPT VALUES
                   (10, 'Sales'),
                   (20, 'Marketing'),
                   (30, 'Engineering'),
                   (40, 'Empty');
                INSERT INTO EMPS VALUES
                   (100, 'Fred',  10, CAST(NULL AS CHAR(1)), CAST(NULL AS VARCHAR(20)), 40,               25, TRUE,    FALSE, DATE '1996-08-03'),
                   (110, 'Eric',  20, 'M',                   'San Francisco',           3,                80, UNKNOWN, FALSE, DATE '2001-01-01'),
                   (110, 'John',  40, 'M',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2002-05-03'),
                   (120, 'Wilma', 20, 'F',                   CAST(NULL AS VARCHAR(20)), 1,                 5, UNKNOWN, TRUE,  DATE '2005-09-07'),
                   (130, 'Alice', 40, 'F',                   'Vancouver',               2, CAST(NULL AS INT), FALSE,   TRUE,  DATE '2007-01-01');
                INSERT INTO TICKER VALUES
                   ('ACME', '2017-12-01', 12),
                   ('ACME', '2017-12-02', 17),
                   ('ACME', '2017-12-03', 19),
                   ('ACME', '2017-12-04', 21),
                   ('ACME', '2017-12-05', 25),
                   ('ACME', '2017-12-06', 12),
                   ('ACME', '2017-12-07', 15),
                   ('ACME', '2017-12-08', 20),
                   ('ACME', '2017-12-09', 24),
                   ('ACME', '2017-12-10', 25),
                   ('ACME', '2017-12-11', 19),
                   ('ACME', '2017-12-12', 15),
                   ('ACME', '2017-12-13', 25),
                   ('ACME', '2017-12-14', 25),
                   ('ACME', '2017-12-15', 14),
                   ('ACME', '2017-12-16', 12),
                   ('ACME', '2017-12-17', 14),
                   ('ACME', '2017-12-18', 24),
                   ('ACME', '2017-12-19', 23),
                   ('ACME', '2017-12-20', 22);
                """);
    }
}
