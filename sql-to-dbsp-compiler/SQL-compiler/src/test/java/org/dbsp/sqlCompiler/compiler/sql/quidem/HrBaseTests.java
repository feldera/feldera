package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;

// From https://github.com/apache/calcite/blob/main/ubenchmark/src/jmh/java/org/apache/calcite/benchmarks/StatementTest.java
public class HrBaseTests extends SqlIoTest {
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
                CREATE TYPE Employee AS (
                   empid      INT,
                   deptno     INT,
                   name       VARCHAR,
                   salary     REAL,
                   commission INT
                );

                CREATE TABLE Emps(
                   empid      INT,
                   deptno     INT,
                   name       VARCHAR,
                   salary     REAL,
                   commission INT
                );

                CREATE TABLE Department(
                   deptno     INT,
                   name       VARCHAR,
                   employees  Employee ARRAY
                );

                INSERT INTO Emps VALUES
                (100, 10, 'Bill', 10000, 1000),
                (200, 20, 'Eric', 8000, 500),
                (150, 10, 'Sebastian', 7000, null),
                (110, 10, 'Theodore', 11500, 250)
                ;

                -- TODO: support for INSERT custom type
                --INSERT INTO Department VALUES
                --(10, 'Sales', ARRAY[Employee(100, 10, 'Bill', 10000, 1000), Employee(150, 10, 'Sebastian', 7000, null)]),
                --(30, 'Marketing', ARRAY()),
                --(40, 'HR', ARRAY[Employee(200, 20, 'Eric', 8000, 500)])
                --;
                """);
    }
}
