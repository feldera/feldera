package org.dbsp.sqlCompiler.compiler.foodmart;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.postgres.PostgresBaseTest;
import org.dbsp.util.Logger;
import org.junit.Ignore;
import org.junit.Test;

/**
 * https://github.com/apache/calcite/blob/bdfb17029f7e205f895dc3dfd0f37c8ff2520823/core/src/test/resources/sql/scalar.iq
 */
public class FoodmartBaseTests extends PostgresBaseTest {
    // https://github.com/apache/calcite/blob/bdfb17029f7e205f895dc3dfd0f37c8ff2520823/innodb/src/test/resources/scott.sq
    @Override
    public void prepareData(DBSPCompiler compiler) {
        compiler.compileStatements("DROP TABLE IF EXISTS DEPT;\n" +
                "CREATE TABLE DEPT(\n" +
                "    DEPTNO TINYINT NOT NULL,\n" +
                "    DNAME VARCHAR(50) NOT NULL,\n" +
                "    LOC VARCHAR(20)\n" +
                ");\n" +
                "CREATE TABLE EMP(\n" +
                "    EMPNO INT NOT NULL,\n" +
                "    ENAME VARCHAR(100) NOT NULL,\n" +
                "    JOB VARCHAR(15) NOT NULL,\n" +
                "    AGE SMALLINT,\n" +
                "    MGR BIGINT,\n" +
                "    HIREDATE DATE,\n" +
                "    SAL DECIMAL(8,2) NOT NULL,\n" +
                "    COMM DECIMAL(6,2),\n" +
                "    DEPTNO TINYINT,\n" +
                "    EMAIL VARCHAR(100),\n" +
                "    CREATE_DATETIME TIMESTAMP,\n" +
                "    CREATE_TIME TIME,\n" +
                "    UPSERT_TIME TIMESTAMP NOT NULL\n" +
                ");\n" +
                "\n" +
                "INSERT INTO DEPT VALUES(10,'ACCOUNTING','NEW YORK');\n" +
                "INSERT INTO DEPT VALUES(20,'RESEARCH','DALLAS');\n" +
                "INSERT INTO DEPT VALUES(30,'SALES','CHICAGO');\n" +
                "INSERT INTO DEPT VALUES(40,'OPERATIONS','BOSTON');\n" +
                "\n" +
                "INSERT INTO EMP VALUES(7369,'SMITH','CLERK',30,7902,'1980-12-17',800,NULL,20,'smith@calcite','2020-01-01 18:35:40','18:35:40','2020-01-01 18:35:40');\n" +
                "INSERT INTO EMP VALUES(7499,'ALLEN','SALESMAN',24,7698,'1981-02-20',1600,300,30,'allen@calcite','2018-04-09 09:00:00','09:00:00','2018-04-09 09:00:00');\n" +
                "INSERT INTO EMP VALUES(7521,'WARD','SALESMAN',41,7698,'1981-02-22',1250,500,30,'ward@calcite','2019-11-16 10:26:40','10:26:40','2019-11-16 10:26:40');\n" +
                "INSERT INTO EMP VALUES(7566,'JONES','MANAGER',28,7839,'1981-02-04',2975,NULL,20,'jones@calcite','2015-03-09 22:16:30','22:16:30','2015-03-09 22:16:30');\n" +
                "INSERT INTO EMP VALUES(7654,'MARTIN','SALESMAN',27,7698,'1981-09-28',1250,1400,30,'martin@calcite','2018-09-02 12:12:56','12:12:56','2018-09-02 12:12:56');\n" +
                "INSERT INTO EMP VALUES(7698,'BLAKE','MANAGER',38,7839,'1981-01-05',2850,NULL,30,'blake@calcite','2018-06-01 14:45:00','14:45:00','2018-06-01 14:45:00');\n" +
                "INSERT INTO EMP VALUES(7782,'CLARK','MANAGER',32,7839,'1981-06-09',2450,NULL,10,NULL,'2019-09-30 02:14:56','02:14:56','2019-09-30 02:14:56');\n" +
                "INSERT INTO EMP VALUES(7788,'SCOTT','ANALYST',45,7566,'1987-04-19',3000,NULL,20,'scott@calcite','2019-07-28 12:12:12','12:12:12','2019-07-28 12:12:12');\n" +
                "INSERT INTO EMP VALUES(7839,'KING','PRESIDENT',22,NULL,'1981-11-17',5000,NULL,10,'king@calcite','2019-06-08 15:15:15',NULL,'2019-06-08 15:15:15');\n" +
                "INSERT INTO EMP VALUES(7844,'TURNER','SALESMAN',54,7698,'1981-09-08',1500,0,30,'turner@calcite','2017-08-17 22:01:37','22:01:37','2017-08-17 22:01:37');\n" +
                "INSERT INTO EMP VALUES(7876,'ADAMS','CLERK',35,7788,'1987-05-23',1100,NULL,20,'adams@calcite',NULL,'23:11:06','2017-08-18 23:11:06');\n" +
                "INSERT INTO EMP VALUES(7900,'JAMES','CLERK',40,7698,'1981-12-03',950,NULL,30,'james@calcite','2020-01-02 12:19:00','12:19:00','2020-01-02 12:19:00');\n" +
                "INSERT INTO EMP VALUES(7902,'FORD','ANALYST',28,7566,'1981-12-03',3000,NULL,20,'ford@calcite','2019-05-29 00:00:00',NULL,'2019-05-29 00:00:00');\n" +
                "INSERT INTO EMP VALUES(7934,'MILLER','CLERK',32,7782,'1982-01-23',1300,NULL,10,NULL,'2016-09-02 23:15:01','23:15:01','2016-09-02 23:15:01')\n");
    }
    
    @Test
    public void testSelect() {
        this.q("SELECT * FROM DEPT;\n" +
                "DEPT NO | DNAME | LOC\n" +
                "---------------------\n" +
                "10 |ACCOUNTING|NEW YORK\n" +
                "20 |RESEARCH|DALLAS\n" +
                "30 |SALES|CHICAGO\n" +
                "40 |OPERATIONS|BOSTON");
    }

    @Test
    public void testScalar() {
        this.qs("select deptno, (select min(empno) from emp where deptno = dept.deptno) as x from dept;\n" +
                "+--------+------+\n" +
                "| DEPTNO | X    |\n" +
                "+--------+------+\n" +
                "|     10 | 7782 |\n" +
                "|     20 | 7369 |\n" +
                "|     30 | 7499 |\n" +
                "|     40 |      |\n" +
                "+--------+------+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select count(*) from emp where deptno = dept.deptno) as x from dept;\n" +
                "+--------+---+\n" +
                "| DEPTNO | X |\n" +
                "+--------+---+\n" +
                "|     10 | 3 |\n" +
                "|     20 | 5 |\n" +
                "|     30 | 6 |\n" +
                "|     40 | 0 |\n" +
                "+--------+---+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select count(*) from emp where deptno = dept.deptno group by deptno) as x from dept;\n" +
                "+--------+---+\n" +
                "| DEPTNO | X |\n" +
                "+--------+---+\n" +
                "|     10 | 3 |\n" +
                "|     20 | 5 |\n" +
                "|     30 | 6 |\n" +
                "|     40 |   |\n" +
                "+--------+---+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select sum(cast(empno as int)) from emp where deptno = dept.deptno group by deptno) as x from dept;\n" +
                "+--------+-------+\n" +
                "| DEPTNO | X     |\n" +
                "+--------+-------+\n" +
                "|     10 | 23555 |\n" +
                "|     20 | 38501 |\n" +
                "|     30 | 46116 |\n" +
                "|     40 |       |\n" +
                "+--------+-------+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select count(*) from emp where 1 = 0) as x from dept;\n" +
                "+--------+---+\n" +
                "| DEPTNO | X |\n" +
                "+--------+---+\n" +
                "|     10 | 0 |\n" +
                "|     20 | 0 |\n" +
                "|     30 | 0 |\n" +
                "|     40 | 0 |\n" +
                "+--------+---+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select count(*) from emp where 1 = 0 group by ()) as x from dept;\n" +
                "+--------+---+\n" +
                "| DEPTNO | X |\n" +
                "+--------+---+\n" +
                "|     10 | 0 |\n" +
                "|     20 | 0 |\n" +
                "|     30 | 0 |\n" +
                "|     40 | 0 |\n" +
                "+--------+---+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select sum(empno) from emp where 1 = 0) as x from dept;\n" +
                "+--------+---+\n" +
                "| DEPTNO | X |\n" +
                "+--------+---+\n" +
                "|     10 |   |\n" +
                "|     20 |   |\n" +
                "|     30 |   |\n" +
                "|     40 |   |\n" +
                "+--------+---+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select empno from emp where 1 = 0) as x from dept;\n" +
                "+--------+---+\n" +
                "| DEPTNO | X |\n" +
                "+--------+---+\n" +
                "|     10 |   |\n" +
                "|     20 |   |\n" +
                "|     30 |   |\n" +
                "|     40 |   |\n" +
                "+--------+---+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select empno from emp where emp.deptno = dept.deptno and job = 'PRESIDENT') as x from dept;\n" +
                "+--------+------+\n" +
                "| DEPTNO | X    |\n" +
                "+--------+------+\n" +
                "|     10 | 7839 |\n" +
                "|     20 |      |\n" +
                "|     30 |      |\n" +
                "|     40 |      |\n" +
                "+--------+------+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select sum(empno) from emp where 1 = 0 group by ()) as x from dept;\n" +
                "+--------+---+\n" +
                "| DEPTNO | X |\n" +
                "+--------+---+\n" +
                "|     10 |   |\n" +
                "|     20 |   |\n" +
                "|     30 |   |\n" +
                "|     40 |   |\n" +
                "+--------+---+\n" +
                "(4 rows)\n");
    }

    @Test @Ignore("LIMIT not yet implemented")
    public void limitTests() {
        Logger.INSTANCE.setLoggingLevel(CalciteCompiler.class, 4);
        this.qs("select deptno, (select sum(empno) from emp where deptno = dept.deptno limit 1) as x from dept;\n" +
                "+--------+----------------------+\n" +
                "| DEPTNO |          X           |\n" +
                "+--------+----------------------+\n" +
                "| 10     | 23555                |\n" +
                "| 20     | 38501                |\n" +
                "| 30     | 46116                |\n" +
                "| 40     | null                 |\n" +
                "+--------+----------------------+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select sum(empno) from emp where deptno = dept.deptno limit 0) as x from dept;\n" +
                "+--------+----------------------+\n" +
                "| DEPTNO |          X           |\n" +
                "+--------+----------------------+\n" +
                "| 10     | 23555                |\n" +
                "| 20     | 38501                |\n" +
                "| 30     | 46116                |\n" +
                "| 40     | null                 |\n" +
                "+--------+----------------------+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select deptno from emp where deptno = dept.deptno limit 1) as x from dept;\n" +
                "+--------+------+\n" +
                "| DEPTNO |  X   |\n" +
                "+--------+------+\n" +
                "| 10     | 10   |\n" +
                "| 20     | 20   |\n" +
                "| 30     | 30   |\n" +
                "| 40     | null |\n" +
                "+--------+------+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select deptno from emp where deptno = dept.deptno limit 0) as x from dept;\n" +
                "+--------+---+\n" +
                "| DEPTNO | X |\n" +
                "+--------+---+\n" +
                "|     10 |   |\n" +
                "|     20 |   |\n" +
                "|     30 |   |\n" +
                "|     40 |   |\n" +
                "+--------+---+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select empno from emp where deptno = dept.deptno order by empno limit 1) as x from dept;\n" +
                "+--------+--------+\n" +
                "| DEPTNO |   X    |\n" +
                "+--------+--------+\n" +
                "| 10     | 7369   |\n" +
                "| 20     | 7369   |\n" +
                "| 30     | 7369   |\n" +
                "| 40     | 7369   |\n" +
                "+--------+--------+\n" +
                "(4 rows)\n" +
                "\n" +
                "select deptno, (select empno from emp order by empno limit 1) as x from dept;\n" +
                "+--------+------+\n" +
                "| DEPTNO | X    |\n" +
                "+--------+------+\n" +
                "|     10 | 7369 |\n" +
                "|     20 | 7369 |\n" +
                "|     30 | 7369 |\n" +
                "|     40 | 7369 |\n" +
                "+--------+------+\n" +
                "(4 rows)");
    }
}
