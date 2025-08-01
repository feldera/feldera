package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;

/* https://github.com/apache/calcite/blob/main/core/src/test/resources/sql/scalar.iq */
public class FoodmartBaseTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // https://github.com/apache/calcite/blob/main/innodb/src/test/resources/scott.sql
        compiler.submitStatementsForCompilation("""
                DROP TABLE IF EXISTS DEPT;
                CREATE TABLE DEPT(
                    DEPTNO TINYINT NOT NULL,
                    DNAME VARCHAR(50) NOT NULL,
                    LOC VARCHAR(20)
                );
                CREATE TABLE EMP(
                    EMPNO INT NOT NULL,
                    ENAME VARCHAR(100) NOT NULL,
                    JOB VARCHAR(15) NOT NULL,
                    AGE SMALLINT,
                    MGR BIGINT,
                    HIREDATE DATE,
                    SAL DECIMAL(8,2) NOT NULL,
                    COMM DECIMAL(6,2),
                    DEPTNO TINYINT,
                    EMAIL VARCHAR(100),
                    CREATE_DATETIME TIMESTAMP,
                    CREATE_TIME TIME,
                    UPSERT_TIME TIMESTAMP NOT NULL
                );

                INSERT INTO DEPT VALUES(10,'ACCOUNTING','NEW YORK');
                INSERT INTO DEPT VALUES(20,'RESEARCH','DALLAS');
                INSERT INTO DEPT VALUES(30,'SALES','CHICAGO');
                INSERT INTO DEPT VALUES(40,'OPERATIONS','BOSTON');

                INSERT INTO EMP VALUES(7369,'SMITH','CLERK',30,7902,'1980-12-17',800,NULL,20,'smith@calcite','2020-01-01 18:35:40','18:35:40','2020-01-01 18:35:40');
                INSERT INTO EMP VALUES(7499,'ALLEN','SALESMAN',24,7698,'1981-02-20',1600,300,30,'allen@calcite','2018-04-09 09:00:00','09:00:00','2018-04-09 09:00:00');
                INSERT INTO EMP VALUES(7521,'WARD','SALESMAN',41,7698,'1981-02-22',1250,500,30,'ward@calcite','2019-11-16 10:26:40','10:26:40','2019-11-16 10:26:40');
                INSERT INTO EMP VALUES(7566,'JONES','MANAGER',28,7839,'1981-02-04',2975,NULL,20,'jones@calcite','2015-03-09 22:16:30','22:16:30','2015-03-09 22:16:30');
                INSERT INTO EMP VALUES(7654,'MARTIN','SALESMAN',27,7698,'1981-09-28',1250,1400,30,'martin@calcite','2018-09-02 12:12:56','12:12:56','2018-09-02 12:12:56');
                INSERT INTO EMP VALUES(7698,'BLAKE','MANAGER',38,7839,'1981-01-05',2850,NULL,30,'blake@calcite','2018-06-01 14:45:00','14:45:00','2018-06-01 14:45:00');
                INSERT INTO EMP VALUES(7782,'CLARK','MANAGER',32,7839,'1981-06-09',2450,NULL,10,NULL,'2019-09-30 02:14:56','02:14:56','2019-09-30 02:14:56');
                INSERT INTO EMP VALUES(7788,'SCOTT','ANALYST',45,7566,'1987-04-19',3000,NULL,20,'scott@calcite','2019-07-28 12:12:12','12:12:12','2019-07-28 12:12:12');
                INSERT INTO EMP VALUES(7839,'KING','PRESIDENT',22,NULL,'1981-11-17',5000,NULL,10,'king@calcite','2019-06-08 15:15:15',NULL,'2019-06-08 15:15:15');
                INSERT INTO EMP VALUES(7844,'TURNER','SALESMAN',54,7698,'1981-09-08',1500,0,30,'turner@calcite','2017-08-17 22:01:37','22:01:37','2017-08-17 22:01:37');
                INSERT INTO EMP VALUES(7876,'ADAMS','CLERK',35,7788,'1987-05-23',1100,NULL,20,'adams@calcite',NULL,'23:11:06','2017-08-18 23:11:06');
                INSERT INTO EMP VALUES(7900,'JAMES','CLERK',40,7698,'1981-12-03',950,NULL,30,'james@calcite','2020-01-02 12:19:00','12:19:00','2020-01-02 12:19:00');
                INSERT INTO EMP VALUES(7902,'FORD','ANALYST',28,7566,'1981-12-03',3000,NULL,20,'ford@calcite','2019-05-29 00:00:00',NULL,'2019-05-29 00:00:00');
                INSERT INTO EMP VALUES(7934,'MILLER','CLERK',32,7782,'1982-01-23',1300,NULL,10,NULL,'2016-09-02 23:15:01','23:15:01','2016-09-02 23:15:01')
                """);
    }
}
