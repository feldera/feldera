package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Test;

/** Tests from Calcite outer.iq */
public class OuterTests extends PostBaseTests {
    @Test
    public void testOuter() {
        this.qs("""
                select * from emp;
                +-------+--------+--------+
                | ENAME | DEPTNO | GENDER |
                +-------+--------+--------+
                | Jane  |     10 | F|
                | Bob   |     10 | M|
                | Eric  |     20 | M|
                | Susan |     30 | F|
                | Alice |     30 | F|
                | Adam  |     50 | M|
                | Eve   |     50 | F|
                | Grace |     60 | F|
                | Wilma |        | F|
                +-------+--------+--------+
                (9 rows)
                
                select * from emp join dept on emp.deptno = dept.deptno;
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Bob   |     10 | M|            10 | Sales       |
                | Eric  |     20 | M|            20 | Marketing   |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                +-------+--------+--------+---------+-------------+
                (5 rows)
                
                select * from emp join dept on emp.deptno = dept.deptno and emp.gender = 'F';
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Alice |     30 | F|            30 | Engineering |
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                +-------+--------+--------+---------+-------------+
                (3 rows)
                
                select * from emp join dept on emp.deptno = dept.deptno where emp.gender = 'F';
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                +-------+--------+--------+---------+-------------+
                (3 rows)
                
                select * from (select * from emp where gender ='F') as emp join dept on emp.deptno = dept.deptno;
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                +-------+--------+--------+---------+-------------+
                (3 rows)
                
                select * from emp left join dept on emp.deptno = dept.deptno and emp.gender = 'F';
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Adam  |     50 | M|               |NULL         |
                | Alice |     30 | F|            30 | Engineering |
                | Bob   |     10 | M|               |NULL         |
                | Eric  |     20 | M|               |NULL         |
                | Eve   |     50 | F|               |NULL         |
                | Grace |     60 | F|               |NULL         |
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Wilma |        | F|               |NULL         |
                +-------+--------+--------+---------+-------------+
                (9 rows)
                
                select * from emp left join dept on emp.deptno = dept.deptno where emp.gender = 'F';
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                | Eve   |     50 | F|               |NULL         |
                | Grace |     60 | F|               |NULL         |
                | Wilma |        | F|               |NULL         |
                +-------+--------+--------+---------+-------------+
                (6 rows)
                
                select * from (select * from emp where gender ='F') as emp left join dept on emp.deptno = dept.deptno;
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                | Eve   |     50 | F|               |NULL         |
                | Grace |     60 | F|               |NULL         |
                | Wilma |        | F|               |NULL         |
                +-------+--------+--------+---------+-------------+
                (6 rows)
                
                select * from emp right join dept on emp.deptno = dept.deptno and emp.gender = 'F';
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Alice |     30 | F|            30 | Engineering |
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                |NULL   |        |NULL|          20 | Marketing   |
                |NULL   |        |NULL|          40 | Empty       |
                +-------+--------+--------+---------+-------------+
                (5 rows)
                
                select * from emp right join dept on emp.deptno = dept.deptno where emp.gender = 'F';
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                +-------+--------+--------+---------+-------------+
                (3 rows)
                
                select * from (select * from emp where gender ='F') as emp right join dept on emp.deptno = dept.deptno;
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                |NULL   |        |NULL|          20 | Marketing   |
                |NULL   |        |NULL|          40 | Empty       |
                +-------+--------+--------+---------+-------------+
                (5 rows)
                
                select * from emp full join dept on emp.deptno = dept.deptno and emp.gender = 'F';
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Adam  |     50 | M|               |NULL         |
                | Alice |     30 | F|            30 | Engineering |
                | Bob   |     10 | M|               |NULL         |
                | Eric  |     20 | M|               |NULL         |
                | Eve   |     50 | F|               |NULL         |
                | Grace |     60 | F|               |NULL         |
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Wilma |        | F|               |NULL         |
                |NULL   |        |NULL|          20 | Marketing   |
                |NULL   |        |NULL|          40 | Empty       |
                +-------+--------+--------+---------+-------------+
                (11 rows)
                
                select * from emp full join dept on emp.deptno = dept.deptno where emp.gender = 'F';
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                | Eve   |     50 | F|               |NULL         |
                | Grace |     60 | F|               |NULL         |
                | Wilma |        | F|               |NULL         |
                +-------+--------+--------+---------+-------------+
                (6 rows)
                
                select * from (select * from emp where gender ='F') as emp full join dept on emp.deptno = dept.deptno;
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                | Eve   |     50 | F|               |NULL         |
                | Grace |     60 | F|               |NULL         |
                | Wilma |        | F|               |NULL         |
                |NULL   |        |NULL|          20 | Marketing   |
                |NULL   |        |NULL|          40 | Empty       |
                +-------+--------+--------+---------+-------------+
                (8 rows)
                
                -- same as above, but expressed as a nestedLoop-join
                select * from (select * from emp where gender ='F') as emp full join dept on emp.deptno - dept.deptno = 0;
                +-------+--------+--------+---------+-------------+
                | ENAME | DEPTNO | GENDER | DEPTNO0 | DNAME       |
                +-------+--------+--------+---------+-------------+
                | Jane  |     10 | F|            10 | Sales       |
                | Susan |     30 | F|            30 | Engineering |
                | Alice |     30 | F|            30 | Engineering |
                | Eve   |     50 | F|               |NULL         |
                | Grace |     60 | F|               |NULL         |
                | Wilma |        | F|               |NULL         |
                |NULL   |        |NULL|          20 | Marketing   |
                |NULL   |        |NULL|          40 | Empty       |
                +-------+--------+--------+---------+-------------+
                (8 rows)
                
                -- [CALCITE-554] Outer join over NULL keys generates wrong result
                with t1(x) as (select * from  (values (1),(2), (case when 1 = 1 then null else 3 end)) as t(x)),
                  t2(x) as (select * from  (values (1),(case when 1 = 1 then null else 3 end)) as t(x))
                select t1.x from t1 left join t2 on t1.x = t2.x;
                +---+
                | X |
                +---+
                | 1 |
                | 2 |
                |   |
                +---+
                (3 rows)
                
                -- Equivalent query, using CAST, and skipping unnecessary aliases
                -- (Postgres doesn't like the missing alias, or the missing parentheses.)
                with t1(x) as (select * from (values 1, 2, cast(null as integer))),
                  t2(x) as (select * from (values 1, cast(null as integer)))
                select t1.x from t1 left join t2 on t1.x = t2.x;
                +---+
                | X |
                +---+
                | 1 |
                | 2 |
                |   |
                +---+
                (3 rows)
                
                -- Similar query, projecting left and right key columns
                with t1(x) as (select * from (values (1), (2), (cast(null as integer))) as t),
                  t2(x) as (select * from (values (1), (cast(null as integer))) as t)
                select t1.x, t2.x from t1 left join t2 on t1.x = t2.x;
                +---+---+
                | X | X |
                +---+---+
                | 1 | 1 |
                | 2 |   |
                |   |   |
                +---+---+
                (3 rows)
                
                -- Similar, with 2 columns on each side projecting both columns
                with t1(x, y) as (select * from (values (1, 10), (2, 20), (cast(null as integer), 30)) as t),
                  t2(x, y) as (select * from (values (1, 100), (cast(null as integer), 200)) as t)
                select * from t1 left join t2 on t1.x = t2.x;
                +---+----+----+-----+
                | X | Y  | X0 | Y0  |
                +---+----+----+-----+
                | 1 | 10 |  1 | 100 |
                | 2 | 20 |    |     |
                |   | 30 |    |     |
                +---+----+----+-----+
                (3 rows)
                
                -- Similar, full join
                with t1(x, y) as (select * from (values (1, 10), (2, 20), (cast(null as integer), 30)) as t),
                  t2(x, y) as (select * from (values (1,100), (cast(null as integer), 200)) as t)
                select * from t1 full join t2 on t1.x = t2.x;
                +---+----+----+-----+
                | X | Y  | X0 | Y0  |
                +---+----+----+-----+
                | 1 | 10 |  1 | 100 |
                | 2 | 20 |    |     |
                |   | 30 |    |     |
                |   |    |    | 200 |
                +---+----+----+-----+
                (4 rows)""", false);
    }
}
