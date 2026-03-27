package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** Regression tests that executed in incremental mode */
public class IncrementalRegression2Tests extends SqlIoTest {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.throwOnError = false;
        options.languageOptions.incrementalize = true;
        options.languageOptions.optimizationLevel = 2;
        options.languageOptions.ignoreOrderBy = true;
        return options;
    }

    @Test
    public void issue5815() {
        var ccs = this.getCCS("""
                CREATE TABLE orders1 (
                    order_id     INT NOT NULL PRIMARY KEY,
                    customer_id  INT,
                    amount       DECIMAL(10,2)
                );
                
                CREATE TABLE orders2 (
                    order_id     INT NOT NULL PRIMARY KEY,
                    customer_id  INT,
                    amount       DECIMAL(10,2)
                );
                
                CREATE TABLE customers (
                    customer_id  INT NOT NULL PRIMARY KEY,
                    name         TEXT,
                    first        TEXT
                );
                
                CREATE LOCAL VIEW V1 AS SELECT
                    o.order_id,
                    o.amount,
                    c.name
                FROM orders1 AS o
                LEFT JOIN customers AS c
                    ON o.customer_id = c.customer_id;
                
                CREATE LOCAL VIEW V2 AS SELECT
                    o.order_id,
                    o.amount,
                    c.first
                FROM orders2 AS o
                LEFT JOIN customers AS c
                    ON o.customer_id = c.customer_id;
                
                CREATE VIEW V AS (SELECT * FROM V1) UNION ALL (SELECT * FROM V2);""");
        // Validated on Postgres
        ccs.step("""
                INSERT INTO customers (customer_id, name, first) VALUES
                  (1, 'Johnson', 'Alice'),
                  (2, 'Smith',   'Bob'),
                  (3, 'White',   'Carol');
                INSERT INTO orders1 (order_id, customer_id, amount) VALUES
                  (101, 1, 120.50),   -- matches Alice
                  (102, 2,  75.00),   -- matches Bob
                  (103, 9,  33.33);   -- no matching customer
                INSERT INTO orders2 (order_id, customer_id, amount) VALUES
                  (201, 2,  88.00),   -- matches Bob
                  (202, 3, 150.00),   -- matches Carol
                  (203, 8,  42.42);   -- no matching customer
                """, """
                 order_id | amount | name    | weight
                ---------------------------------------
                 101      | 120.50 | Johnson|  1
                 102      | 75.00  | Smith|    1
                 103      | 33.33  |NULL     | 1
                 201      | 88.00  | Bob|      1
                 202      | 150.00 | Carol|    1
                 203      | 42.42  |NULL     | 1""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int mapIndexCount = 0;

            @Override
            public void postorder(DBSPMapIndexOperator unused) {
                this.mapIndexCount++;
            }

            @Override
            public void endVisit() {
                // If sharing of map-index operators works, there are 3,
                // otherwise there are 4
                Assert.assertEquals(3, this.mapIndexCount);
            }
        });
    }

    @Test
    public void issue5815a() {
        // Same as before, but simple inner joins
        var ccs = this.getCCS("""
                CREATE TABLE orders1 (
                    order_id     INT NOT NULL PRIMARY KEY,
                    customer_id  INT,
                    amount       DECIMAL(10,2)
                );
                
                CREATE TABLE orders2 (
                    order_id     INT NOT NULL PRIMARY KEY,
                    customer_id  INT,
                    amount       DECIMAL(10,2)
                );
                
                CREATE TABLE customers (
                    customer_id  INT NOT NULL PRIMARY KEY,
                    name         TEXT,
                    first        TEXT
                );
                
                CREATE LOCAL VIEW V1 AS SELECT
                    o.order_id,
                    o.amount,
                    c.name
                FROM orders1 AS o
                JOIN customers AS c
                    ON o.customer_id = c.customer_id;
                
                CREATE LOCAL VIEW V2 AS SELECT
                    o.order_id,
                    o.amount,
                    c.first
                FROM orders2 AS o
                JOIN customers AS c
                    ON o.customer_id = c.customer_id;
                
                CREATE VIEW V AS (SELECT * FROM V1) UNION ALL (SELECT * FROM V2);""");
        // Validated on Postgres
        ccs.step("""
                INSERT INTO customers (customer_id, name, first) VALUES
                  (1, 'Johnson', 'Alice'),
                  (2, 'Smith',   'Bob'),
                  (3, 'White',   'Carol');
                INSERT INTO orders1 (order_id, customer_id, amount) VALUES
                  (101, 1, 120.50),   -- matches Alice
                  (102, 2,  75.00),   -- matches Bob
                  (103, 9,  33.33);   -- no matching customer
                INSERT INTO orders2 (order_id, customer_id, amount) VALUES
                  (201, 2,  88.00),   -- matches Bob
                  (202, 3, 150.00),   -- matches Carol
                  (203, 8,  42.42);   -- no matching customer
                """, """
                 order_id | amount | name    | weight
                ---------------------------------------
                 101      | 120.50 | Johnson|  1
                 102      | 75.00  | Smith|    1
                 201      | 88.00  | Bob|      1
                 202      | 150.00 | Carol|    1""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int mapIndexCount = 0;

            @Override
            public void postorder(DBSPMapIndexOperator unused) {
                this.mapIndexCount++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.mapIndexCount);
            }
        });
    }

    @Test
    public void issue5815b() {
        // Same as before, but one inne join and one left join,
        // where customers is on the left side of the left join
        var ccs = this.getCCS("""
                CREATE TABLE orders1 (
                    order_id     INT NOT NULL PRIMARY KEY,
                    customer_id  INT,
                    amount       DECIMAL(10,2)
                );
                
                CREATE TABLE orders2 (
                    order_id     INT NOT NULL PRIMARY KEY,
                    customer_id  INT,
                    amount       DECIMAL(10,2)
                );
                
                CREATE TABLE customers (
                    customer_id  INT NOT NULL PRIMARY KEY,
                    name         TEXT,
                    first        TEXT
                );
                
                CREATE LOCAL VIEW V1 AS SELECT
                    o.order_id,
                    o.amount,
                    c.name
                FROM customers AS c
                LEFT JOIN orders1 AS o
                    ON o.customer_id = c.customer_id;
                
                CREATE LOCAL VIEW V2 AS SELECT
                    o.order_id,
                    o.amount,
                    c.first
                FROM orders2 AS o
                JOIN customers AS c
                    ON o.customer_id = c.customer_id;
                
                CREATE VIEW V AS (SELECT * FROM V1) UNION ALL (SELECT * FROM V2);""");
        // Validated on Postgres
        ccs.step("""
                INSERT INTO customers (customer_id, name, first) VALUES
                  (1, 'Johnson', 'Alice'),
                  (2, 'Smith',   'Bob'),
                  (3, 'White',   'Carol');
                INSERT INTO orders1 (order_id, customer_id, amount) VALUES
                  (101, 1, 120.50),   -- matches Alice
                  (102, 2,  75.00),   -- matches Bob
                  (103, 9,  33.33);   -- no matching customer
                INSERT INTO orders2 (order_id, customer_id, amount) VALUES
                  (201, 2,  88.00),   -- matches Bob
                  (202, 3, 150.00),   -- matches Carol
                  (203, 8,  42.42);   -- no matching customer
                """, """
                 order_id | amount | name    | weight
                ---------------------------------------
                 101      | 120.50 | Johnson|  1
                 102      | 75.00  | Smith|    1
                          |        | White|    1
                 201      | 88.00  | Bob|      1
                 202      | 150.00 | Carol|    1""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int mapIndexCount = 0;

            @Override
            public void postorder(DBSPMapIndexOperator unused) {
                this.mapIndexCount++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.mapIndexCount);
            }
        });
    }

    @Test
    public void issue5815c() {
        // Self-join
        var ccs = this.getCCS("""
                CREATE TABLE customers (
                    customer_id  INT NOT NULL PRIMARY KEY,
                    name         TEXT,
                    first        TEXT
                );
                
                CREATE VIEW V AS SELECT
                    c1.first,
                    c2.name as name1,
                    c1.name
                FROM customers AS c1
                JOIN customers AS c2
                    ON c1.customer_id = c2.customer_id;""");
        // Validated on Postgres
        ccs.step("""
                INSERT INTO customers (customer_id, name, first) VALUES
                  (1, 'Johnson', 'Alice'),
                  (2, 'Smith',   'Bob'),
                  (3, 'White',   'Carol');""", """
                 first | name1 | name    | weight
                ---------------------------------------
                 Alice| Johnson| Johnson|  1
                 Bob| Smith| Smith|        1
                 Carol| White| White|      1""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int mapIndexCount = 0;

            @Override
            public void postorder(DBSPMapIndexOperator unused) {
                this.mapIndexCount++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.mapIndexCount);
            }
        });
    }

    @Test
    public void issue5815d() {
        // three pairwise joins, validated on Postgres
        var ccs = this.getCCS("""
                CREATE TABLE A (
                    id INT,
                    value varchar,
                    a INT
                );
                
                CREATE TABLE B (
                    id INT,
                    value varchar,
                    b INT
                );
                
                CREATE TABLE C (
                    id INT,
                    value varchar,
                    C INT
                );
                
                CREATE VIEW V AS
                WITH
                    AB AS (
                        SELECT
                            A.id,
                            'A_B' AS type,
                            A.value AS l,
                            B.value AS r,
                            A.a
                        FROM A
                        JOIN B ON A.id = B.id
                    ),
                    BC AS (
                        SELECT
                            B.id,
                            'B_C' AS type,
                            B.value AS l,
                            C.value AS r,
                            B.b
                        FROM B
                        JOIN C ON B.id = C.id
                    ),
                    AC AS (
                        SELECT
                            A.id,
                            'A_C' AS type,
                            A.value AS l,
                            C.value AS r,
                            C.c
                        FROM A
                        JOIN C ON A.id = C.id
                    )
                SELECT * FROM AB
                UNION ALL
                SELECT * FROM BC
                UNION ALL
                SELECT * FROM AC;
                """);
        ccs.step("""
                INSERT INTO A VALUES
                  (1, 'A1', 0), (2, 'A2', 0), (3, 'A3', 0);
                
                INSERT INTO B VALUES
                  (1, 'B1', 1), (2, 'B2', 1), (4, 'B4', 1);
                
                INSERT INTO C VALUES
                  (1, 'C1', 2), (2, 'C2', 2), (5, 'C5', 2);""", """
                  id | type | l | r | a | weight
                 --------------------------------
                  1   | A_B| A1| B1| 0 | 1
                  2   | A_B| A2| B2| 0 | 1
                  1   | B_C| B1| C1| 1 | 1
                  2   | B_C| B2| C2| 1 | 1
                  1   | A_C| A1| C1| 2 | 1
                  2   | A_C| A2| C2| 2 | 1""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int mapIndex = 0;

            @Override
            public void postorder(DBSPFlatMapIndexOperator node) {
                this.mapIndex++;
            }

            @Override
            public void postorder(DBSPMapIndexOperator node) {
                this.mapIndex++;
            }

            @Override
            public void endVisit() {
                // Only one index for each input
                Assert.assertEquals(3, this.mapIndex);
            }
        });
    }

    @Test
    public void testSharedIndexGC() {
        // Example from issue5815 with LATENESS
        var ccs = this.getCCS("""
                CREATE TABLE orders1 (
                    order_id     INT NOT NULL PRIMARY KEY,
                    customer_id  INT,
                    amount       DECIMAL(10,2)
                );
                
                CREATE TABLE orders2 (
                    order_id     INT NOT NULL PRIMARY KEY,
                    customer_id  INT LATENESS 2,
                    amount       DECIMAL(10,2)
                );
                
                CREATE TABLE customers (
                    customer_id  INT NOT NULL PRIMARY KEY LATENESS 2,
                    name         TEXT,
                    first        TEXT
                );
                
                CREATE LOCAL VIEW V1 AS SELECT
                    o.order_id,
                    o.amount,
                    c.name
                FROM orders1 AS o
                LEFT JOIN customers AS c
                    ON o.customer_id = c.customer_id;
                
                CREATE LOCAL VIEW V2 AS SELECT
                    o.order_id,
                    o.amount,
                    c.first
                FROM orders2 AS o
                LEFT JOIN customers AS c
                    ON o.customer_id = c.customer_id;
                
                CREATE VIEW V AS (SELECT * FROM V1) UNION ALL (SELECT * FROM V2);""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int mapIndexCount = 0;

            @Override
            public void postorder(DBSPMapIndexOperator unused) {
                this.mapIndexCount++;
            }

            @Override
            public void endVisit() {
                // If sharing of map-index operators works, there are 3,
                // otherwise there are 4.  LATENESS prevents sharing
                Assert.assertEquals(4, this.mapIndexCount);
            }
        });
    }

    @Test
    public void issue5935() {
        var ccs = this.getCCS("""
                CREATE TABLE orders (
                    order_id              INT NOT NULL PRIMARY KEY,
                    order_date            DATE NOT NULL,
                    billing_customer_id   INT,
                    shipping_customer_id  INT
                );
                
                CREATE TABLE customers (
                    customer_id   INT NOT NULL PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL,
                    region        TINYINT NOT NULL
                );
                
                CREATE VIEW V AS SELECT
                    o.order_id,
                    o.order_date,
                    bc.customer_name AS billing_name,
                    bc.region        AS billing_region,
                    sc.customer_name AS shipping_name,
                    sc.region        AS shipping_region
                FROM orders AS o
                LEFT JOIN customers AS bc
                    ON o.billing_customer_id = bc.customer_id
                LEFT JOIN customers AS sc
                    ON o.shipping_customer_id = sc.customer_id
                ORDER BY o.order_id;""");
        // Validated on Postgres
        ccs.stepWeightOne("""
                INSERT INTO CUSTOMERS VALUES
                  (1, 'Alice',   0),
                  (2, 'Bob',     1),
                  (3, 'Carol',   2),
                  (4, 'Dave',    3);
                INSERT INTO orders (order_id, order_date, billing_customer_id, shipping_customer_id) VALUES
                  -- both billing + shipping
                  (101, DATE '2024-01-10', 1, 2),
                  -- billing only
                  (102, DATE '2024-01-11', 3, NULL),
                  -- shipping only
                  (103, DATE '2024-01-12', NULL, 4),
                  -- neither
                  (104, DATE '2024-01-13', NULL, NULL),
                  -- same customer for both roles
                  (105, DATE '2024-01-14', 2, 2);""", """
                 order_id | order_date | billing_name | billing_region | shipping_name | shipping_region
                ------------------------------------------------------------------------------------------
                  101     | 2024-01-10 | Alice|         0              | Bob|            1
                  102     | 2024-01-11 | Carol|         2              |NULL           |
                  103     | 2024-01-12 |NULL          |                | Dave|           3
                  104     | 2024-01-13 |NULL          |                |NULL           |
                  105     | 2024-01-14 | Bob|           1              | Bob|            1""");
    }
}
