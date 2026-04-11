package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.util.HashString;
import org.dbsp.util.NullPrintStream;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.PrintStream;

public class Regression2Tests extends SqlIoTest {
    @Test
    public void issue5479() {
        this.getCCS("""
                CREATE TABLE tbl(mapp MAP<VARCHAR, INT>);
                
                CREATE MATERIALIZED VIEW v AS
                SELECT mapp FROM tbl MINUS
                SELECT MAP['a', 2] FROM tbl;""");
    }

    @Test
    public void issue5481() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(
                str VARCHAR,
                bin BINARY,
                uuidd UUID,
                arr VARCHAR ARRAY);
                
                CREATE MATERIALIZED VIEW v1 AS
                SELECT str FROM tbl
                EXCEPT ALL
                SELECT arr[1] FROM tbl;""", "Not yet implemented: EXCEPT/MINUS ALL");
    }

    @Test
    public void intersectAllTest() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT);
                CREATE VIEW V AS SELECT * FROM T INTERSECT ALL SELECT * FROM T;""", "Not yet implemented: INTERSECT ALL");
    }

    @Test
    public void issue5425() {
        this.statementsFailingInCompilation("""
                CREATE TABLE  tbl(
                arr VARCHAR ARRAY);
                
                CREATE MATERIALIZED VIEW v AS SELECT * FROM TABLE(
                HOP(TABLE tbl, DESCRIPTOR(arr[1]), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE));""",
                "The argument of DESCRIPTOR must be an identifier");
    }

    @Test
    public void issue5484() {
        this.getCCS("""
                CREATE TYPE user_def AS(i1 INT, v1 VARCHAR NULL);
                CREATE TABLE tbl(udt user_def);
                
                CREATE MATERIALIZED VIEW v AS
                SELECT udt FROM tbl
                INTERSECT
                SELECT (5, NULL) FROM tbl;""");
    }

    @Test
    public void issue5493() {
        this.getCCS("""
                CREATE TABLE tbl(mapp MAP<VARCHAR, INT>);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                SAFE_CAST(mapp AS MAP<VARCHAR, VARCHAR>) AS to_map
                FROM tbl;""");
    }

    @Test
    public void checkNowChainAggregateHash() {
        /* The circuit generated for a NOW operator is always the same:
           input -> map_index -> chain_aggregate.  This test ensures that the
           Merkle hash computation does not change for the chain_aggregate.
           This can happen, for example, when the Rust code generated for any
           of its sources changes.  This is problematic because this would require
           boostrapping such circuits from the NOW() source, which is never materialized --
           this would make the circuits impossible to restart after an upgrade of the compiler.
         */
        var ccs = this.getCCS("""
                CREATE TABLE T(x TIMESTAMP, y INT);
                CREATE VIEW V AS SELECT y FROM T WHERE x < NOW() - INTERVAL 10 MINUTES;""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            boolean found = false;

            @Override
            public void postorder(DBSPChainAggregateOperator operator) {
                this.found = true;
                HashString hash = OperatorHash.getHash(operator, true);
                Utilities.enforce(hash != null);
                Utilities.enforce(hash.toString()
                        .equals("1f3808fdce7cee3559c13945ba24280abf22b777542af1bac25aa7d57cf29892"));
            }

            @Override
            public void endVisit() {
                Utilities.enforce(this.found);
            }
        });
    }

    @Test
    public void unusedFieldTest() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.ioOptions.quiet = false;  // show warnings
        String sql = """
                CREATE TABLE T (
                    item string,
                    user_id bigint,
                    id bigint not null PRIMARY KEY,
                    item_id bigint,
                    company_id bigint,
                    updated_at timestamp,
                    type string,
                    status int
                );
                
                CREATE TABLE S (
                    origin_id bigint,
                    item string,
                    id bigint not null PRIMARY KEY,
                    item_id bigint,
                    company_id bigint,
                    project_id bigint,
                    updated_at timestamp
                );
                
                CREATE VIEW V (
                  company_id,
                  item_id,
                  item,
                  latest_status,
                  updated_at,
                  user_id,
                  row_num)
                AS with ED as (
                        SELECT
                            id,
                            company_id,
                            item_id,
                            item,
                            origin_id,
                            updated_at
                            FROM S
                            WHERE item in ('A','B','C','D','E','F')
                                AND origin_id IS NOT NULL
                    )
                select
                    coalesce(T.company_id, ED.company_id) as company_id,
                    coalesce(T.item_id, ED.item_id) as item_id,
                    coalesce(T.item, ED.item)    as item,
                    case when ED.item_id is not null then '0' else
                        case
                            when T.status = 0 then 'a'
                            when T.status = 1 then 'b'
                            when T.status = 2 then 'c'
                            when T.status = 3 then 'd'
                            when T.status = 4 then 'e'
                            when T.status = 5 then 'f'
                        end
                    end,
                    coalesce(T.updated_at, ED.updated_at) as updated_at,
                    T.user_id,
                    row_number() over(
                        partition by
                            coalesce(T.company_id, ED.company_id),
                            coalesce(T.item_id, ED.item_id),
                            coalesce(T.item, ED.item)
                        ORDER BY
                            coalesce(T.updated_at, ED.updated_at) desc,
                            coalesce(T.id, ED.id) desc
                    ) as row_num
                from T
                full outer join ED
                    on T.item_id = ED.item_id
                    and T.item = ED.item
                    and T.company_id = ED.company_id
                where (
                    T.item in ('A','B','C','D','E','F')
                    OR ED.item in ('A','B','C','D','E','F')
                )
                qualify row_num = 1;
                """;
        compiler.submitStatementsForCompilation(sql);
        PrintStream save = System.err;
        System.setErr(NullPrintStream.INSTANCE);
        var ccs = this.getCCS(compiler);
        System.setErr(save);
        String messages = ccs.compiler.messages.toString();
        Assert.assertTrue(messages.contains("of table 's' is unused"));
        Assert.assertTrue(messages.contains("of table 't' is unused"));
    }

    @Test
    public void testTwoWindows() {
        var ccs = this.getCCS("""
                CREATE TABLE T(id INT, x TIMESTAMP);
                CREATE VIEW V AS
                SELECT * FROM T WHERE x < NOW() - INTERVAL 10 SECONDS
                                AND x > NOW() - INTERVAL 20 SECONDS;""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int windows = 0;

            @Override
            public void postorder(DBSPWindowOperator window) {
                this.windows++;
            }

            @Override
            public void endVisit() {
                // Check that only one window was used to implement both conditions.
                Assert.assertEquals(1, this.windows);
            }
        });
    }

    @Test
    public void issue5520() {
        this.q("""
                SELECT EXP(10192);
                 e
                ---
                 Infinity""");
    }

    @Test
    public void issue5569() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(
                id INT,
                intt INT);
                
                CREATE MATERIALIZED VIEW v AS
                SELECT id, intt FROM tbl
                QUALIFY
                ROW_NUMBER() OVER (ORDER BY intt DESC) <= 2 OR
                RANK() OVER (ORDER BY intt DESC) <= 2 OR
                DENSE_RANK() OVER (ORDER BY intt DESC) <= 2;""",
                "Not yet implemented: Multiple RANK aggregates per window");
    }

    @Test
    public void issue5593() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(str VARCHAR);
                
                CREATE VIEW v AS SELECT
                str SIMILAR TO '(abc|def)%' AS str
                FROM tbl;""", "Function 'SIMILAR TO' not yet implemented");
    }

    @Test
    public void issue5637() {
        this.statementsFailingInCompilation("""
                CREATE MATERIALIZED VIEW v AS SELECT
                (DATE '2020-06-21', DATE '2020-06-21' + INTERVAL '1' YEAR) CONTAINS TIME '12:00:00' AS res;""",
                "Cannot apply 'CONTAINS' to arguments of type '<RECORDTYPE(DATE EXPR$0, DATE EXPR$1)> CONTAINS <TIME(0)>'");
    }

    @Test
    public void issue5677() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT) WITH ('connectors' = '[{
                    "format":{
                        "name":"avro",
                        "config": {
                            "update_format":"raw",
                            "registry_urls": ["http://localhost:18081/"]
                        }
                    },
                }]');
                """, """
                Compilation error: 'connectors' is not legal JSON: Unexpected character ('}' (code 125)): was expecting double-quote to start field name
                    8|    },
                    9|}]');
                      ^""");
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT) WITH ('connectors' = '[{
                    <blah>
                }]');
                """, """
                Compilation error: 'connectors' is not legal JSON: Unexpected character ('<' (code 60)): was expecting double-quote to start field name
                    2|    <blah>
                          ^
                    3|}]');""");
    }

    @Test
    public void testSet() {
        // Check that SET does not affect normal identifier resolution
        var ccs = this.getCCS("""
                SET T = 1;
                CREATE TABLE T(x INT);
                CREATE VIEW V AS SELECT * FROM t;""");
        ccs.step("INSERT INTO T VALUES (1)", """
                 x | weight
                ------------
                 1 | 1""");
        Assert.assertEquals(1, ccs.compiler.messages.messages.size());
        Assert.assertTrue(ccs.compiler.messages.getMessage(0).toString().contains(
                "warning: Unknown setting: Variable 't' does not control any known settings"));
    }

    @Test
    public void issue5541() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT);
                CREATE LOCAL VIEW V AS SELECT MIN(x) AS min, MAX(x) AS max FROM T;
                CREATE VIEW W AS SELECT min FROM V;""");
        final int[] aggregatesFound = {0};
        InnerVisitor noMax = new InnerVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPMinMax node) {
                Assert.assertNotEquals(DBSPMinMax.Aggregation.Max, node.aggregation);
                aggregatesFound[0]++;
            }
        };
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPStreamAggregateOperator aggregate) {
                noMax.apply(aggregate.getFunction());
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, aggregatesFound[0]);
            }
        });

        ccs.step("INSERT INTO T VALUES(1), (2);", """
                 min | weight
                --------------
                 1   | 1""");
    }

    @Test
    public void issue5541a() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT);
                CREATE LOCAL VIEW V AS SELECT ARRAY_AGG(x) AS a, MAX(x) AS max FROM T;
                CREATE VIEW W AS SELECT a FROM V;""");
        final int[] aggregatesFound = {0};
        InnerVisitor noMax = new InnerVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPMinMax node) {
                Utilities.enforce (node.aggregation != DBSPMinMax.Aggregation.Max);
                aggregatesFound[0]++;
            }

            @Override
            public void postorder(DBSPFold fold) {
                aggregatesFound[0]++;
            }
        };
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPStreamAggregateOperator aggregate) {
                noMax.apply(aggregate.getFunction());
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, aggregatesFound[0]);
            }
        });

        ccs.step("INSERT INTO T VALUES(1), (2);", """
                 a | weight
                --------------
                 { 1, 2 } | 1""");
    }

    @Test
    public void issue5541b() {
        var ccs = this.getCCS("""
                CREATE TABLE T(r ROW(x INT, y INT));
                CREATE LOCAL VIEW V AS SELECT MIN(r) AS min, MAX(r) AS max FROM T;
                CREATE VIEW W AS SELECT min FROM V;""");
        final int[] aggregatesFound = {0};
        InnerVisitor noMax = new InnerVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPMinMax node) {
                Utilities.enforce (node.aggregation != DBSPMinMax.Aggregation.Max);
                aggregatesFound[0]++;
            }

            @Override
            public void postorder(DBSPFold fold) {
                aggregatesFound[0]++;
            }
        };
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPStreamAggregateOperator aggregate) {
                noMax.apply(aggregate.getFunction());
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, aggregatesFound[0]);
            }
        });

        ccs.step("INSERT INTO T VALUES(ROW(ROW(1, 2))), (ROW(ROW(3, 4)));", """
                 min      | weight
                -------------------
                 { 1, 2 } | 1""");
    }

    @Test
    public void issue5541c() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(x INT, z INT, y INT);
                CREATE LOCAL VIEW V AS SELECT SUM(x) AS sum, AVG(z) AS max, y FROM T GROUP BY y;
                CREATE VIEW W AS SELECT sum, y FROM V;""");
        var ccs = this.getCCS(compiler);
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int aggregates;

            @Override
            public void postorder(DBSPAggregateLinearPostprocessOperator aggregate) {
                aggregates++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, aggregates);
            }
        });

        ccs.step("INSERT INTO T VALUES(1, 2, 2), (2, 3, 2), (3, 0, 1);", """
                 sum | y | weight
                --------------
                  3  | 2 | 1
                  3  | 1 | 1""");
        TestUtil.assertMessagesContain(compiler, "Column 'z' of table 't' is unused");
    }

    @Test
    public void issue2234() {
        var ccs = this.getCCS("""
                CREATE TABLE transaction_with_customer(id INT, amt INT64, cc_num VARCHAR, unix_time INT64);
                CREATE VIEW FEATURE AS
                SELECT
                    id,
                    SUM(amt) OVER window_60_minute AS trans500_60min
                FROM transaction_with_customer as t
                WINDOW
                  window_60_minute AS (PARTITION BY cc_num ORDER BY t.unix_time RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW);""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int aggregates = 0;

            @Override
            public void postorder(DBSPSimpleOperator operator) {
                if (operator.is(DBSPAggregateOperatorBase.class)) {
                    Assert.assertTrue(operator.is(DBSPPartitionedRollingAggregateOperator.class));
                    this.aggregates++;
                }
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.aggregates);
                super.endVisit();
            }
        });
    }

    @Test
    public void issue2998() {
        var cc = this.getCC("""
                CREATE TABLE T(x VARCHAR);
                CREATE VIEW V AS SELECT regexp_replace(x, '1', '2') FROM T;""");

        boolean[] found = new boolean[1];
        InnerVisitor hasRegexConstructor = new InnerVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPConstructorExpression expression) {
                if (expression.function.toString().equalsIgnoreCase("Regex::new")) {
                    found[0] = true;
                }
            }
        };

        cc.visit(new CircuitVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPDeclaration decl) {
                if (decl.item.is(DBSPStaticItem.class)) {
                    // Check whether there is a static declaration holding the regex constructor
                    hasRegexConstructor.apply(decl.item);
                }
            }
        });

        Assert.assertTrue(found[0]);
    }

    @Test
    public void testEmptyAggregate() {
        // After optimization an aggregate is completely removed
        String sql = """
                CREATE TABLE LINEITEM (
                        L_ORDERKEY    INTEGER NOT NULL,
                        L_PARTKEY     INTEGER NOT NULL,
                        L_SUPPKEY     INTEGER NOT NULL,
                        L_LINENUMBER  INTEGER NOT NULL,
                        L_QUANTITY    DECIMAL(15,2) NOT NULL,
                        L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                        L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                        L_TAX         DECIMAL(15,2) NOT NULL,
                        L_RETURNFLAG  CHAR(1) NOT NULL,
                        L_LINESTATUS  CHAR(1) NOT NULL,
                        L_SHIPDATE    DATE NOT NULL,
                        L_COMMITDATE  DATE NOT NULL,
                        L_RECEIPTDATE DATE NOT NULL,
                        L_SHIPINSTRUCT CHAR(25) NOT NULL,
                        L_SHIPMODE     CHAR(10) NOT NULL,
                        L_COMMENT      VARCHAR(44) NOT NULL
                );
                
                -- Orders
                CREATE TABLE ORDERS  (
                        O_ORDERKEY       INTEGER NOT NULL,
                        O_CUSTKEY        INTEGER NOT NULL,
                        O_ORDERSTATUS    CHAR(1) NOT NULL,
                        O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                        O_ORDERDATE      DATE NOT NULL,
                        O_ORDERPRIORITY  CHAR(15) NOT NULL,
                        O_CLERK          CHAR(15) NOT NULL,
                        O_SHIPPRIORITY   INTEGER NOT NULL,
                        O_COMMENT        VARCHAR(79) NOT NULL
                );
                
                -- Customer
                CREATE TABLE CUSTOMER (
                        C_CUSTKEY     INTEGER NOT NULL,
                        C_NAME        VARCHAR(25) NOT NULL,
                        C_ADDRESS     VARCHAR(40) NOT NULL,
                        C_NATIONKEY   INTEGER NOT NULL,
                        C_PHONE       CHAR(15) NOT NULL,
                        C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                        C_MKTSEGMENT  CHAR(10) NOT NULL,
                        C_COMMENT     VARCHAR(117) NOT NULL
                );
                
                -- Order Priority Checking
                create materialized view q4
                as select
                        o_orderpriority,
                        count(*) as order_count
                from
                        orders
                where
                        o_orderdate >= date '1993-07-01'
                        and o_orderdate < date '1993-07-01' + interval '3' month
                        and exists (
                                select
                                        *
                                from
                                        lineitem
                                where
                                        l_orderkey = o_orderkey
                                        and l_commitdate < l_receiptdate
                        )
                group by
                        o_orderpriority
                order by
                        o_orderpriority;
                """;
        this.getCCS(sql);
    }

    @Test
    public void testBetween() {
        this.qs("""
                SELECT 1 BETWEEN 2 AND 0;
                 r
                ---
                false
                (1 row)
                
                SELECT 1 BETWEEN SYMMETRIC 2 AND 0;
                 r
                ---
                true
                (1 row)
                
                SELECT 1 BETWEEN ASYMMETRIC 2 AND 0;
                 r
                ---
                false
                (1 row)""");
    }

    @Test
    public void testStdDevPop() {
        this.qs("""
                WITH T(x) as (VALUES(CAST(NULL AS DECIMAL(5, 2)))) SELECT STDDEV_POP(x) FROM T;
                 r
                ---
                NULL
                (1 row)""");
    }

    @Test
    public void issue1956() {
        var ccs = this.getCCS("""
                CREATE TABLE auctions (
                  id INT NOT NULL PRIMARY KEY,
                  seller INT,
                  item TEXT
                );
                
                CREATE TABLE bids (
                  id INT NOT NULL PRIMARY KEY,
                  buyer INT,
                  auction_id INT,
                  amount INT
                );
                
                CREATE VIEW V AS SELECT id, (SELECT array_agg(buyer) FROM (
                  SELECT buyer FROM bids WHERE auction_id = auctions.id
                  ORDER BY buyer LIMIT 10
                )) FROM auctions;""");
        // Output validated using postgres
        ccs.step("""
                INSERT INTO auctions (id, seller, item) VALUES
                  (1, 101, 'Vintage Camera'),
                  (2, 102, 'Mountain Bike'),
                  (3, 103, 'Gaming Laptop'),
                  (4, 101, 'Antique Vase'),
                  (5, 104, 'Smartphone');
                INSERT INTO bids (id, buyer, auction_id, amount) VALUES
                  (1, 201, 1, 120),
                  (2, 202, 1, 150),
                  (3, 203, 1, 180),
                
                  (4, 204, 2, 300),
                  (5, 205, 2, 350),
                
                  (6, 206, 3, 700),
                  (7, 207, 3, 720),
                  (8, 208, 3, 750),
                  (9, 209, 3, 760),
                
                  (10, 210, 4, 90),
                  (11, 211, 4, 110),
                
                  -- Auction 5 intentionally has no bids
                  (12, 212, 2, 360);  -- extra competing bid on auction 2""", """
                 id | arr                    | weight
                ----------------------------------------
                 1  | { 201, 202, 203 }      | 1
                 2  | { 204, 205, 212 }      | 1
                 3  | { 206, 207, 208, 209 } | 1
                 4  | { 210, 211 }           | 1
                 5  | NULL                   | 1""");
    }

    @Test @Ignore("https://github.com/feldera/feldera/issues/2555")
    public void issue2555() {
        this.getCC("""
                create table spreadsheet (
                    id int64 not null primary key,
                    cell text not null,
                    mentions int64 array\s
                ) with ('materialized' = 'true');
                
                create materialized view spreadsheet_view as
                select
                    s.id,
                    s.cell,
                    array(
                        select sp.cell
                            from unnest(s.mentions) as mention_id
                            join spreadsheet sp on sp.id = mention_id
                        ) as mentioned_cells
                from spreadsheet s;""");
    }

    @Test
    public void issue2555a() {
        this.getCCS("""
                create table a(
                    col1 text not null,
                    col2 text not null,
                    PRIMARY KEY (col1, col2)
                );
                
                create table b(
                    col1 text not null,
                    col2 text not null,
                    PRIMARY KEY (col1, col2)
                );
                
                DECLARE RECURSIVE VIEW foo(
                    out1 text not null,
                    out2 text not null
                );
                
                CREATE MATERIALIZED VIEW foo(out1, out2) as (
                    SELECT a.col1, a.col2
                    FROM a
                    WHERE NOT EXISTS (
                        SELECT true FROM b
                        WHERE a.col1 = b.col1
                        UNION
                        SELECT true FROM b
                        WHERE a.col2 = b.col2
                    )
                    UNION
                    SELECT foo.out2, a.col1
                    FROM a, foo
                    WHERE foo.out1 = a.col2
                );""");
    }

    @Test
    public void issue5821() {
        this.statementsFailingInCompilation("""
                CREATE TABLE shipments (
                  shipment_id BIGINT,
                  warehouse_id BIGINT,
                  shipped_at TIMESTAMP,
                  expected_at TIMESTAMP,
                  delivered_at TIMESTAMP,
                  shipping_mode VARCHAR
                );
                
                CREATE MATERIALIZED VIEW bm07_shipping_performance AS
                SELECT
                  warehouse_id,
                  MAX(DATEDIFF(delivered_at, shipped_at)) AS max_days_in_transit
                FROM shipments
                GROUP BY
                  warehouse_id,
                  TIMESTAMP_TRUNC(shipped_at, WEEK);
                """, "Invalid number of arguments to function 'DATEDIFF'.");
    }

    @Test
    public void issue5822() {
        this.statementsFailingInCompilation("""
                CREATE TABLE payments (
                  payment_id BIGINT,
                  customer_id BIGINT,
                  payment_time TIMESTAMP,
                  amount DECIMAL(12, 2),
                  payment_method VARCHAR
                );
                
                CREATE VIEW bm06_customer_payment_windows AS
                SELECT
                  customer_id,
                  payment_time,
                  amount,
                  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY payment_time) AS payment_number,
                  LAG(amount) OVER (PARTITION BY customer_id ORDER BY payment_time) AS previous_amount,
                  SUM(amount) OVER (
                    PARTITION BY customer_id
                    ORDER BY payment_time
                    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                  ) AS running_amount
                FROM payments""", "Not yet implemented: ROW_NUMBER only supported in a TopK pattern");
    }

    @Test
    public void rowsTest() {
        this.statementsFailingInCompilation("""
                CREATE TABLE purchase (
                    ts TIMESTAMP NOT NULL,
                    amount BIGINT,
                    value BIGINT LATENESS 5
                );
                
                CREATE MATERIALIZED VIEW rolling_sum AS
                SELECT ts,
                    SUM(value) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_sum
                    FROM purchase;""", "Not yet implemented: Window aggregates with ROWS");
    }

    @Test
    public void jsonTests() {
        this.getCCS("""
                CREATE TYPE c_t AS ("value" VARCHAR);
                CREATE TYPE b_t AS ("x" c_t, "y" c_t);
                CREATE TYPE a_t AS ("b" b_t);
                CREATE FUNCTION jsonstring_as_a_t(input VARCHAR) RETURNS a_t;
                
                CREATE TABLE src ("data" VARCHAR);
                
                CREATE VIEW v AS
                SELECT
                    jsonstring_as_a_t("data")."b"."x"."value" AS "x_val",
                    jsonstring_as_a_t("data")."b"."y"."value" AS "y_val"
                FROM src;""");
    }

    @Test
    public void issue5899a() {
        this.getCCS("""
                CREATE TABLE F(file_id INT, original_path VARCHAR, replacement VARCHAR);
                CREATE VIEW V AS SELECT file_id, original_path,
                OVERLAY(original_path PLACING replacement FROM 10 FOR 4) AS updated_path
                FROM F WHERE original_path IS NOT NULL AND replacement IS NOT NULL;""");
        boolean[] b = new boolean[] { false, true };
        for (var a1: b)
            for (var a2: b)
                for (var a3: b)
                    for (var a4: b) {
                        var e1 = a1 ? "x" : "y";
                        var e2 = a2 ? "x" : "y";
                        var e3 = a3 ? "1" : "NULL";
                        var e4 = a4 ? "1" : "NULL";
                        String query = "CREATE TABLE T(x VARCHAR NOT NULL, y VARCHAR, i INT NOT NULL);";
                        query += "CREATE VIEW V AS SELECT OVERLAY(" + e1 +
                                " PLACING " + e2 + " FROM " + e3 + " FOR " + e4 + ") FROM T;";
                        this.getCCS(query);
                    }
    }

    @Test
    public void issue5899b() {
        this.getCCS("""
                CREATE VIEW V AS
                SELECT COUNT(*)
                FROM (
                  VALUES
                    (CAST(ROW(ARRAY[MAP[1, 2, 2, 3], MAP[1, 3]]) AS ROW(b MAP<INT, INT> ARRAY))),
                    (CAST(ROW(ARRAY[MAP[2, 3], MAP[1, 3]]) AS ROW(b MAP<INT, INT> ARRAY))),
                    (CAST(ROW(ARRAY[MAP[2, 3, 1, 2], MAP[1, 3]]) AS ROW(b MAP<INT, INT> ARRAY)))
                ) AS t(a)
                GROUP BY a""");
    }

    @Test
    public void issue5899c() {
        var ccs = this.getCCS("""
                CREATE TABLE E(hire_date DATE);
                CREATE VIEW Q AS
                SELECT DATE_TRUNC(hire_date, QUARTER) FROM E;""");
        ccs.step("INSERT INTO E VALUES (DATE '2020-02-01') , (DATE '2020-05-01')", """
                 trunc | weight
                ----------------
                 2020-01-01 | 1
                 2020-04-01 | 1""");
    }

    @Test
    public void issue5946() {
        var ccs = this.getCCS("""
                CREATE TABLE test ("tt" DECIMAL(38,10));
                create view  ff as select tt, tt / 100.0 as tt2 from test;""");
        ccs.stepWeightOne("INSERT INTO test VALUES (36)", """
                 ff | ff2
                ----------
                 36 | 0.36""");
    }

    @Test
    public void issue5989() {
        this.getCCS("""
                CREATE TABLE ee (
                    ab VARCHAR(110) NOT NULL PRIMARY KEY,
                    b VARCHAR(70) NOT NULL,
                    c SMALLINT NOT NULL,
                    d BIGINT NOT NULL PRIMARY KEY LATENESS 67::BIGINT,
                    e VARCHAR(60) NOT NULL ,
                    f VARCHAR(60) NOT NULL ,
                    g DECIMAL(38, 10),
                    h DECIMAL(38, 10),
                    i INT,
                    j INT,
                    k BOOLEAN,
                    l TIMESTAMP
                ) WITH ('materialized' = 'true');""");
    }

    @Test
    public void issue5981() {
        this.qs("""
                SELECT TO_HEX(x'48656c6c6f');
                 r
                ---
                 48656c6c6f
                (1 row)
                
                SELECT TO_HEX(x'0ABC');
                 r
                ---
                 0abc
                (1 row)""");
    }
}
