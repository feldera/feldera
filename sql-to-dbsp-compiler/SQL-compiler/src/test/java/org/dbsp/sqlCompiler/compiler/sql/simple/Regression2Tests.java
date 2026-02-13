package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.util.HashString;
import org.dbsp.util.NullPrintStream;
import org.dbsp.util.Utilities;
import org.junit.Assert;
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
                 a | weight
                --------------
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
        this.getCC(sql);
    }
}
