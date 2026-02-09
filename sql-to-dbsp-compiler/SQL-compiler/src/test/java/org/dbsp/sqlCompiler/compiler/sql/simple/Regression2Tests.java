package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
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
}
