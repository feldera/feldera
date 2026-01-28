package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.HashString;
import org.dbsp.util.Utilities;
import org.junit.Test;

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
}
