package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for {@link DecomposeExpensiveFilters} */
public class DecomposeExpensiveFiltersTests extends SqlIoTest {
    /** Counts the calls to a function in the whole circuit.
     * Matches by prefix. */
    static class CountCalls extends InnerVisitor {
        final String function;
        int count = 0;

        public CountCalls(DBSPCompiler compiler, String function) {
            super(compiler);
            this.function = function;
        }

        @Override
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
        }

        @Override
        public void postorder(DBSPApplyExpression node) {
            String name = node.getFunctionName();
            if (name != null && name.startsWith(this.function))
                this.count++;
        }
    }

    /** Collects non-source operators whose output contains a VARIANT field */
    static class VariantOutputs extends CircuitVisitor {
        final List<String> operators = new ArrayList<>();

        public VariantOutputs(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPSimpleOperator operator) {
            if (operator.is(DBSPSourceBaseOperator.class))
                return;
            if (operator.outputType.toString().contains("VARIANT"))
                this.operators.add(operator.getClass().getSimpleName() + " " + operator.getIdString());
        }
    }

    /** A view filtering on expensive computations over a VARIANT column,
     * compared against NOW() windows.  The filter must be decomposed so that
     * - the expensive function is evaluated once per distinct argument
     * - the VARIANT column does not flow past the map holding the
     *   hoisted computations. */
    @Test
    public void testDecomposition() {
        String sql = """
                CREATE TABLE data (
                  id VARCHAR NOT NULL PRIMARY KEY,
                  properties VARIANT,
                  sid VARCHAR
                );
                CREATE FUNCTION to_ts(d VARCHAR) RETURNS TIMESTAMP AS
                  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', d);
                CREATE VIEW segment AS
                SELECT id, sid FROM data u
                WHERE u.sid = 'x'
                  AND TO_TS(SAFE_CAST(u.properties['created'] AS VARCHAR))
                      BETWEEN NOW() - INTERVAL 93 DAYS AND NOW() - INTERVAL 1 DAYS
                  AND TO_TS(SAFE_CAST(u.properties['deleted'] AS VARCHAR))
                      >= NOW() - INTERVAL 1 MONTHS
                  AND SAFE_CAST(u.properties['opt'] AS VARCHAR) = 'true';""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler);

        CountCalls counter = new CountCalls(compiler, "to_ts");
        ccs.visit(counter.getCircuitVisitor(false));
        Assert.assertEquals(2, counter.count);

        VariantOutputs variants = new VariantOutputs(compiler);
        ccs.visit(variants);
        Assert.assertEquals(List.of(), variants.operators);
    }

    /** Operands containing nested closures (array lambdas) must be hoisted
     * whole, and the two structurally equal copies that BETWEEN creates must
     * share one hoisted column despite containing closures. */
    @Test
    public void testNestedClosure() {
        String sql = """
                CREATE TABLE data (
                  id VARCHAR NOT NULL PRIMARY KEY,
                  properties VARIANT,
                  sid VARCHAR
                );
                CREATE VIEW segment AS
                SELECT id, sid FROM data u
                WHERE u.sid = 'x'
                  AND ARRAY_EXISTS(CAST(u.properties['tags'] AS VARCHAR ARRAY), t -> t = 'pro')
                  AND CARDINALITY(TRANSFORM(CAST(u.properties['tags'] AS VARCHAR ARRAY), t -> UPPER(t)))
                      BETWEEN 1 AND 5;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler).withStringTrim();

        CountCalls exists = new CountCalls(compiler, "array_exists");
        ccs.visit(exists.getCircuitVisitor(false));
        Assert.assertEquals(1, exists.count);

        CountCalls transform = new CountCalls(compiler, "transform");
        ccs.visit(transform.getCircuitVisitor(false));
        Assert.assertEquals(1, transform.count);

        VariantOutputs variants = new VariantOutputs(compiler);
        ccs.visit(variants);
        Assert.assertEquals(List.of(), variants.operators);

        // Only 'a' passes: 'b' lacks the 'pro' tag, 'c' has the wrong sid,
        // 'd' has more than 5 tags, 'e' has no properties
        ccs.stepWeightOne("""
                        INSERT INTO data VALUES
                        ('a', PARSE_JSON('{"tags": ["pro", "basic"]}'), 'x'),
                        ('b', PARSE_JSON('{"tags": ["basic"]}'), 'x'),
                        ('c', PARSE_JSON('{"tags": ["pro"]}'), 'y'),
                        ('d', PARSE_JSON('{"tags": ["pro", "t1", "t2", "t3", "t4", "t5"]}'), 'x'),
                        ('e', NULL, 'x');""",
                """
                         id | sid
                        ----------
                         a  | x""");
    }
}
