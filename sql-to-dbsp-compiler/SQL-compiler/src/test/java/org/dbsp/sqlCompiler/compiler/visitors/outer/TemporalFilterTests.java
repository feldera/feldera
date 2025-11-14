package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.RewriteNow;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class TemporalFilterTests extends SqlIoTest {
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        // Avoid all circuit transformations
        options.ioOptions.inputCircuit = true;
        options.ioOptions.raw = true;
        return options;
    }

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(a INT, b DOUBLE, c BIGINT, ts TIMESTAMP);""");
    }

    List<RewriteNow.BooleanExpression> findFilters(DBSPFilterOperator operator) {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        RewriteNow in = new RewriteNow(compiler);
        return in.findTemporalFilters(operator, operator.getClosureFunction());
    }

    static class FindFilter extends CircuitVisitor {
        @Nullable
        DBSPFilterOperator filter = null;

        public FindFilter(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPFilterOperator operator) {
            Utilities.enforce(this.filter == null);
            this.filter = operator;
        }
    }

    DBSPFilterOperator extractFilter(DBSPCompiler compiler, DBSPCircuit circuit) {
        FindFilter filter = new FindFilter(compiler);
        filter.apply(circuit);
        return Objects.requireNonNull(filter.filter);
    }

    void test(String program, List<String> expectedFilters) {
        var cc = this.getCC(program);
        var canon = new CanonicalForm(cc.compiler).getCircuitRewriter(false);
        var newCircuit = canon.apply(cc.getCircuit());
        var filter = this.extractFilter(cc.compiler, newCircuit);
        var list = this.findFilters(filter);
        String expected = expectedFilters.toString();
        Assert.assertEquals(expected, list.toString());
    }

    @Test
    public void extractFilters() {
        final String param = "parameter=p0: &Tup4<i32?, d?, i64?, Timestamp?>";
        final String base = "TemporalFilter[" + param + ", noNow=((*p0).3), withNow=now(), opcode=>]";
        final String aComp = "NoNow[noNow=(((*p0).0) ?> 2)]";
        final String bComp = "NoNow[noNow=(((*p0).1) ?== 5.0)]";
        final String cComp = "NoNow[noNow=(((*p0).2) ?!= 3)]";

        String sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW();";
        this.test(sql, Linq.list(base));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() AND a > 2;";
        this.test(sql, Linq.list(base, aComp));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE a > 2 AND ts > NOW();";
        this.test(sql, Linq.list(aComp, base));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() AND a > 2 AND b = 5;";
        this.test(sql, Linq.list(base, aComp, bComp));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE a > 2 AND ts > NOW() AND b = 5;";
        this.test(sql, Linq.list(aComp, base, bComp));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE a > 2 AND ts > NOW() AND b = 5 AND c <> 3;";
        this.test(sql, Linq.list(aComp, base, bComp, cComp));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE a > 2 AND b = 5 AND ts > NOW();";
        this.test(sql, Linq.list(aComp, bComp, base));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() AND a IS NOT NULL;";
        this.test(sql, Linq.list(base, "NoNow[noNow=(! ((*p0).0).is_none())]"));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() AND a IS NOT NULL AND b IS NOT NULL;";
        final String nt = "NoNow[noNow=((! ((*p0).0).is_none()) && (! ((*p0).1).is_none()))]";
        this.test(sql, Linq.list(base, nt));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() AND a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL;";
        this.test(sql, Linq.list(base, "NoNow[noNow=(((! ((*p0).0).is_none()) && (! ((*p0).1).is_none())) && (! ((*p0).2).is_none()))]"));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND ts > NOW();";
        // same as above
        this.test(sql, Linq.list(base, "NoNow[noNow=(((! ((*p0).0).is_none()) && (! ((*p0).1).is_none())) && (! ((*p0).2).is_none()))]"));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE a IS NOT NULL AND ts > NOW() AND b IS NOT NULL AND c IS NOT NULL;";
        // same as above
        this.test(sql, Linq.list(base, "NoNow[noNow=(((! ((*p0).0).is_none()) && (! ((*p0).1).is_none())) && (! ((*p0).2).is_none()))]"));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() AND a IS NOT NULL AND b IS NOT NULL AND SIN(a) < COS(b);";
        this.test(sql, Linq.list(base, "NoNow[noNow=(sin_dN(((d?)((*p0).0))) ?<? cos_dN(((*p0).1)))]", nt));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() - INTERVAL 5 MINUTES AND TS < NOW() - INTERVAL 10 MINUTES";
        this.test(sql, Linq.list("TemporalFilter[" + param + 
                        ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(300000)), opcode=>]",
                "TemporalFilter[" + param + ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(600000)), opcode=<]"));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() - INTERVAL 5 MINUTES " +
                "AND TS < NOW() - INTERVAL 10 MINUTES " +
                "AND a > 2";
        this.test(sql, Linq.list("TemporalFilter[" + param + 
                        ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(300000)), opcode=>]",
                "TemporalFilter[" + param + 
                        ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(600000)), opcode=<]",
                aComp));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() - INTERVAL 5 MINUTES " +
                "AND TS < NOW() - INTERVAL 10 MINUTES " +
                "AND a > 2 AND b = 5";
        this.test(sql, Linq.list("TemporalFilter[" + param + 
                        ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(300000)), opcode=>]",
                "TemporalFilter[" + param + 
                        ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(600000)), opcode=<]",
                aComp, bComp));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() - INTERVAL 5 MINUTES " +
                "AND TS < NOW() - INTERVAL 10 MINUTES " +
                "AND a > 2 AND b = 5 AND a IS NOT NULL";  // Calcite removes last check
        this.test(sql, Linq.list("TemporalFilter[" + param + 
                        ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(300000)), opcode=>]",
                "TemporalFilter[" + param + 
                        ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(600000)), opcode=<]",
                aComp, bComp));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() - INTERVAL 5 MINUTES " +
                "AND TS < TIMESTAMP '2020-01-01 00:00:00' + CAST(a AS INTERVAL DAYS)";
        this.test(sql, Linq.list("TemporalFilter[" + param + 
                        ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(300000)), opcode=>]",
                "NoNow[noNow=(((*p0).3) ?<? (2020-01-01 00:00:00 +? ((ShortInterval?)((*p0).0))))]"));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() - INTERVAL 5 MINUTES " +
                "AND NOW() < TIMESTAMP '2020-01-01 00:00:00' + CAST(a AS INTERVAL DAYS)";
        this.test(sql, Linq.list("TemporalFilter[" + param +
                        ", noNow=((*p0).3), withNow=(now() - ShortInterval::new(300000)), opcode=>]",
                "TemporalFilter[" + param +
                        ", noNow=(2020-01-01 00:00:00 +? ((ShortInterval?)((*p0).0))), withNow=now(), opcode=>=]"));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE EXTRACT(DAY FROM ts) > EXTRACT(DAY FROM NOW());";
        this.test(sql, Linq.list("NonTemporalFilter[expression=(extract_day_TimestampN(((*p0).3)) ?> extract_day_Timestamp(now()))]"));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE ts > NOW() AND EXTRACT(DAY FROM ts) > EXTRACT(DAY FROM NOW());";
        this.test(sql, Linq.list(base, "NonTemporalFilter[expression=(extract_day_TimestampN(((*p0).3)) ?> extract_day_Timestamp(now()))]"));

        sql = "CREATE VIEW V AS SELECT * FROM T WHERE EXTRACT(DAY FROM ts) > EXTRACT(DAY FROM NOW()) AND ts > NOW();";
        this.test(sql, Linq.list("NonTemporalFilter[expression=(wrap_bool? ((extract_day_TimestampN(((*p0).3)) ?> extract_day_Timestamp(now())) ?&&? (((*p0).3) ?> now())))]"));
    }
}
