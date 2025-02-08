package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.junit.Test;

import java.io.IOException;

public class TpchTest extends BaseSQLTests {
    @Test
    public void compileTpch() throws IOException {
        String tpch = TestUtil.readStringFromResourceFile("tpch.sql");
        CompilerOptions options = this.testOptions(true, true);
        DBSPCompiler compiler = new DBSPCompiler(options);
        options.languageOptions.ignoreOrderBy = true;
        compiler.submitStatementsForCompilation(tpch);
        CompilerCircuitStream ccs = this.getCCS(compiler);
        ccs.showErrors();
    }

    @Test
    public void compileTpch5() throws IOException {
        // Checks that optimizations remove unused fields from tuples
        String tpch = TestUtil.readStringFromResourceFile("tpch.sql");
        tpch = tpch.replace("create view q", "create local view q");
        tpch = tpch.replace("create local view q5", "create view q5");
        CompilerOptions options = this.testOptions(true, true);
        DBSPCompiler compiler = new DBSPCompiler(options);
        options.languageOptions.ignoreOrderBy = true;
        compiler.submitStatementsForCompilation(tpch);
        CompilerCircuit cc = new CompilerCircuit(compiler);
        cc.showErrors();
        InnerVisitor visitor = new InnerVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPTypeTuple type) {
                assert type.size() < 17;
            }
        };
        cc.visit(visitor.getCircuitVisitor(false));
    }
}
