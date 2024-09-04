package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Test;

import java.io.IOException;

public class TpchTest extends BaseSQLTests {
    @Test
    public void compileTpch() throws IOException {
        // Logger.INSTANCE.setLoggingLevel(DBSPCompiler.class, 2);
        // Logger.INSTANCE.setLoggingLevel(CalciteCompiler.class, 2);
        String tpch = TestUtil.readStringFromResourceFile("tpch.sql");
        // The following convert every view except 21 into a local view
        //tpch = tpch.replace("create view q", "create local view q");
        //tpch = tpch.replace("create local view q21", "create view q21");
        CompilerOptions options = this.testOptions(true, true);
        DBSPCompiler compiler = new DBSPCompiler(options);
        options.languageOptions.ignoreOrderBy = true;
        compiler.compileStatements(tpch);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.showErrors();
        this.addRustTestCase(ccs);
    }

    @Test
    public void testQ5Simple() {
        this.showFinal();
        String sql = """
                CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
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
                                        L_COMMENT      VARCHAR(44) NOT NULL);

                create view q5 as
                select
                    sum(l_extendedprice * (1 - l_discount)) as revenue
                from
                    lineitem
                """;
        this.compileRustTestCase(sql);
    }
}
