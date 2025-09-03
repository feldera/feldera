package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TpchTest extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions result = super.testOptions();
        result.languageOptions.incrementalize = true;
        result.languageOptions.optimizationLevel = 2;
        result.languageOptions.ignoreOrderBy = true;
        return result;
    }

    String getQuery(int query) throws IOException {
        String tpch = TestUtil.readStringFromResourceFile("tpch.sql");
        // Convert all other views to local, which will cause them to be removed
        tpch = tpch.replace("create view q", "create local view q");
        tpch = tpch.replace("create local view q" + query + " ", "create view q" + query + " ");
        return tpch;
    }

    @Test
    public void compileTpch() throws IOException {
        String tpch = TestUtil.readStringFromResourceFile("tpch.sql");
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(tpch);
        CompilerCircuitStream ccs = this.getCCS(compiler);
        ccs.showErrors();
    }

    CompilerCircuit compileQuery(int query) throws IOException {
        String tpch = this.getQuery(query);
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(tpch);
        CompilerCircuit cc = new CompilerCircuit(compiler);
        cc.showErrors();
        return cc;
    }

    @Test
    public void compileTpch5() throws IOException {
        // Checks that optimizations remove unused fields from tuples
        var cc = this.compileQuery(5);
        InnerVisitor visitor = new InnerVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPTypeTuple type) {
                Assert.assertTrue(type.size() < 17);
            }
        };
        cc.visit(visitor.getCircuitVisitor(false));
    }

    @Test
    public void compileTpch16() throws IOException {
        var cc = this.compileQuery(16);
        cc.visit(new CircuitVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPJoinFilterMapOperator join) {
                Assert.fail();
            }
        });
    }

    @Test
    public void compileTpch19() throws IOException {
        var cc = this.compileQuery(19);
        cc.visit(new CircuitVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPJoinOperator operator) {
                Assert.assertNotEquals(0, operator.left().getOutputIndexedZSetType()
                        .keyType.to(DBSPTypeTupleBase.class).size());
            }

            @Override
            public void postorder(DBSPJoinFilterMapOperator join) {
                Assert.fail();
            }
        });
    }

    @Test
    public void compilerTpch21() throws IOException {
        var cc = this.compileQuery(21);
        cc.visit(new CircuitVisitor(cc.compiler) {
            void someKey(DBSPJoinBaseOperator operator) {
                // Check that the keys are not Tup0<>
                Assert.assertNotEquals(0, operator.left().getOutputIndexedZSetType()
                        .keyType.to(DBSPTypeTupleBase.class).size());
            }

            @Override
            public void postorder(DBSPJoinOperator operator) {
                this.someKey(operator);
            }

            @Override
            public void postorder(DBSPJoinIndexOperator operator) {
                this.someKey(operator);
            }

            @Override
            public void postorder(DBSPJoinFilterMapOperator operator) {
                this.someKey(operator);
            }
        });
    }

    @Test
    public void compilerTpch2() throws IOException {
        var cc = this.compileQuery(2);
        cc.visit(new CircuitVisitor(cc.compiler) {
            void someKey(DBSPJoinBaseOperator operator) {
                // Check that the keys are not Tup0<>
                Assert.assertNotEquals(0, operator.left().getOutputIndexedZSetType()
                        .keyType.to(DBSPTypeTupleBase.class).size());
            }

            @Override
            public void postorder(DBSPJoinOperator operator) {
                this.someKey(operator);
            }

            @Override
            public void postorder(DBSPJoinIndexOperator operator) {
                this.someKey(operator);
            }

            @Override
            public void postorder(DBSPJoinFilterMapOperator operator) {
                this.someKey(operator);
            }
        });
    }
}
