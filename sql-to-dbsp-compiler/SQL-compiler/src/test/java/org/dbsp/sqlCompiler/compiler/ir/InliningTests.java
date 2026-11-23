package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Maybe;
import org.junit.Assert;
import org.junit.Test;

public class InliningTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    @Test
    public void canonicalizeDeeplyShadowedVariables() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var row = b.tup(b.i32n());
        var source = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(row));
        circuit.addOperator(source);
        var input = new DBSPVariablePath("input", row.ref());
        var value = new DBSPVariablePath("value", b.i32n());
        DBSPExpression nested = value.deepCopy();
        for (int i = 0; i < 144; i++)
            nested = new DBSPLetExpression(new DBSPVariablePath("value", b.i32n()), value.deepCopy(), nested);
        nested = new DBSPLetExpression(new DBSPVariablePath("value", b.i32n()), input.deref().field(0), nested);
        var map = new DBSPMapOperator(site, b.tuple(nested).closure(input), source.outputPort());
        circuit.addOperator(map);
        var canonical = new CanonicalForm(compiler).getCircuitRewriter(false);
        var direct = canonical.apply(circuit).allOperators.get(1).to(DBSPMapOperator.class).getClosureFunction();
        Assert.assertEquals("p0", direct.parameters[0].name);
        var body = direct.body.to(DBSPTupleExpression.class).get(0);
        for (int i = 1; i <= 145; i++) {
            var binding = body.to(DBSPLetExpression.class);
            Assert.assertEquals("p" + i, binding.variable.variable);
            if (i > 1)
                Assert.assertEquals("p" + (i - 1), binding.initializer.to(DBSPVariablePath.class).variable);
            body = binding.consumer;
        }
        Assert.assertEquals("p145", body.to(DBSPVariablePath.class).variable);
        if (Boolean.getBoolean("feldera.optimizer.compareRustCanonicalForm")) {
            var pass = new RustCircuitPass(compiler, canonical, new OptimizerSnapshots(compiler));
            var hybrid = pass.apply(circuit).allOperators.get(1).to(DBSPMapOperator.class).getClosureFunction();
            Assert.assertEquals(direct.toString(), hybrid.toString());
        }
    }

    @Test
    public void testInlining() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        // inner = |x: &Tup2<i32, i32>| Tup3::new(f(x.0), x.0, x.1)
        DBSPClosureExpression inner = b.closure(b.tup(b.i32n(), b.i32n()), x ->
                b.tuple(
                        b.call(b.i32n(), "f", b.field(x, 0)),
                        b.field(x, 0),
                        b.field(x, 1)));

        // project = |v: &Tup3<i32, i32, i32>| Tup2::new(v.0, v.2)
        DBSPClosureExpression project = b.closure(
                inner.getResultType().to(DBSPTypeTuple.class), v ->
                        b.tuple(b.field(v, 0), b.field(v, 2)));

        DBSPClosureExpression compose = project.applyAfter(compiler, inner, Maybe.NO);
        CanonicalForm cf = new CanonicalForm(compiler);
        Assert.assertEquals("""
                (|p0: &Tup2<i32?, i32?>|
                {let p1 = &Tup3::new(f(((*p0).0)), ((*p0).0), ((*p0).1), );
                Tup2::new(((*p1).0), ((*p1).2), )})""", cf.apply(compose).toString());

        compose = project.applyAfter(compiler, inner, Maybe.YES);
        Assert.assertEquals("""
                (|p0: &Tup2<i32?, i32?>|
                Tup2::new(f(((*p0).0)), ((*p0).1), ))""", cf.apply(compose).toString());

        // Will inline because outer is a projection
        DBSPClosureExpression compose2 = project.applyAfter(compiler, inner, Maybe.MAYBE);
        EquivalenceContext context = new EquivalenceContext();
        Assert.assertTrue(context.equivalent(compose, compose2));
    }

    @Test
    public void testLambdaInlining() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        // lambda = |e: &i32| abs(e)
        DBSPClosureExpression lambda = b.closure(b.i32n(), e ->
                b.call(b.i32n(), "abs", e.deref()));

        // inner = |x: &Tup1<i32>| Tup1::new(hof(lambda, x.0))
        DBSPClosureExpression inner = b.closure(b.tup(b.i32n()), x ->
                b.tuple(b.call(b.i32n(), "hof", lambda, b.field(x, 0))));

        // project = |v: &Tup1<i32>| Tup2::new(v.0, v.0): uses the inner value twice,
        // so inlining copies the lambda into both use sites
        DBSPClosureExpression project = b.closure(
                inner.getResultType().to(DBSPTypeTuple.class), v ->
                        b.tuple(b.field(v, 0), b.field(v, 0)));
        DBSPClosureExpression compose = project.applyAfter(compiler, inner, Maybe.YES);

        // This would crash if the expression is malformed
        new CanonicalForm(compiler).apply(compose);
    }

    @Test
    public void testEnsureTreeWithLambda() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        // lambda = |e: &i32| abs(e)
        DBSPClosureExpression lambda = b.closure(b.i32n(), e ->
                b.call(b.i32n(), "abs", e.deref()));

        // The same lambda-bearing subtree appears twice: the body is a DAG
        DBSPClosureExpression function = b.closure(b.tup(b.i32n()), x -> {
            DBSPExpression shared = b.call(b.i32n(), "hof", lambda, b.field(x, 0));
            return b.tuple(shared, shared);
        });

        DBSPClosureExpression tree = function.ensureTree(compiler).to(DBSPClosureExpression.class);
        // The function's own parameters are preserved: analyses key results by them
        Assert.assertSame(function.parameters[0], tree.parameters[0]);
        // The two copies of the lambda must not share parameter objects;
        // CanonicalForm crashes if they do
        new CanonicalForm(compiler).apply(tree);
    }
}
