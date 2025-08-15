package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Maybe;
import org.junit.Assert;
import org.junit.Test;

public class InliningTests {
    @Test
    public void testInlining() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPType i32 = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true);

        DBSPType twice = new DBSPTypeTuple(i32, i32);
        DBSPVariablePath x = twice.ref().var();
        // inner = |x: Tup2<i32, i32>| Tup3::new(f(x), x.0, x.1)
        DBSPClosureExpression inner = new DBSPTupleExpression(
                new DBSPApplyExpression("f", i32, x.deref().field(0)),
                x.deref().field(0),
                x.deref().field(1)).closure(x);

        DBSPVariablePath var = inner.getResultType().ref().var();
        // project = |x: Tup3<i32, i32, i32>| Tup2::new(x.0, x.2)
        DBSPClosureExpression project = new DBSPTupleExpression(
                var.deref().field(0),
                var.deref().field(2)).closure(var);

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
}
