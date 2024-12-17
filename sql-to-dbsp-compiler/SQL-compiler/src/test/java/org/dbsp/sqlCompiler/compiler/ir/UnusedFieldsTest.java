package org.dbsp.sqlCompiler.compiler.ir;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields.FieldMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields.FindUnusedFields;
import org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields.ParameterFieldRemap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields.RewriteFields;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.util.Maybe;
import org.junit.Assert;
import org.junit.Test;

public class UnusedFieldsTest {
    @Test
    public void testZSetString() {
        DBSPExpression none = new DBSPTypeVec(DBSPTypeString.varchar(false), true).none();
        Assert.assertEquals("(Vec<s>?)null", none.toString());
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        Assert.assertEquals("None::<Vec<String>>",
                ToRustInnerVisitor.toRustString(compiler, none, false));
        DBSPZSetExpression zset = new DBSPZSetExpression(none);
        Assert.assertEquals("zset!((Vec<s>?)null => 1,)", zset.toString());
        Assert.assertEquals("zset!(None::<Vec<String>> => 1)",
                ToRustInnerVisitor.toRustString(compiler, zset, false));
        DBSPTupleExpression tup = new DBSPTupleExpression(none);
        Assert.assertEquals("Tup1::new((Vec<s>?)null, )", tup.toString());
        Assert.assertEquals("Tup1::new(None::<Vec<String>>)",
                ToRustInnerVisitor.toRustString(compiler, tup, false));
        DBSPZSetExpression zset1 = new DBSPZSetExpression(tup);
        Assert.assertEquals("zset!(Tup1::new((Vec<s>?)null, ) => 1,)", zset1.toString());
        Assert.assertEquals("zset!(Tup1::new(None::<Vec<String>>) => 1)",
                ToRustInnerVisitor.toRustString(compiler, zset1, false));
    }

    @Test
    public void unusedFieldsTest() {
        DBSPTypeTuple tuple = new DBSPTypeTuple(
                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true),
                DBSPTypeBool.create(false),
                DBSPTypeBool.create(true),
                new DBSPTypeTuple(
                        DBSPTypeString.varchar(true),
                        DBSPTypeString.varchar(false)));
        DBSPVariablePath var = tuple.ref().var();
        DBSPExpression body = new DBSPTupleExpression(
                var.deref().field(0),
                var.deref().field(2),
                var.deref().field(3).field(0));
        DBSPClosureExpression closure = body.closure(var.asParameter());

        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        closure = closure.ensureTree(compiler).to(DBSPClosureExpression.class);
        CanonicalForm cf = new CanonicalForm(compiler);

        FindUnusedFields fu = new FindUnusedFields(compiler);
        fu.apply(closure);
        Assert.assertTrue(fu.foundUnusedFields());

        Pair<ParameterFieldRemap, RewriteFields> pair = fu.getFieldRemap();
        RewriteFields rw = pair.right;
        DBSPClosureExpression result = cf.apply(rw.apply(closure)).to(DBSPClosureExpression.class);
        Assert.assertEquals("(|p0: &Tup3<i32?, b?, Tup2<s?, s>>| Tup3::new(((*p0).0), ((*p0).1), (((*p0).2).0), ))",
                result.toString());

        FieldMap fm = pair.left.get(closure.parameters[0]);
        assert fm != null;
        DBSPClosureExpression projection = fm.getProjection();

        projection = cf.apply(projection).to(DBSPClosureExpression.class);
        Assert.assertEquals(
                "(|p0: &Tup4<i32?, b, b?, Tup2<s?, s>>| Tup3::new(((*p0).0), ((*p0).2), (((*p0).3).clone()), ))",
                projection.toString());

        DBSPClosureExpression compose = result.applyAfter(compiler, projection, Maybe.YES);
        compose = cf.apply(compose).to(DBSPClosureExpression.class);
        Assert.assertEquals(
                "(|p0: &Tup4<i32?, b, b?, Tup2<s?, s>>| Tup3::new(((*p0).0), ((*p0).2), ((((*p0).3).clone()).0), ))",
                compose.toString());
        // This last assertion does not exactly hold because of the extra clone of p0.3
        // Assert.assertTrue(compose.equivalent(closure));
    }
}
