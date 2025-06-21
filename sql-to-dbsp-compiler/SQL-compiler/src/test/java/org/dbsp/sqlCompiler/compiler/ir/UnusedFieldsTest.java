package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FieldUseMap;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindUnusedFields;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.RewriteFields;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.Maybe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

public class UnusedFieldsTest {
    @Test
    public void testZSetString() {
        DBSPExpression none = new DBSPTypeArray(DBSPTypeString.varchar(false), true).none();
        Assert.assertEquals("(Array<s>?)null", none.toString());
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        Assert.assertEquals("None::<Array<SqlString>>",
                ToRustInnerVisitor.toRustString(compiler, none, false));
        DBSPZSetExpression zset = new DBSPZSetExpression(none);
        Assert.assertEquals("zset!((Array<s>?)null => 1,)", zset.toString());
        Assert.assertEquals("zset!(None::<Array<SqlString>> => 1)",
                ToRustInnerVisitor.toRustString(compiler, zset, false));
        DBSPTupleExpression tup = new DBSPTupleExpression(none);
        Assert.assertEquals("Tup1::new((Array<s>?)null, )", tup.toString());
        Assert.assertEquals("Tup1::new(None::<Array<SqlString>>)",
                ToRustInnerVisitor.toRustString(compiler, tup, false));
        DBSPZSetExpression zset1 = new DBSPZSetExpression(tup);
        Assert.assertEquals("zset!(Tup1::new((Array<s>?)null, ) => 1,)", zset1.toString());
        Assert.assertEquals("zset!(Tup1::new(None::<Array<SqlString>>) => 1)",
                ToRustInnerVisitor.toRustString(compiler, zset1, false));
    }

    @Test
    public void testReduce() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPTypeTuple tuple = new DBSPTypeTuple(
                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true),
                DBSPTypeBool.create(false),
                DBSPTypeBool.create(true),
                new DBSPTypeTuple(
                        DBSPTypeString.varchar(true),
                        DBSPTypeString.varchar(false)));

        DBSPVariablePath var0 = tuple.ref().var();
        DBSPExpression body0 = new DBSPTupleExpression(
                var0.deref().field(1),
                var0.deref().field(3).field(0));
        DBSPClosureExpression closure0 = body0.closure(var0.asParameter());

        DBSPVariablePath var1 = tuple.ref().var();
        DBSPExpression body1 = new DBSPTupleExpression(var1.deref().field(0));
        DBSPClosureExpression closure1 = body1.closure(var1.asParameter());

        FindUnusedFields fu = new FindUnusedFields(compiler);
        fu.findUnusedFields(closure0);
        Assert.assertTrue(fu.foundUnusedFields(2));
        DBSPParameter param0 = fu.parameterFieldMap.getParameters().iterator().next();
        FieldUseMap fieldMap0 = fu.parameterFieldMap.get(param0);
        Assert.assertEquals("Ref([_, X, _, [X, _]])", fieldMap0.toString());

        fu.findUnusedFields(closure1);
        Assert.assertTrue(fu.foundUnusedFields(2));
        DBSPParameter param1 = fu.parameterFieldMap.getParameters().iterator().next();
        FieldUseMap fieldMap1 = fu.parameterFieldMap.get(param1);
        Assert.assertEquals("Ref([X, _, _, [_, _]])", fieldMap1.toString());

        FieldUseMap reduced = fieldMap0.reduce(fieldMap1);
        Assert.assertEquals("Ref([X, X, _, [X, _]])", reduced.toString());
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
                var.deref().field(3).field(0).applyClone());
        DBSPClosureExpression closure = body.closure(var.asParameter());

        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        CanonicalForm cf = new CanonicalForm(compiler);

        FindUnusedFields fu = new FindUnusedFields(compiler);
        closure = fu.findUnusedFields(closure);
        Assert.assertTrue(fu.foundUnusedFields(2));

        RewriteFields rw = fu.getFieldRewriter(1);
        DBSPClosureExpression rewritten = rw.rewriteClosure(closure);
        DBSPClosureExpression result = cf.apply(rewritten).to(DBSPClosureExpression.class);
        Assert.assertEquals("""
                        (|p0: &Tup3<i32?, b?, Tup2<s?, s>>|
                        Tup3::new(((*p0).0), ((*p0).1), ((((*p0).2).0).clone()), ))""",
                result.toString());

        FieldUseMap fm = rw.getUseMap(closure.parameters[0]);
        DBSPClosureExpression projection = Objects.requireNonNull(fm.getProjection(1));

        projection = cf.apply(projection).to(DBSPClosureExpression.class);
        Assert.assertEquals("""
                        (|p0: &Tup4<i32?, b, b?, Tup2<s?, s>>|
                        Tup3::new(((*p0).0), ((*p0).2), (((*p0).3).clone()), ))""",
                projection.toString());

        DBSPClosureExpression compose = result.applyAfter(compiler, projection, Maybe.YES);
        compose = cf.apply(compose).to(DBSPClosureExpression.class);
        Assert.assertEquals("""
                        (|p0: &Tup4<i32?, b, b?, Tup2<s?, s>>|
                        Tup3::new(((*p0).0), ((*p0).2), ((((*p0).3).0).clone()), ))""",
                compose.toString());
        Assert.assertTrue(compose.equivalent(closure));
    }
}
