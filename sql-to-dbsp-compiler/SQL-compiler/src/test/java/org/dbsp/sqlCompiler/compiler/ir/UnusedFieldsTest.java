package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FieldUseMap;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindUsedFields;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ParameterFieldUse;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.RemoveUnusedFields;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.RewriteFields;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.Maybe;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

public class UnusedFieldsTest {
    final ExpressionBuilder b = new ExpressionBuilder();

    @Test
    public void removeUnusedFieldsPreservesDiscardedReads() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE TABLE t(a INT, unused INT, b INT); CREATE VIEW v AS SELECT * FROM t;");
        var source = compiler.getFinalCircuit(true).getInput(new ProgramIdentifier("t", false)).asOperator();
        for (int discarded : List.of(0, 2)) {
            var row = source.getOutput(0).getOutputRowType().ref().var();
            var statement = row.deref().field(discarded).toStatement();
            var tail = new DBSPTupleExpression(row.deepCopy().deref().field(2 - discarded));
            var body = new DBSPBlockExpression(List.of(statement), tail);
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            circuit.addOperator(source);
            circuit.addOperator(new DBSPMapOperator(CalciteEmptyRel.INSTANCE, body.closure(row), source.getOutput(0)));

            compiler.options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.optimizer.compareRustFieldUses");
            var result = new RemoveUnusedFields(compiler, new AnalyzedSet<>()).apply(circuit);
            Assert.assertEquals(3, result.allOperators.size());
            var projection = result.allOperators.get(1).to(DBSPMapOperator.class)
                    .getClosureFunction().body.to(DBSPTupleExpression.class);
            Assert.assertEquals(2, projection.size());
            Assert.assertEquals(0, projection.get(0).to(DBSPFieldExpression.class).fieldNo);
            Assert.assertEquals(2, projection.get(1).to(DBSPFieldExpression.class).fieldNo);
            var reader = result.allOperators.get(2).to(DBSPMapOperator.class)
                    .getClosureFunction().body.to(DBSPBlockExpression.class);
            var read = reader.contents.get(0).to(DBSPExpressionStatement.class).expression;
            Assert.assertEquals(discarded / 2, read.to(DBSPFieldExpression.class).fieldNo);
            var returned = reader.lastExpression.to(DBSPTupleExpression.class).get(0);
            Assert.assertEquals(1 - discarded / 2, returned.to(DBSPFieldExpression.class).fieldNo);
        }
    }

    @Test
    public void selectFromBlockResultKeepsStatementReads() {
        var type = b.tup(b.i32n(), b.i32n(), b.i32n());
        var reader = b.closure(type, row -> {
            var statement = b.field(row, 0).toStatement();
            return new DBSPBlockExpression(List.of(statement), row.deref()).field(2);
        });
        var visitor = new FindUsedFields(new DBSPCompiler(new CompilerOptions()));
        Assert.assertEquals("Ref([X, _, X])", visitor.findUsedFields(reader).getUse().toString());
        var nextReader = b.closure(type, row -> b.field(row, 1));
        Assert.assertEquals("Ref([_, X, _])", visitor.findUsedFields(nextReader).getUse().toString());
    }

    @Test
    public void testZSetString() {
        DBSPExpression none = new DBSPTypeArray(DBSPTypeString.varchar(false), true).none();
        Assert.assertEquals("(Array<s>?)null", none.toString());
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        Assert.assertEquals("None::<Array<SqlString>>",
                ToRustInnerVisitor.toRustString(compiler, none, null, false));
        DBSPZSetExpression zset = new DBSPZSetExpression(none);
        Assert.assertEquals("zset!((Array<s>?)null => 1i64,)", zset.toString());
        Assert.assertEquals("zset!(None::<Array<SqlString>> => 1i64)",
                ToRustInnerVisitor.toRustString(compiler, zset, null, false));
        DBSPTupleExpression tup = new DBSPTupleExpression(none);
        Assert.assertEquals("Tup1::new((Array<s>?)null, )", tup.toString());
        Assert.assertEquals("Tup1::new(None::<Array<SqlString>>)",
                ToRustInnerVisitor.toRustString(compiler, tup, null, false));
        DBSPZSetExpression zset1 = new DBSPZSetExpression(tup);
        Assert.assertEquals("zset!(Tup1::new((Array<s>?)null, ) => 1i64,)", zset1.toString());
        Assert.assertEquals("zset!(Tup1::new(None::<Array<SqlString>>) => 1i64)",
                ToRustInnerVisitor.toRustString(compiler, zset1, null, false));
    }

    @Test
    public void testU128None() {
        DBSPType u128 = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.UINT128, true);
        DBSPExpression none = u128.none();
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        Assert.assertEquals("None::<u128>", ToRustInnerVisitor.toRustString(compiler, none, null, false));
    }

    @Test
    public void testReduce() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPTypeTuple tuple = b.tup(
                b.i32n(), b.bool(), b.booln(), b.tup(b.strn(), b.str()));

        DBSPClosureExpression closure0 = b.closure(tuple, t ->
                b.tuple(b.field(t, 1), b.field(t, 3).field(0)));
        DBSPClosureExpression closure1 = b.closure(tuple, t ->
                b.tuple(b.field(t, 0)));

        FindUsedFields fuf = new FindUsedFields(compiler);
        ParameterFieldUse fum = fuf.findUsedFields(closure0);
        FieldUseMap fieldMap0 = fum.getUse();
        Assert.assertEquals("Ref([_, X, _, [X, _]])", fieldMap0.toString());

        fum = fuf.findUsedFields(closure1);
        Assert.assertTrue(fum.hasUnusedFields(2));
        FieldUseMap fieldMap1 = fum.getUse();
        Assert.assertEquals("Ref([X, _, _, [_, _]])", fieldMap1.toString());

        FieldUseMap reduced = fieldMap0.reduce(fieldMap1);
        Assert.assertEquals("Ref([X, X, _, [X, _]])", reduced.toString());
    }

    @Test
    public void unusedFieldsTest() {
        DBSPTypeTuple tuple = b.tup(
                b.i32n(), b.bool(), b.booln(), b.tup(b.strn(), b.str()));
        DBSPClosureExpression closure = b.closure(tuple, t ->
                b.tuple(
                        b.field(t, 0),
                        b.field(t, 2),
                        b.field(t, 3).field(0).applyClone()));

        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        CanonicalForm cf = new CanonicalForm(compiler);

        FindUsedFields fu = new FindUsedFields(compiler);
        var used = fu.findUsedFields(closure);
        Assert.assertTrue(used.hasUnusedFields(2));

        RewriteFields rw = used.getFieldRewriter(compiler, 1);
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

    @Test
    public void fieldUseMapTests() {
        DBSPType i = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true);
        DBSPType simple = new DBSPTypeTuple(i, i, i);
        FieldUseMap fum = new FieldUseMap(simple, true);
        Assert.assertEquals("[X, X, X]", fum.toString());
        Assert.assertFalse(fum.hasUnusedFields());
        Assert.assertFalse(fum.hasUnusedFields(1));
        fum = new FieldUseMap(simple, false);
        Assert.assertEquals("[_, _, _]", fum.toString());
        Assert.assertTrue(fum.hasUnusedFields());
        Assert.assertTrue(fum.hasUnusedFields(1));
        Assert.assertTrue(fum.hasUnusedFields(2));
        fum.setUsed(1);
        Assert.assertEquals("[_, X, _]", fum.toString());
        Assert.assertTrue(fum.hasUnusedFields());
        Assert.assertTrue(fum.hasUnusedFields(1));
        fum = fum.project(1);
        Assert.assertEquals("[_, X, _]", fum.toString());

        DBSPType nested = new DBSPTypeTuple(i, simple);
        fum = new FieldUseMap(nested, true);
        Assert.assertEquals("[X, [X, X, X]]", fum.toString());
        Assert.assertFalse(fum.hasUnusedFields());
        Assert.assertFalse(fum.hasUnusedFields(1));
        fum = new FieldUseMap(nested, false);
        Assert.assertEquals("[_, [_, _, _]]", fum.toString());
        Assert.assertTrue(fum.hasUnusedFields());
        Assert.assertTrue(fum.hasUnusedFields(1));
        fum.setUsed(0);
        Assert.assertEquals("[X, [_, _, _]]", fum.toString());
        Assert.assertTrue(fum.hasUnusedFields());
        Assert.assertTrue(fum.hasUnusedFields(1));
        fum.field(1).setUsed(1);
        Assert.assertEquals("[X, [_, X, _]]", fum.toString());
        Assert.assertTrue(fum.hasUnusedFields());
        Assert.assertEquals("[X, [X, X, X]]", fum.project(1).toString());
        Assert.assertFalse(fum.hasUnusedFields(1));

        DBSPType deeper = new DBSPTypeTuple(i, nested);
        fum = new FieldUseMap(deeper, true);
        Assert.assertEquals("[X, [X, [X, X, X]]]", fum.toString());
        Assert.assertFalse(fum.hasUnusedFields());
        Assert.assertFalse(fum.hasUnusedFields(1));
        fum = new FieldUseMap(deeper, false);
        Assert.assertEquals("[_, [_, [_, _, _]]]", fum.toString());
        Assert.assertTrue(fum.hasUnusedFields());
        Assert.assertTrue(fum.hasUnusedFields(1));
        Assert.assertTrue(fum.hasUnusedFields(2));
        fum.setUsed(0);
        Assert.assertEquals("[X, [_, [_, _, _]]]", fum.toString());
        Assert.assertTrue(fum.hasUnusedFields());
        Assert.assertTrue(fum.hasUnusedFields(1));
        Assert.assertTrue(fum.hasUnusedFields(2));
        fum.field(1).field(1).setUsed(0);
        Assert.assertEquals("[X, [_, [X, _, _]]]", fum.toString());
        Assert.assertTrue(fum.hasUnusedFields());
        Assert.assertFalse(fum.hasUnusedFields(1));
        Assert.assertEquals("[X, [_, [X, X, X]]]", fum.project(2).toString());
        Assert.assertTrue(fum.hasUnusedFields(2));
        fum.field(1).setUsed(0);
        Assert.assertEquals("[X, [X, [X, _, _]]]", fum.toString());
        Assert.assertTrue(fum.hasUnusedFields());
        Assert.assertFalse(fum.hasUnusedFields(1));
        Assert.assertFalse(fum.hasUnusedFields(2));
    }
}
