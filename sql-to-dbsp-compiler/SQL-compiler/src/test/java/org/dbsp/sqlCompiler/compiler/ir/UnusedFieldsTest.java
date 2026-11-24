package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
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
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPReturnExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
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
        var input = source.getOutput(0).getOutputRowType();
        var narrowed = b.tup(b.i32n(), b.i32n());
        var projection = b.closure(input, row -> b.tuple(b.field(row, 0), b.field(row, 2)));

        // |row| { row.0; Tup1::new(row.2) } becomes two maps:
        // |row| Tup2::new(row.0, row.2), then |pair| { pair.0; Tup1::new(pair.1) }.
        assertTrimmedMap(compiler, source, statementMap(input, 0, 2), projection, statementMap(narrowed, 0, 1));
        // With the reads reversed, the second map is |pair| { pair.1; Tup1::new(pair.0) }.
        assertTrimmedMap(compiler, source, statementMap(input, 2, 0), projection, statementMap(narrowed, 1, 0));
    }

    private DBSPClosureExpression statementMap(DBSPType input, int statementField, int resultField) {
        return b.closure(input, row -> new DBSPBlockExpression(
                List.of(b.field(row, statementField).toStatement()), b.tuple(b.field(row, resultField))));
    }

    private static void assertTrimmedMap(DBSPCompiler compiler, DBSPOperator source, DBSPClosureExpression map,
                                         DBSPClosureExpression projection, DBSPClosureExpression expectedMap) {
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        circuit.addOperator(source);
        circuit.addOperator(new DBSPMapOperator(CalciteEmptyRel.INSTANCE, map, source.getOutput(0)));
        var result = new RemoveUnusedFields(compiler, new AnalyzedSet<>()).apply(circuit);
        Assert.assertEquals(3, result.allOperators.size());
        var first = result.allOperators.get(1).to(DBSPMapOperator.class).getClosureFunction();
        var second = result.allOperators.get(2).to(DBSPMapOperator.class).getClosureFunction();
        Assert.assertTrue(first.toString(), projection.equivalent(first));
        Assert.assertTrue(second.toString(), expectedMap.equivalent(second));
    }

    @Test
    public void selectFromBlockResultKeepsStatementReads() {
        var type = b.tup(b.i32n(), b.i32n(), b.i32n());
        // |row| { row.0; *row }.2
        var selectField = b.closure(type, row -> {
            var statement = b.field(row, 0).toStatement();
            return new DBSPBlockExpression(List.of(statement), row.deref()).field(2);
        });
        var visitor = new FindUsedFields(new DBSPCompiler(new CompilerOptions()));
        Assert.assertEquals("Ref([X, _, X])", visitor.findUsedFields(selectField).getUse().toString());
        // |row| row.1
        var middleField = b.closure(type, row -> b.field(row, 1));
        Assert.assertEquals("Ref([_, X, _])", visitor.findUsedFields(middleField).getUse().toString());
    }

    @Test
    public void aggregateMapRetainsStatementReads() {
        var value = b.i32n();
        var rowType = b.tup(value, value, value);
        // The aggregate maps |row| { row.0; row.2 } and finishes with |accumulator| accumulator.
        var map = b.closure(rowType, row -> new DBSPBlockExpression(
                List.of(b.field(row, 0).toStatement()), b.field(row, 2)));
        var postProcess = b.lambda(value, accumulator -> accumulator);
        var aggregate = new LinearAggregate(CalciteObject.EMPTY, map, postProcess, value.none());
        var finder = new FindUsedFields(new DBSPCompiler(new CompilerOptions()));
        Assert.assertEquals("Ref([X, _, X])", finder.computeUsedFields(aggregate).useMap().getUse().toString());

        // Analyze |row| row.1 with the same finder.
        var nextMap = b.closure(rowType, row -> b.field(row, 1));
        var nextAggregate = new LinearAggregate(CalciteObject.EMPTY, nextMap, postProcess, value.none());
        Assert.assertEquals("Ref([_, X, _])", finder.computeUsedFields(nextAggregate).useMap().getUse().toString());
    }

    @Test
    public void selectingATupleFieldStillEvaluatesItsOtherFields() {
        var rowType = b.tup(b.i32n(), b.i32n(), b.i32n(), b.i32n());
        var finder = new FindUsedFields(new DBSPCompiler(new CompilerOptions()));
        // |row| { Tup2::new(row.0, row.1).1; row.2 }
        var select = b.closure(rowType, row -> {
            var tuple = b.tuple(b.field(row, 0), b.field(row, 1));
            var statement = tuple.field(CalciteObject.EMPTY, 1).toStatement();
            return new DBSPBlockExpression(List.of(statement), b.field(row, 2));
        });
        // The first closure shares the variable node. Its copy has a distinct node at each read.
        for (var map : List.of(select, select.deepCopy().to(DBSPClosureExpression.class)))
            Assert.assertEquals("Ref([X, X, X, _])", finder.findUsedFields(map).getUse().toString());
    }

    @Test
    public void returnStatementsKeepReadsEvenWithoutASymbolicResult() {
        var rowType = b.tup(b.i32n(), b.i32n(), b.i32n(), b.i32n());
        var compiler = new DBSPCompiler(new CompilerOptions());
        var finder = new FindUsedFields(compiler);
        // |row| { return row.0; row.2 }
        var earlyReturn = b.closure(rowType, row -> new DBSPBlockExpression(
                List.of(new DBSPReturnExpression(CalciteObject.EMPTY, b.field(row, 0)).toStatement()), b.field(row, 2)));
        Assert.assertEquals("Ref([X, _, X, _])", finder.findUsedFields(earlyReturn).getUse().toString());
    }

    @Test
    public void testingRowNullabilityInAStatementNeedsNoFields() {
        var rowType = b.tup(b.i32n(), b.i32n()).withMayBeNull(true);
        // |row| { row.is_none(); 7 }
        var map = b.closure(rowType, row -> new DBSPBlockExpression(
                List.of(new DBSPIsNullExpression(CalciteObject.EMPTY, row.deref()).toStatement()), b.lit(7)));
        var finder = new FindUsedFields(new DBSPCompiler(new CompilerOptions()));
        Assert.assertEquals("Ref([_, _])", finder.findUsedFields(map).getUse().toString());
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
