package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.Utf8String;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

public class StaticCandidatesTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    static ImplementStatics pass() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.optimizer.compareRustStatics");
        return new ImplementStatics(compiler, true);
    }

    private static DBSPExpression replacement(ImplementStatics pass, DBSPLiteral literal) {
        return pass.apply(literal).to(DBSPCloneExpression.class).expression;
    }

    @Test
    public void poolStaticLiteralsAcrossRootsByValueAndSqlType() {
        var pass = pass();
        var first = replacement(pass, new DBSPStringLiteral("Aa"));
        var collision = replacement(pass, new DBSPStringLiteral("BB"));
        Assert.assertEquals(Utf8String.fromJava("Aa").hashCode(), Utf8String.fromJava("BB").hashCode());
        Assert.assertNotSame(first, collision);
        Assert.assertSame(collision, replacement(pass, new DBSPStringLiteral("BB")));
        Assert.assertSame(first, replacement(pass, new DBSPStringLiteral("Aa")));
        Assert.assertNotSame(first, replacement(pass, new DBSPStringLiteral("Aa", true)));

        var fixed = DBSPTypeString.create(CalciteObject.EMPTY, 2, true, false);
        var varying = DBSPTypeString.create(CalciteObject.EMPTY, 2, false, false);
        Assert.assertNotSame(replacement(pass, new DBSPStringLiteral(CalciteObject.EMPTY, fixed, Utf8String.fromJava("Aa"))),
                replacement(pass, new DBSPStringLiteral(CalciteObject.EMPTY, varying, Utf8String.fromJava("Aa"))));

        var ty = new DBSPTypeDecimal(CalciteObject.EMPTY, 12, 2, true);
        var n = replacement(pass, new DBSPDecimalLiteral(ty, new BigDecimal("1.0")));
        Assert.assertSame(n, replacement(pass, new DBSPDecimalLiteral(ty, new BigDecimal("1.0"))));
        Assert.assertNotSame(n, replacement(pass, new DBSPDecimalLiteral(ty, new BigDecimal("1.00"))));
        var nil = replacement(pass, new DBSPDecimalLiteral(ty, null));
        Assert.assertSame(nil, replacement(pass, new DBSPDecimalLiteral(ty, null)));
        Assert.assertNotSame(nil, replacement(pass, new DBSPStringLiteral(null, true)));
        Assert.assertEquals(9, pass.newDeclarations.size());

        var inline = new ImplementStatics(pass.compiler(), false);
        Assert.assertNotSame(replacement(inline, new DBSPStringLiteral("Aa")),
                replacement(inline, new DBSPStringLiteral("Aa")));
        Assert.assertTrue(inline.newDeclarations.isEmpty());
    }

    @Test
    public void rustFindsTheFirstMatchingStaticDeclaration() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustStatics"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var decimal = DBSPTypeDecimal.getDefault();
        var value = new DBSPDecimalLiteral(decimal, new BigDecimal("1.0"));
        var equal = new DBSPDecimalLiteral(decimal, new BigDecimal("1.0"));
        var scaled = new DBSPDecimalLiteral(decimal, new BigDecimal("1.00"));
        var wide = new DBSPDecimalLiteral(new DBSPTypeDecimal(CalciteObject.EMPTY, 30, 4, true), value.value);
        Assert.assertEquals(2, RustCircuitPass.findStaticLiteral(compiler, value, List.of(scaled, wide, equal, value)));
        Assert.assertEquals(-1, RustCircuitPass.findStaticLiteral(compiler, value, List.of(scaled, wide)));
    }

    @Test
    public void distinguishConstructedValuesFromCallsWithConstantArguments() {
        var parameter = b.i32().var();
        var call = b.call("f", b.lit(1));
        var constructed = new DBSPArrayExpression(b.lit(2));
        var computed = new DBSPArrayExpression(call);
        var captures = b.call("g", b.lambda(b.i32(), inner -> b.add(inner, parameter)));
        var closed = b.call("g", b.lambda(b.i32(), inner -> b.add(inner, b.lit(3))));
        var pass = pass();
        pass.preorder(b.tuple(call, constructed, computed, captures, closed).closure(parameter));
        Assert.assertTrue(pass.shouldBeStatic(call));
        Assert.assertTrue(pass.shouldBeStatic(constructed));
        Assert.assertFalse(pass.shouldBeStatic(computed));
        Assert.assertFalse(pass.shouldBeStatic(captures));
        Assert.assertTrue(pass.shouldBeStatic(closed));
    }

    @Test
    public void staticErrorsCountRepeatedUsesAcrossInitializers() {
        var origin = CalciteObject.create(new SourcePositionRange(new SourcePosition(2, 1), new SourcePosition(2, 2)));
        var value = new DBSPI32Literal(origin, b.i32(), 1);
        var behavior = DBSPHandleErrorExpression.RuntimeBehavior.PanicWithSource;
        var shared = new DBSPHandleErrorExpression(origin, 19, behavior, value, true);
        var silent = new DBSPHandleErrorExpression(origin, 19, behavior, value, false);
        var last = new DBSPHandleErrorExpression(origin, 19, behavior, value, true);
        var binding = b.let(shared, variable -> variable);
        var pass = pass().renumber;
        pass.index = 7;

        var tuple = b.tuple(binding, binding, shared, shared, silent, last);
        var rewritten = pass.renumber(tuple).to(DBSPTupleExpression.class);
        Assert.assertSame(rewritten.fields[0], rewritten.fields[1]);
        Assert.assertSame(rewritten.fields[2], rewritten.fields[3]);
        Assert.assertEquals(7, rewritten.fields[2].to(DBSPHandleErrorExpression.class).index);
        Assert.assertEquals(0, rewritten.fields[4].to(DBSPHandleErrorExpression.class).index);
        Assert.assertEquals(11, rewritten.fields[5].to(DBSPHandleErrorExpression.class).index);
        Assert.assertEquals(12, pass.renumber(shared).to(DBSPHandleErrorExpression.class).index);
        Assert.assertEquals(13, pass.index);
    }

    @Test
    public void deepSharedStaticInitializersKeepTheFirstErrorSlot() {
        var origin = CalciteObject.create(new SourcePositionRange(new SourcePosition(2, 1), new SourcePosition(2, 2)));
        var literal = new DBSPI32Literal(origin, b.i32(), 1);
        var shared = new DBSPHandleErrorExpression(origin, 0,
                DBSPHandleErrorExpression.RuntimeBehavior.PanicWithSource, literal, true);
        DBSPExpression sum = shared;
        for (int i = 0; i < 160; i++)
            sum = b.add(sum, literal);

        var pass = pass().renumber;
        pass.index = 7;
        var result = pass.renumber(b.tuple(sum, sum, shared)).to(DBSPTupleExpression.class);
        Assert.assertSame(result.fields[0], result.fields[1]);
        DBSPExpression first = result.fields[0];
        for (int i = 0; i < 160; i++)
            first = first.to(DBSPBinaryExpression.class).left;
        Assert.assertSame(first, result.fields[2]);
        Assert.assertEquals(7, first.to(DBSPHandleErrorExpression.class).index);
        Assert.assertEquals(10, pass.index);
    }

    @Test
    public void firstUntypedArrayKeepsItsSourcePosition() {
        var span = new SourcePositionRange(new SourcePosition(3, 2), new SourcePosition(3, 7));
        var origin = CalciteObject.create(span);
        var ty = new DBSPTypeArray(DBSPTypeAny.INSTANCE, false);
        var first = new DBSPArrayExpression(origin, ty, List.of());
        var second = new DBSPArrayExpression(ty, false);
        var closure = b.tuple(first, second).closure(b.i32().var());
        var error = Assert.assertThrows(CompilationError.class, () -> pass().apply(closure));
        Assert.assertSame(origin, error.calciteObject);
        Assert.assertEquals("Could not infer a type for array elements; please specify it using CAST(array AS X ARRAY)",
                error.getMessage());
    }
}
