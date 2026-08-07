package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link ResolveReferences}. */
public class ResolveReferencesTests extends BaseSQLTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    ReferenceMap resolve(DBSPExpression expression, boolean allowFree) {
        ResolveReferences resolver = new ResolveReferences(this.testCompiler(), allowFree);
        resolver.apply(expression);
        return resolver.reference;
    }

    /** Uses of the closure parameter resolve to the parameter */
    @Test
    public void testParameterResolves() {
        DBSPVariablePath t = b.refVar(b.tup(b.i32(), b.i32()));
        DBSPClosureExpression closure = b.add(b.field(t, 0), b.field(t, 1)).closure(t);
        ReferenceMap references = this.resolve(closure, false);
        Assert.assertSame(closure.parameters[0], references.get(t));
    }

    /** Let expression */
    @Test
    public void testLetResolves() {
        DBSPVariablePath t = b.refVar(b.tup(b.i32(), b.i32()));
        DBSPVariablePath[] bound = new DBSPVariablePath[1];
        DBSPExpression let = b.let(b.call("f", b.field(t, 0)),
                x -> {
                    bound[0] = x;
                    return b.call("g", x);
                });
        ReferenceMap references = this.resolve(let.closure(t), false);
        Assert.assertSame(let, references.get(bound[0]));
    }

    @Test
    public void testLetBinderDistinctNode() {
        DBSPVariablePath t = b.refVar(b.tup(b.i32(), b.i32()));
        DBSPVariablePath binder = b.i32().var();
        DBSPVariablePath use = new DBSPVariablePath(binder.variable, b.i32());
        DBSPExpression let = new DBSPLetExpression(
                binder, b.call("f", b.field(t, 0)), b.call("g", use));
        DBSPClosureExpression closure = b.tuple(let).closure(t);

        ReferenceMap references = this.resolve(closure, false);
        Assert.assertSame(let, references.get(binder));
        Assert.assertSame(let, references.get(use));

        // ValueNumbering visits the binder node and enforces that it resolves
        new ValueNumbering(this.testCompiler()).apply(closure);
    }

    /** A let initializer resolves in the scope outside the let:
     * in let x = 1; let x = x + 1; x
     * the second initializer sees the first x, the consumer the second. */
    @Test
    public void testShadowing() {
        DBSPVariablePath outerBinder = b.i32().var();
        String name = outerBinder.variable;
        DBSPVariablePath innerBinder = new DBSPVariablePath(name, b.i32());
        DBSPVariablePath initializerUse = new DBSPVariablePath(name, b.i32());
        DBSPVariablePath consumerUse = new DBSPVariablePath(name, b.i32());
        DBSPLetExpression inner = new DBSPLetExpression(
                innerBinder, b.add(initializerUse, b.lit(1)), consumerUse);
        DBSPLetExpression outer = new DBSPLetExpression(outerBinder, b.lit(1), inner);

        ReferenceMap references = this.resolve(outer, false);
        Assert.assertSame(outer, references.get(initializerUse));
        Assert.assertSame(inner, references.get(consumerUse));
        Assert.assertSame(outer, references.get(outerBinder));
        Assert.assertSame(inner, references.get(innerBinder));
    }

    /** A lambda parameter shadows an outer variable with the same name */
    @Test
    public void testLambdaShadowing() {
        DBSPVariablePath outerParam = b.i32().var();
        String name = outerParam.variable;
        DBSPVariablePath outerUse = new DBSPVariablePath(name, b.i32());
        DBSPVariablePath innerParam = new DBSPVariablePath(name, b.i32());
        DBSPVariablePath innerUse = new DBSPVariablePath(name, b.i32());
        DBSPClosureExpression inner = b.call("h", innerUse).closure(innerParam);
        DBSPClosureExpression outer = b.call("m", outerUse, inner).closure(outerParam);

        ReferenceMap references = this.resolve(outer, false);
        Assert.assertSame(outer.parameters[0], references.get(outerUse));
        Assert.assertSame(inner.parameters[0], references.get(innerUse));
    }

    /** A variable declared by a let statement resolves to the statement */
    @Test
    public void testBlockStatement() {
        DBSPVariablePath t = b.refVar(b.tup(b.i32(), b.i32()));
        DBSPVariablePath[] bound = new DBSPVariablePath[1];
        DBSPExpression block = b.block(b.call("f", b.field(t, 0)),
                s -> {
                    bound[0] = s;
                    return b.add(s, b.lit(1));
                });
        ReferenceMap references = this.resolve(block.closure(t), false);
        DBSPLetStatement statement = block.to(DBSPBlockExpression.class)
                .contents.get(0).to(DBSPLetStatement.class);
        Assert.assertSame(statement, references.get(bound[0]));
    }

    /** A free variable is tolerated only when allowed */
    @Test
    public void testFreeVariable() {
        DBSPVariablePath free = b.i32().var();
        DBSPExpression expression = b.add(free, b.lit(1));

        // The only test that needs the resolver itself, for the flag
        ResolveReferences resolver = new ResolveReferences(this.testCompiler(), true);
        resolver.apply(expression);
        Assert.assertTrue(resolver.freeVariablesFound);
        Assert.assertNull(resolver.reference.get(free));

        Assert.assertThrows(InternalCompilerError.class,
                () -> this.resolve(expression, false));
    }
}
