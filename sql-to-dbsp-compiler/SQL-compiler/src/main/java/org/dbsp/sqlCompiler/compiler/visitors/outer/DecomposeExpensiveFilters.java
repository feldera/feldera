package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Expensive;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.ContainsNow;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;

import java.util.ArrayList;
import java.util.List;

/** Decomposes a filter whose predicate contains expensive computations and now() into
 * map -> filter -> map:
 * - the first map appends the results of the expensive computations to the input row
 * - the filter evaluates only cheap Boolean computations over the appended fields
 * - the last map restores the original row type.
 * Computations that involve now() are never hoisted. */
public class DecomposeExpensiveFilters extends CircuitCloneVisitor {
    final ContainsNow containsNow;

    public DecomposeExpensiveFilters(DBSPCompiler compiler) {
        super(compiler, false);
        this.containsNow = new ContainsNow(compiler, true);
    }

    static boolean isConnective(DBSPOpcode opcode) {
        return opcode == DBSPOpcode.AND || opcode == DBSPOpcode.OR;
    }

    static boolean isComparison(DBSPOpcode opcode) {
        return switch (opcode) {
            case EQ, NEQ, LT, GT, LTE, GTE, IS_DISTINCT -> true;
            default -> false;
        };
    }

    static boolean isBooleanUnary(DBSPOpcode opcode) {
        return switch (opcode) {
            case NOT, WRAP_BOOL, IS_TRUE, IS_FALSE, IS_NOT_TRUE, IS_NOT_FALSE -> true;
            default -> false;
        };
    }

    /** Expressions that are as cheap as a hoisted field reference */
    static boolean isTrivial(DBSPExpression expression) {
        while (expression.is(DBSPCloneExpression.class))
            expression = expression.to(DBSPCloneExpression.class).expression;
        if (expression.is(DBSPLiteral.class))
            return true;
        if (expression.is(DBSPFieldExpression.class))
            return expression.to(DBSPFieldExpression.class).expression.is(DBSPDerefExpression.class);
        return false;
    }

    /** Finds the maximal now-free subexpressions of a filter predicate worth
     * hoisting into a preceding map, then rewrites the predicate to reference
     * them as fields of a widened input tuple. */
    class Hoister {
        final DBSPParameter param;
        final List<DBSPExpression> hoisted = new ArrayList<>();

        Hoister(DBSPParameter param) {
            this.param = param;
        }

        /** Index of an equivalent computation present in this.hoisted, or -1. */
        int indexOf(DBSPExpression expression) {
            for (int i = 0; i < this.hoisted.size(); i++)
                if (EquivalenceContext.equiv(
                        this.hoisted.get(i).closure(this.param),
                        expression.closure(this.param)))
                    return i;
            return -1;
        }

        void collect(DBSPExpression expression) {
            if (expression.is(DBSPBinaryExpression.class)) {
                DBSPBinaryExpression bin = expression.to(DBSPBinaryExpression.class);
                if (isConnective(bin.opcode)) {
                    this.collect(bin.left);
                    this.collect(bin.right);
                    return;
                }
                if (isComparison(bin.opcode)) {
                    this.operand(bin.left);
                    this.operand(bin.right);
                    return;
                }
            } else if (expression.is(DBSPUnaryExpression.class)) {
                DBSPUnaryExpression unary = expression.to(DBSPUnaryExpression.class);
                if (isBooleanUnary(unary.opcode)) {
                    this.collect(unary.source);
                    return;
                }
            } else if (expression.is(DBSPIsNullExpression.class)) {
                this.operand(expression.to(DBSPIsNullExpression.class).expression);
                return;
            }
            this.operand(expression);
        }

        void operand(DBSPExpression expression) {
            DecomposeExpensiveFilters.this.containsNow.apply(expression);
            if (DecomposeExpensiveFilters.this.containsNow.found)
                return;
            if (isTrivial(expression))
                return;
            if (!Expensive.isExpensive(DecomposeExpensiveFilters.this.compiler(), expression))
                return;
            if (this.indexOf(expression) < 0)
                this.hoisted.add(expression);
        }

        DBSPExpression rewrite(DBSPExpression expression, DBSPVariablePath newVar, int base) {
            if (expression.is(DBSPBinaryExpression.class)) {
                DBSPBinaryExpression bin = expression.to(DBSPBinaryExpression.class);
                if (isConnective(bin.opcode)) {
                    return new DBSPBinaryExpression(bin.getNode(), bin.getType(), bin.opcode,
                            this.rewrite(bin.left, newVar, base),
                            this.rewrite(bin.right, newVar, base));
                }
                if (isComparison(bin.opcode)) {
                    return new DBSPBinaryExpression(bin.getNode(), bin.getType(), bin.opcode,
                            this.rewriteOperand(bin.left, newVar, base),
                            this.rewriteOperand(bin.right, newVar, base));
                }
            } else if (expression.is(DBSPUnaryExpression.class)) {
                DBSPUnaryExpression unary = expression.to(DBSPUnaryExpression.class);
                if (isBooleanUnary(unary.opcode)) {
                    return new DBSPUnaryExpression(unary.getNode(), unary.getType(), unary.opcode,
                            this.rewrite(unary.source, newVar, base));
                }
            } else if (expression.is(DBSPIsNullExpression.class)) {
                DBSPIsNullExpression isNull = expression.to(DBSPIsNullExpression.class);
                return new DBSPIsNullExpression(isNull.getNode(),
                        this.rewriteOperand(isNull.expression, newVar, base));
            }
            return this.rewriteOperand(expression, newVar, base);
        }

        /** Hoisted operands become field references; other operands keep
         * referencing the original parameter. */
        DBSPExpression rewriteOperand(DBSPExpression expression, DBSPVariablePath newVar, int base) {
            int index = this.indexOf(expression);
            if (index >= 0)
                return newVar.deref().field(base + index).applyCloneIfNeeded();
            return expression;
        }
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPClosureExpression function = operator.getClosureFunction();
        Simplify simplify = new Simplify(this.compiler());
        function = simplify.apply(function).to(DBSPClosureExpression.class);
        DBSPParameter param = function.parameters[0];
        Hoister hoister = new Hoister(param);
        hoister.collect(function.body);
        final boolean shouldHoist;
        if (hoister.hoisted.isEmpty()) {
            shouldHoist = false;
        } else if (hoister.hoisted.size() == 1) {
            // Heuristic: one expensive expression is hoisted only if the
            // filter may be a temporal filter.
            this.containsNow.apply(function);
            shouldHoist = this.containsNow.found();
        } else {
            shouldHoist = true;
        }
        if (!shouldHoist) {
            super.postorder(operator);
            return;
        }

        DBSPTypeTuple inputType = param.type.to(DBSPTypeRef.class).deref().to(DBSPTypeTuple.class);
        int n = inputType.size();
        int m = hoister.hoisted.size();

        // Map appending the hoisted computations to the input row
        DBSPExpression[] fields = new DBSPExpression[n + m];
        DBSPVariablePath t = param.asVariable();
        for (int i = 0; i < n; i++)
            fields[i] = t.deref().field(i).applyCloneIfNeeded();
        for (int j = 0; j < m; j++)
            fields[n + j] = hoister.hoisted.get(j);
        DBSPTupleExpression tuple = new DBSPTupleExpression(fields);
        DBSPClosureExpression mapFunction = tuple.closure(param);
        DBSPMapOperator map = new DBSPMapOperator(
                operator.getRelNode(), mapFunction, this.mapped(operator.input()));
        this.addOperator(map);

        // Filter with the cheap predicate over the widened tuple.
        DBSPVariablePath filterVar = tuple.getType().ref().var();
        DBSPExpression[] row = new DBSPExpression[n];
        for (int i = 0; i < n; i++)
            row[i] = filterVar.deref().field(i).applyCloneIfNeeded();
        DBSPExpression newBody = hoister.rewrite(function.body, filterVar, n)
                .closure(param)
                .call(new DBSPTupleExpression(operator.getNode(), inputType, row).borrow())
                .reduce(this.compiler());
        DBSPFilterOperator filter = new DBSPFilterOperator(
                operator.getRelNode(),
                newBody.wrapBoolIfNeeded().closure(filterVar), map.outputPort());
        this.addOperator(filter);

        // Projection restoring the original row type
        DBSPVariablePath projVar = tuple.getType().ref().var();
        DBSPExpression[] back = new DBSPExpression[n];
        for (int i = 0; i < n; i++)
            back[i] = projVar.deref().field(i).applyCloneIfNeeded();
        DBSPClosureExpression projFunction =
                new DBSPTupleExpression(operator.getNode(), inputType, back).closure(projVar);
        DBSPMapOperator projection = new DBSPMapOperator(
                operator.getRelNode(), projFunction, filter.outputPort())
                .addAnnotation(new IsProjection(n + m), DBSPMapOperator.class);
        this.map(operator, projection);
    }
}
