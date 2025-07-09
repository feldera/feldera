package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.List;
import java.util.Objects;

/**
 * Analyze functions in operators and discover unused fields.
 * Rewrite such operators as a composition of a map followed by a version of the original operator.
 *
 * <p>An unused field is a field of an input parameter which does not affect the output of the function.
 * An example is f = |x| x.1.  Here x.0 is an unused fields.
 * Such functions are decomposed into two functions such that f = g(h),
 * where h is a projection which removes the unused field h = |x| x.1 in this case
 * and g is a compressed version of the function f, g = |x| x.0 in this case.
 * Most of this work is done by the {@link FindUnusedFields} visitor.
 */
public class RemoveUnusedFields extends CircuitCloneVisitor {
    public final FindUnusedFields find;
    final AnalyzedSet<DBSPExpression> functionsAnalyzed;

    public RemoveUnusedFields(DBSPCompiler compiler, AnalyzedSet<DBSPExpression> functionsAnalyzed) {
        super(compiler, false);
        this.find = new FindUnusedFields(compiler);
        this.functionsAnalyzed = functionsAnalyzed;
    }

    OutputPort getProjection(CalciteRelNode node, FieldUseMap fieldMap, OutputPort input) {
        OutputPort source = this.mapped(input);
        if (!fieldMap.hasUnusedFields())
            return source;

        DBSPType inputType = input.getOutputIndexedZSetType().getKVRefType();
        DBSPVariablePath var = inputType.var();
        List<DBSPExpression> resultFields = Linq.map(fieldMap.deref().getUsedFields(),
                f -> var.field(1).deref().field(f).applyCloneIfNeeded());
        DBSPRawTupleExpression raw = new DBSPRawTupleExpression(
                DBSPTupleExpression.flatten(var.field(0).deref()),
                new DBSPTupleExpression(resultFields, false));
        DBSPClosureExpression projection = raw.closure(var);

        DBSPMapIndexOperator map = new DBSPMapIndexOperator(node, projection, source);
        this.addOperator(map);
        return map.outputPort();
    }

    boolean done(DBSPExpression expression) {
        return this.functionsAnalyzed.done(expression);
    }

    boolean processJoin(DBSPJoinBaseOperator join) {
        DBSPClosureExpression joinFunction = join.getClosureFunction();
        if (this.done(joinFunction))
            return false;
        joinFunction = this.find.findUnusedFields(joinFunction);

        Utilities.enforce(joinFunction.parameters.length == 3);
        DBSPParameter left = joinFunction.parameters[1];
        DBSPParameter right = joinFunction.parameters[2];

        RewriteFields rw = this.find.getFieldRewriter(1);
        FieldUseMap leftRemap = rw.getUseMap(left);
        FieldUseMap rightRemap = rw.getUseMap(right);
        if (!leftRemap.hasUnusedFields(1) && !rightRemap.hasUnusedFields(1))
            return false;

        OutputPort leftMap = getProjection(join.getRelNode(), leftRemap, join.left());
        OutputPort rightMap = getProjection(join.getRelNode(), rightRemap, join.right());

        // Parameter 0 does not emit fields in the body of the function, leave it unchanged
        rw.parameterFullyUsed(joinFunction.parameters[0]);
        DBSPClosureExpression newJoinFunction = rw.rewriteClosure(joinFunction);
        DBSPSimpleOperator replacement =
                join.withFunctionAndInputs(newJoinFunction, leftMap, rightMap);
        this.map(join, replacement);
        return true;
    }

    @Override
    public void postorder(DBSPAsofJoinOperator join) {
        boolean done = false;
        DBSPClosureExpression joinFunction = join.getClosureFunction();
        if (!this.done(joinFunction)) {
            // Make up a new function which always uses the timestamp fields
            Utilities.enforce(joinFunction.parameters.length == 3);
            DBSPParameter left = joinFunction.parameters[1];
            DBSPParameter right = joinFunction.parameters[2];
            DBSPTupleExpression tuple = joinFunction.body.to(DBSPTupleExpression.class);
            Utilities.enforce(tuple.fields != null);
            DBSPExpression[] extra = new DBSPExpression[tuple.size() + 2];
            System.arraycopy(tuple.fields, 0, extra, 0, tuple.size());
            extra[tuple.size()] = left.asVariable().deref().field(join.leftTimestampIndex);
            extra[tuple.size() + 1] = right.asVariable().deref().field(join.rightTimestampIndex);
            DBSPClosureExpression fakeFunction = new DBSPTupleExpression(extra).closure(joinFunction.parameters);
            this.find.findUnusedFields(fakeFunction);
            RewriteFields rw = this.find.getFieldRewriter(1);
            FieldUseMap leftRemap = rw.getUseMap(left);
            FieldUseMap rightRemap = rw.getUseMap(right);
            if (leftRemap.hasUnusedFields(1) || rightRemap.hasUnusedFields(1)) {
                // Parameter 0 does not emit fields in the body of the function, leave it unchanged
                rw.parameterFullyUsed(joinFunction.parameters[0]);
                OutputPort leftMap = getProjection(join.getRelNode(), leftRemap, join.left());
                OutputPort rightMap = getProjection(join.getRelNode(), rightRemap, join.right());
                DBSPClosureExpression newJoinFunction = rw.rewriteClosure(joinFunction);
                int leftTimestampIndex = leftRemap.deref().getNewIndex(join.leftTimestampIndex);
                int rightTimestampIndex = rightRemap.deref().getNewIndex(join.rightTimestampIndex);
                DBSPSimpleOperator replacement =
                        new DBSPAsofJoinOperator(join.getRelNode(), join.getOutputZSetType(),
                                newJoinFunction, leftTimestampIndex, rightTimestampIndex,
                                join.comparator, join.isMultiset, join.isLeft, leftMap, rightMap);
                this.map(join, replacement);
                done = true;
            }
        }

        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPJoinOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        if (operator.hasAnnotation(a -> a.is(IsProjection.class))
                || !operator.getFunction().is(DBSPClosureExpression.class)) {
            // avoid infinite recursion
            super.postorder(operator);
            return;
        }

        DBSPClosureExpression closure = operator.getClosureFunction();
        if (this.done(closure)) {
            super.postorder(operator);
            return;
        }

        Utilities.enforce(closure.parameters.length == 1);
        closure = this.find.findUnusedFields(closure);
        int size = closure.getType().getToplevelFieldCount();

        if (operator.input().outputType().is(DBSPTypeZSet.class)) {
            if (!this.find.foundUnusedFields(1)) {
                super.postorder(operator);
                return;
            }
            RewriteFields rw = this.find.getFieldRewriter(1);
            FieldUseMap fm = rw.getUseMap(closure.parameters[0]);
            DBSPClosureExpression projection = Objects.requireNonNull(fm.getProjection(1));
            DBSPClosureExpression compressed = rw.rewriteClosure(closure);

            if (EquivalenceContext.equiv(closure, projection) ||
                    EquivalenceContext.equiv(closure, compressed)) {
                // This optimization achieves nothing
                super.postorder(operator);
                return;
            }
            OutputPort source = this.mapped(operator.input());
            DBSPSimpleOperator adjust = new DBSPMapOperator(operator.getRelNode(), projection, source)
                    .addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
            this.addOperator(adjust);

            DBSPSimpleOperator result = new DBSPMapOperator(
                    operator.getRelNode(), compressed, operator.getOutputZSetType(), adjust.outputPort());
            this.map(operator, result);
        } else {
            if (!this.find.foundUnusedFields(2)) {
                super.postorder(operator);
                return;
            }
            // closure = compressed \circ projection
            RewriteFields rw = this.find.getFieldRewriter(2);
            FieldUseMap fm = rw.getUseMap(closure.parameters[0]);
            DBSPClosureExpression compressed = rw.rewriteClosure(closure);
            if (EquivalenceContext.equiv(closure, compressed)) {
                // This optimization achieves nothing
                super.postorder(operator);
                return;
            }

            OutputPort source = this.mapped(operator.input());
            DBSPClosureExpression projection = Objects.requireNonNull(fm.getProjection(2));
            DBSPSimpleOperator adjust = new DBSPMapIndexOperator(operator.getRelNode(), projection, source)
                    .addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
            this.addOperator(adjust);
            DBSPSimpleOperator result = new DBSPMapOperator(
                    operator.getRelNode(), compressed, operator.getOutputZSetType(), adjust.outputPort());
            this.map(operator, result);
        }
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        // almost identical to the Map case
        if (operator.hasAnnotation(a -> a.is(IsProjection.class))) {
            // avoid infinite recursion
            super.postorder(operator);
            return;
        }

        DBSPClosureExpression closure = operator.getClosureFunction();
        Utilities.enforce(closure.parameters.length == 1);
        if (this.done(closure)) {
            super.postorder(operator);
            return;
        }

        closure = this.find.findUnusedFields(closure);
        int size = closure.getType().getToplevelFieldCount();

        if (operator.input().outputType().is(DBSPTypeZSet.class)) {
            if (!this.find.foundUnusedFields(1)) {
                super.postorder(operator);
                return;
            }
            RewriteFields rw = this.find.getFieldRewriter(1);
            FieldUseMap fm = rw.getUseMap(closure.parameters[0]);
            DBSPClosureExpression compressed = rw.rewriteClosure(closure);
            OutputPort source = this.mapped(operator.input());
            DBSPClosureExpression projection = Objects.requireNonNull(fm.getProjection(1));
            if (EquivalenceContext.equiv(compressed, closure)) {
                super.postorder(operator);
                return;
            }

            DBSPSimpleOperator adjust = new DBSPMapOperator(operator.getRelNode(), projection, source)
                    .addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
            this.addOperator(adjust);

            DBSPSimpleOperator result = new DBSPMapIndexOperator(
                    operator.getRelNode(), compressed, operator.getOutputIndexedZSetType(), adjust.outputPort());
            this.map(operator, result);
        } else {
            if (!this.find.foundUnusedFields(2)) {
                super.postorder(operator);
                return;
            }
            // closure = compressed \circ projection
            RewriteFields rw = this.find.getFieldRewriter(2);
            FieldUseMap fm = rw.getUseMap(closure.parameters[0]);
            DBSPClosureExpression projection = Objects.requireNonNull(fm.getProjection(2));
            if (EquivalenceContext.equiv(closure, projection) ||
                    EquivalenceContext.equiv(closure, projection)) {
                // This optimization achieves nothing
                super.postorder(operator);
                return;
            }

            DBSPClosureExpression compressed = rw.rewriteClosure(closure);
            OutputPort source = this.mapped(operator.input());
            DBSPSimpleOperator adjust = new DBSPMapIndexOperator(operator.getRelNode(), projection, source)
                    .addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
            this.addOperator(adjust);
            DBSPSimpleOperator result = new DBSPMapIndexOperator(
                    operator.getRelNode(), compressed, operator.getOutputIndexedZSetType(), adjust.outputPort());
            this.map(operator, result);
        }
    }

    @Override
    public Token startVisit(IDBSPOuterNode circuit) {
        // ToDot.dumper(compiler, "x.png", 2).apply(circuit.to(DBSPCircuit.class));
        return super.startVisit(circuit);
    }
}
