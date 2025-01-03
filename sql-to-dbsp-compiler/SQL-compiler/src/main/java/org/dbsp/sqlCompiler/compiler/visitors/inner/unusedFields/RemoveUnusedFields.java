package org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Projection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;

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

    public RemoveUnusedFields(DBSPCompiler compiler) {
        super(compiler, false);
        this.find = new FindUnusedFields(compiler);
    }

    DBSPMapIndexOperator getProjection(CalciteObject node, FieldMap fieldMap, OutputPort input) {
        DBSPType inputType = input.getOutputIndexedZSetType().getKVRefType();
        DBSPVariablePath var = inputType.var();
        List<DBSPExpression> resultFields = Linq.map(fieldMap.getUsedFields(),
                f -> var.field(1).deref().field(f).applyCloneIfNeeded());
        DBSPRawTupleExpression raw = new DBSPRawTupleExpression(
                DBSPTupleExpression.flatten(var.field(0).deref()),
                new DBSPTupleExpression(resultFields, false));
        DBSPClosureExpression projection = raw.closure(var);

        OutputPort source = this.mapped(input);
        DBSPTypeIndexedZSet ix = TypeCompiler.makeIndexedZSet(projection.getResultType().to(DBSPTypeRawTuple.class));
        DBSPMapIndexOperator map = new DBSPMapIndexOperator(node, projection, ix, source);
        this.addOperator(map);
        return map;
    }

    public DBSPClosureExpression findUnused(DBSPClosureExpression closure) {
        closure = closure.ensureTree(this.compiler)
                .to(DBSPClosureExpression.class);
        this.find.apply(closure);
        return closure;
    }

    boolean processJoin(DBSPJoinBaseOperator join) {
        DBSPClosureExpression joinFunction = join.getClosureFunction();
        joinFunction = this.findUnused(joinFunction);

        assert joinFunction.parameters.length == 3;
        DBSPParameter left = joinFunction.parameters[1];
        DBSPParameter right = joinFunction.parameters[2];

        var pair = this.find.getFieldRemap();
        ParameterFieldRemap remap = pair.left;
        FieldMap leftRemap = Objects.requireNonNull(remap.get(left));
        FieldMap rightRemap = Objects.requireNonNull(remap.get(right));
        if (!leftRemap.hasUnusedFields() && !rightRemap.hasUnusedFields())
            return false;

        DBSPSimpleOperator leftMap = getProjection(join.getNode(), leftRemap, join.left());
        DBSPSimpleOperator rightMap = getProjection(join.getNode(), rightRemap, join.right());

        RewriteFields rw = pair.right;
        // Parameter 0 is not used in the body of the function, leave it unchanged
        rw.changeToIdentity(joinFunction.parameters[0]);
        DBSPExpression newJoinFunction = rw.apply(joinFunction).to(DBSPExpression.class);
        DBSPSimpleOperator replacement =
                join.withFunctionAndInputs(newJoinFunction, leftMap.outputPort(), rightMap.outputPort());
        this.map(join, replacement);
        return true;
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
    public void postorder(DBSPMapOperator operator) {
        if (!operator.input().outputType().is(DBSPTypeZSet.class) ||
                // avoid infinite recursion
                operator.hasAnnotation(a -> a.is(Projection.class))) {
            super.postorder(operator);
            return;
        }

        DBSPClosureExpression closure = operator.getClosureFunction();
        assert closure.parameters.length == 1;
        closure = this.findUnused(closure);
        if (!this.find.foundUnusedFields()) {
            super.postorder(operator);
            return;
        }

        Pair<ParameterFieldRemap, RewriteFields> pair = this.find.getFieldRemap();
        FieldMap fm = pair.left.get(closure.parameters[0]);
        assert fm != null;

        DBSPClosureExpression compressed = pair.right.apply(closure).to(DBSPClosureExpression.class);
        OutputPort source = this.mapped(operator.input());
        DBSPClosureExpression projection = fm.getProjection();
        DBSPSimpleOperator adjust = new DBSPMapOperator(operator.getNode(), projection,
                new DBSPTypeZSet(projection.getResultType()), source).addAnnotation(new Projection());
        this.addOperator(adjust);

        DBSPSimpleOperator result = new DBSPMapOperator(
                operator.getNode(), compressed, operator.getOutputZSetType(), adjust.outputPort());
        this.map(operator, result);
    }
}
