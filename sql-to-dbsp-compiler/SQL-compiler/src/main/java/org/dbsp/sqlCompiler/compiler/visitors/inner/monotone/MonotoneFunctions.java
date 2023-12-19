package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.DeclarationValue;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.TranslateVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.Logger;

import javax.annotation.Nullable;

/**
 * Given a function (ClosureExpression) and a set of almost monotone columns
 * it computes which of the output columns are almost monotone.
 * It also synthesizes a new expression which computes
 * just the monotone values.
 *
 * <p>For example, let's consider the following expression:
 * |t: &Tuple2<d?, Timestamp>| Tuple2::new((Date)(t.1), (t.0))
 * Further, assume that only the second argument is monotone.
 * Then the corresponding translation of this expression is:
 * |p: &Tuple1<Timestamp>| Tuple1::new((Date)(p.0))
 *
 * <p>Notice how the expression was sliced to only compute on the
 * monotone fields, and to only produce the output fields which are
 * themselves monotone.
 */
public class MonotoneFunctions extends TranslateVisitor<MonotoneValue> {
    final ValueProjection inputProjection;

    /** Maps each declaration to its current value. */
    final DeclarationValue<MonotoneValue> variables;
    final ResolveReferences resolver;
    /** True iff the closure we analyze has a parameter of the form
     * (&k, &v).  Otherwise, the parameter is of the form &k. */
    final boolean pairOfReferences;
    /** Operator where the analyzed closure originates from.
     * Only used for debugging. */
    final DBSPOperator operator;

    public MonotoneFunctions(IErrorReporter reporter,
                             DBSPOperator operator,
                             ValueProjection inputProjection,
                             boolean pairOfReferences) {
        super(reporter);
        this.operator = operator;
        this.inputProjection = inputProjection;
        this.pairOfReferences = pairOfReferences;
        this.variables = new DeclarationValue<>();
        this.resolver = new ResolveReferences(reporter, false);
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        if (!this.context.isEmpty())
            // This means that we are analyzing a closure within another closure.
            throw new UnimplementedException(expression);

        // Outermost call.
        // This closure is expected to have a single parameter with a tuple type.
        assert expression.parameters.length == 1: "Expected a single parameter " + expression;
        DBSPParameter param = expression.parameters[0];

        DBSPType paramType = param.getType();
        DBSPType inputProjectionType = this.inputProjection.getType();
        assert inputProjectionType.ref().sameType(paramType) :
          "Expected same type " + inputProjectionType + " and " + paramType;

        DBSPType projectedType = this.inputProjection.getProjectedType();
        DBSPParameter projectedParameterRef = new DBSPParameter(param.getName(), projectedType.ref());
        DBSPParameter projectedParameter = new DBSPParameter(param.getName(), projectedType);
        MonotoneValue monotoneParam = this.inputProjection.createInput(projectedParameter.asVariable());
        this.variables.put(param, monotoneParam);
        this.push(expression);
        expression.body.accept(this);
        this.pop(expression);
        MonotoneValue bodyValue = this.maybeGet(expression.body);
        if (bodyValue != null) {
            DBSPClosureExpression closure = bodyValue.getExpression().closure(projectedParameterRef);
            MonotoneClosure result = new MonotoneClosure(closure, bodyValue);
            this.set(expression, result);
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("Monotone value for " + expression + " is " + result);
        }
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPVariablePath var) {
        IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(var);
        MonotoneValue value = this.variables.get(declaration);
        this.maybeSet(var, value);  // may overwrite
    }

    @Override
    public void postorder(DBSPFieldExpression expression) {
        MonotoneValue value = this.maybeGet(expression.expression);
        if (value == null)
            return;
        MonotoneTuple tuple = value.to(MonotoneTuple.class);
        value = tuple.field(expression.fieldNo);
        this.maybeSet(expression, value);
    }

    @Override
    public void postorder(DBSPBaseTupleExpression expression) {
        MonotoneTuple tuple = new MonotoneTuple(expression.getType().to(DBSPTypeTupleBase.class));
        int index = 0;
        for (DBSPExpression field: expression.fields) {
            MonotoneValue value = this.maybeGet(field);
            if (value != null)
                tuple.addField(index, value);
            index++;
        }
        if (tuple.isEmpty())
            return;
        this.set(expression, tuple);
    }

    @Override
    public void postorder(DBSPLiteral expression) {
        this.set(expression, new MonotoneConstant(expression));
    }

    @Override
    public void postorder(DBSPBlockExpression expression) {
        if (expression.lastExpression != null) {
            MonotoneValue value = this.maybeGet(expression.lastExpression);
            this.maybeSet(expression, value);
        }
    }

    @Override
    public void postorder(DBSPLetStatement statement) {
        if (statement.initializer == null)
            return;
        MonotoneValue value = this.maybeGet(statement.initializer);
        if (value == null)
            return;
        this.variables.put(statement, value);
    }

    @Override
    public void postorder(DBSPBinaryExpression expression) {
        MonotoneValue left = this.maybeGet(expression.left);
        MonotoneValue right = this.maybeGet(expression.right);
        if (left == null || right == null)
            return;
        if (expression.operation == DBSPOpcode.ADD ||
            expression.operation == DBSPOpcode.MAX) {
            MonotoneValue result = new MonotoneScalar(expression.replaceSources(
                    left.getExpression(), right.getExpression()));
            this.set(expression, result);
        }
        // Some expressions are monotone if some of their operands are constant
        if (expression.operation == DBSPOpcode.SUB) {
            if (right.is(MonotoneConstant.class)) {
                MonotoneScalar result = new MonotoneScalar(expression.replaceSources(
                        left.getExpression(), right.getExpression()));
                this.set(expression, result);
            }
        }
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        MonotoneValue source = this.maybeGet(expression.source);
        if (source == null)
            return;
        MonotoneValue result = new MonotoneScalar(expression.replaceSource(source.getExpression()));
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPUnaryExpression expression) {
        MonotoneValue source = this.maybeGet(expression.source);
        if (source == null)
            return;
        if (expression.operation == DBSPOpcode.UNARY_PLUS) {
            MonotoneValue result = new MonotoneScalar(expression.replaceSource(source.getExpression()));
            this.set(expression, result);
        }
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        // Monotone functions applied to monotone arguments.
        DBSPExpression[] arguments = new DBSPExpression[expression.arguments.length];
        int index = 0;
        for (DBSPExpression argument: expression.arguments) {
            MonotoneValue arg = this.maybeGet(argument);
            if (arg == null)
                return;
            arguments[index++] = arg.getExpression();
        }
        if (expression.function.is(DBSPPathExpression.class)) {
            DBSPPathExpression path = expression.function.to(DBSPPathExpression.class);
            String name = path.toString();
            if (name.startsWith("log10_") ||
                    name.startsWith("ln_") ||
                    name.startsWith("ceil_") ||
                    name.startsWith("sqrt_") ||
                    name.startsWith("round_") ||
                    name.startsWith("truncate_") ||
                    name.startsWith("floor_") ||
                    name.startsWith("sign_") ||
                    name.startsWith("numeric_inc") ||
                    name.startsWith("extract_year_") ||
                    name.startsWith("extract_epoch_") ||
                    name.startsWith("extract_hour_Time")
            ) {
                MonotoneValue result = new MonotoneScalar(expression.replaceArguments(arguments));
                this.set(expression, result);
            }

            if (name.startsWith("datediff_") || name.startsWith("timestamp_diff_")) {
                MonotoneValue arg1 = this.maybeGet(expression.arguments[1]);
                if (arg1 != null && arg1.is(MonotoneConstant.class)) {
                    MonotoneValue result = new MonotoneScalar(expression.replaceArguments(arguments));
                    this.set(expression, result);
                }
            }
        }
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        this.resolver.apply(node);
        return super.apply(node);
    }

    @Override
    public String toString() {
        return super.toString() + ": " + this.operator + " " + this.inputProjection;
    }
}
