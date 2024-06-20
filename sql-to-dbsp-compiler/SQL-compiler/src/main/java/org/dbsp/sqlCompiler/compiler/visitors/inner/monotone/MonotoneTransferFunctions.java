package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.DeclarationValue;
import org.dbsp.sqlCompiler.compiler.visitors.inner.RepeatedExpressions;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.IsNumericLiteral;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Given a function (ClosureExpression) and the
 * monotonicity information of its parameters, this visitor
 * computes a MonotoneExpression corresponding to the function. */
public class MonotoneTransferFunctions extends TranslateVisitor<MonotoneExpression> {
    IMaybeMonotoneType[] parameterTypes;
    /** Maps each declaration to its current value. */
    final DeclarationValue<MonotoneExpression> variables;
    final ResolveReferences resolver;
    /** True iff the closure we analyze has a parameter of the form
     * (&k, &v).  Otherwise, each parameter is of the form &k. */
    final boolean indexedSet;
    /** Operator where the analyzed closure originates from.
     * Only used for debugging. */
    final DBSPOperator operator;

    /** Create a visitor to analyze the monotonicity of a closure
     * @param reporter      Error reporter.
     * @param operator      Operator whose function is analyzed.
     * @param parameterTypes Monotonicity information for the function parameters.
     *                      This information comes from the operator that is
     *                      a source for this function.
     * @param indexedSet    True if the operator's input data is an IndexedZSet.
     *                      In this case the parameter is a two-tuple. */
    public MonotoneTransferFunctions(IErrorReporter reporter,
                                     DBSPOperator operator,
                                     boolean indexedSet,
                                     IMaybeMonotoneType... parameterTypes) {
        super(reporter);
        this.operator = operator;
        this.parameterTypes = parameterTypes;
        for (IMaybeMonotoneType p: parameterTypes)
            assert !p.is(MonotoneClosureType.class);
        this.indexedSet = indexedSet;
        this.variables = new DeclarationValue<>();
        this.resolver = new ResolveReferences(reporter, false);
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        if (!this.context.isEmpty())
            // This means that we are analyzing a closure within another closure.
            throw new UnimplementedException(expression);

        // Must be the outermost call of the visitor.
        DBSPType[] projectedTypes = Linq.map(
                Objects.requireNonNull(this.parameterTypes), IMaybeMonotoneType::getProjectedType, DBSPType.class);

        DBSPParameter[] projectedParameters;
        if (this.indexedSet) {
            // Functions that iterate over IndexedZSets have the signature: |t: (&key, &value)| body.
            assert this.parameterTypes.length == 1;
            PartiallyMonotoneTuple tuple = this.parameterTypes[0].to(PartiallyMonotoneTuple.class);
            List<IMaybeMonotoneType> fields = Linq.map(tuple.fields, MonotoneRefType::new);
            this.parameterTypes = new IMaybeMonotoneType[] { new PartiallyMonotoneTuple(fields, tuple.raw) };
            DBSPType paramType;
            DBSPType[] tupleFields = Linq.map(projectedTypes[0].to(DBSPTypeTupleBase.class).tupFields,
                    DBSPType::ref, DBSPType.class);
            if (tuple.raw) {
                paramType = new DBSPTypeRawTuple(tupleFields);
            } else {
                paramType = new DBSPTypeTuple(tupleFields);
            }
            projectedParameters = new DBSPParameter[] {
                    new DBSPParameter(expression.parameters[0].getName(), paramType)
            };
        } else {
            // Functions with one parameter that iterate over ZSets have the
            // signature |t: &value| body.
            this.parameterTypes = Linq.map(this.parameterTypes, MonotoneRefType::new, IMaybeMonotoneType.class);
            projectedParameters = new DBSPParameter[projectedTypes.length];
            for (int i = 0; i < projectedTypes.length; i++)
                projectedParameters[i] = new DBSPParameter(
                        expression.parameters[i].getName(), projectedTypes[i].ref());
        }

        for (int i = 0; i < projectedTypes.length; i++) {
            MonotoneExpression parameterValue =
                    new MonotoneExpression(expression.parameters[i].asVariable(),
                            this.parameterTypes[i],
                            projectedParameters[i].asVariable());
            this.variables.put(expression.parameters[i], parameterValue);
        }
        this.push(expression);
        // Check that the expression is a pure tree; this is required by the dataflow analysis,
        // which represents monotonicity information as a key-value map indexed by expressions.
        new RepeatedExpressions(this.errorReporter, true).apply(expression.body);
        expression.body.accept(this);
        this.pop(expression);
        MonotoneExpression bodyValue = this.get(expression.body);
        if (bodyValue.mayBeMonotone()) {
            // Synthesize function for apply operator which will
            // compute the associated monotone value.
            DBSPParameter[] applyParameters;
            DBSPExpression applyBody = bodyValue.getReducedExpression();
            if (this.indexedSet) {
                // The "apply" DBSP node that computes the limit values does not have the type (&K, &V),
                // but &(K, V).  So we generate this code:
                // |a: &(K, V)| -> R {
                //    let t = (&(*a).0, &(*a).1);  // t has type (&K, &V), as expected by the previous body
                //    <previous body using t>
                // }
                PartiallyMonotoneTuple tuple = this.parameterTypes[0].to(PartiallyMonotoneTuple.class);
                DBSPParameter applyParameter = new DBSPParameter("a", projectedTypes[0].ref());
                List<DBSPExpression> parameterFieldsToKeep = new ArrayList<>();
                int index = 0;
                // However, not all fields of the "a" parameter may be monotone, so we only
                // include the ones that are.
                if (tuple.getField(0).mayBeMonotone()) {
                    parameterFieldsToKeep.add(applyParameter.asVariable().deref().field(index).borrow());
                    index++;
                }
                if (tuple.getField(1).mayBeMonotone()) {
                    parameterFieldsToKeep.add(applyParameter.asVariable().deref().field(index).borrow());
                }
                assert !parameterFieldsToKeep.isEmpty();
                applyBody = new DBSPBlockExpression(
                        Linq.list(
                                new DBSPLetStatement(expression.parameters[0].getName(),
                                        new DBSPRawTupleExpression(parameterFieldsToKeep))),
                        applyBody
                );
                applyParameters = new DBSPParameter[] { applyParameter };
            } else {
                applyParameters = projectedParameters;
            }
            DBSPClosureExpression closure = applyBody.closure(applyParameters);
            MonotoneClosureType cloType = new MonotoneClosureType(bodyValue.type,
                    expression.parameters,
                    applyParameters);
            MonotoneExpression result = new MonotoneExpression(expression, cloType, closure);
            this.set(expression, result);
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("MonotoneExpression for " + expression + " is " + result)
                    .newline();
        }
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPVariablePath var) {
        IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(var);
        MonotoneExpression value = this.variables.get(declaration);
        this.maybeSet(var, value);
    }

    @Override
    public void postorder(DBSPFieldExpression expression) {
        // t.0, where t may have monotone fields.
        MonotoneExpression value = this.get(expression.expression);
        PartiallyMonotoneTuple tuple = value.type.to(PartiallyMonotoneTuple.class);
        IMaybeMonotoneType fieldType = tuple.getField(expression.fieldNo);
        DBSPExpression reduced = null;
        if (fieldType.mayBeMonotone())
            reduced = value.getReducedExpression().field(tuple.compressedIndex(expression.fieldNo));
        MonotoneExpression result = new MonotoneExpression(
                expression, fieldType, reduced);
        this.maybeSet(expression, result);
    }

    @Override
    public void postorder(DBSPDerefExpression expression) {
        MonotoneExpression value = this.get(expression.expression);
        IMaybeMonotoneType type = value.type;
        DBSPExpression reduced = null;
        if (type.mayBeMonotone())
            reduced = value.getReducedExpression().deref();
        MonotoneExpression result = new MonotoneExpression(expression, type.to(MonotoneRefType.class).base, reduced);
        this.maybeSet(expression, result);
    }

    @Override
    public void postorder(DBSPBaseTupleExpression expression) {
        MonotoneExpression[] fields = Linq.map(expression.fields, this::get, MonotoneExpression.class);
        IMaybeMonotoneType[] types = Linq.map(fields, MonotoneExpression::getMonotoneType, IMaybeMonotoneType.class);
        PartiallyMonotoneTuple tuple = new PartiallyMonotoneTuple(Linq.list(types), expression.isRaw());
        DBSPExpression reduced = null;
        if (tuple.mayBeMonotone()) {
            MonotoneExpression[] monotoneFields = Linq.where(
                    fields, f -> f.getMonotoneType().mayBeMonotone(), MonotoneExpression.class);
            assert monotoneFields.length > 0;
            DBSPExpression[] monotoneComponents = Linq.map(
                    monotoneFields, MonotoneExpression::getReducedExpression, DBSPExpression.class);
            reduced = expression.isRaw() ?
                    new DBSPRawTupleExpression(monotoneComponents) :
                    new DBSPTupleExpression(monotoneComponents);
        }
        MonotoneExpression result = new MonotoneExpression(expression, tuple, reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPLiteral expression) {
        MonotoneExpression result = new MonotoneExpression(
                expression, new MonotoneType(expression.getType()), expression);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPExpression expression) {
        // All other cases: result is not monotone.
        MonotoneExpression result = new MonotoneExpression(
                expression, new NonMonotoneType(expression.getType()), null);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPBlockExpression expression) {
        MonotoneExpression result;
        if (expression.lastExpression != null) {
            result = this.get(expression.lastExpression);
        } else {
            result = new MonotoneExpression(expression, new NonMonotoneType(expression.type), null);
        }
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPLetStatement statement) {
        if (statement.initializer == null)
            return;
        MonotoneExpression value = this.get(statement.initializer);
        this.variables.put(statement, value);
    }

    static boolean isMonotoneBinaryOperation(DBSPOpcode opcode) {
        return opcode == DBSPOpcode.ADD ||
                opcode == DBSPOpcode.MAX;
    }

    @Override
    public void postorder(DBSPBinaryExpression expression) {
        MonotoneExpression left = this.get(expression.left);
        MonotoneExpression right = this.get(expression.right);
        DBSPExpression reduced = null;
        // Assume type is not monotone
        IMaybeMonotoneType resultType = new NonMonotoneType(expression.type);
        if (left.mayBeMonotone() && right.mayBeMonotone() &&
                isMonotoneBinaryOperation(expression.operation)) {
            resultType = new MonotoneType(expression.type);
            reduced = expression.replaceSources(
                    left.getReducedExpression(), right.getReducedExpression());
        }
        // Some expressions are monotone if some of their operands are constant
        if (left.mayBeMonotone() && expression.operation == DBSPOpcode.SUB) {
            // Subtracting a constant from a monotone expression produces a monotone result
            if (expression.right.is(DBSPLiteral.class)) {
                assert right.getReducedExpression() == expression.right;
                resultType = left.copyMonotonicity(expression.type);
                reduced = expression.replaceSources(
                        left.getReducedExpression(), right.getReducedExpression());
            }
        }
        if (left.mayBeMonotone() &&
                (expression.operation == DBSPOpcode.DIV || expression.operation == DBSPOpcode.MUL)) {
            // Multiplying or dividing a monotone expression by
            // a positive constant produces a monotone result
            // TODO: multiplication is commutative.
            if (expression.right.is(DBSPLiteral.class)) {
                if (expression.right.is(IsNumericLiteral.class)) {
                    if (expression.right.to(IsNumericLiteral.class).gt0()) {
                        assert right.getReducedExpression() == expression.right;
                        resultType = left.copyMonotonicity(expression.type);
                        reduced = expression.replaceSources(
                                left.getReducedExpression(), right.getReducedExpression());
                    }
                }
            }
        }
        MonotoneExpression result = new MonotoneExpression(expression, resultType, reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        // Casts always preserve monotonicity in SQL
        MonotoneExpression source = this.get(expression.source);
        DBSPExpression reduced = null;
        if (source.mayBeMonotone()) {
            reduced = expression.replaceSource(source.getReducedExpression());
        }
        MonotoneExpression result = new MonotoneExpression(
                expression, source.copyMonotonicity(expression.getType()), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPUnsignedWrapExpression expression) {
        MonotoneExpression source = this.get(expression.source);
        DBSPExpression reduced = null;
        if (source.mayBeMonotone()) {
            reduced = expression.replaceSource(source.getReducedExpression());
        }
        MonotoneExpression result = new MonotoneExpression(
                expression, source.copyMonotonicity(expression.getType()), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPUnsignedUnwrapExpression expression) {
        MonotoneExpression source = this.get(expression.source);
        DBSPExpression reduced = null;
        if (source.mayBeMonotone()) {
            reduced = expression.replaceSource(source.getReducedExpression());
        }
        MonotoneExpression result = new MonotoneExpression(
                expression, source.copyMonotonicity(expression.getType()), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPUnaryExpression expression) {
        MonotoneExpression source = this.get(expression.source);
        DBSPExpression reduced = null;
        if ((expression.operation == DBSPOpcode.UNARY_PLUS ||
                expression.operation == DBSPOpcode.TYPEDBOX) &&
            source.mayBeMonotone()) {
            reduced = expression.replaceSource(source.getReducedExpression());
        }
        MonotoneExpression result = new MonotoneExpression(
                expression, source.copyMonotonicity(expression.getType()), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        // Monotone functions applied to monotone arguments.
        MonotoneExpression[] arguments = Linq.map(expression.arguments, this::get, MonotoneExpression.class);
        boolean isMonotone = Linq.all(arguments, MonotoneExpression::mayBeMonotone);
        DBSPExpression reduced = null;
        IMaybeMonotoneType resultType = new NonMonotoneType(expression.getType());
        if (isMonotone) {
            DBSPExpression[] reducedArgs = Linq.map(
                    arguments, MonotoneExpression::getReducedExpression, DBSPExpression.class);
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
                        name.startsWith("extract_hour_Time") ||
                        name.equals("hop_start_timestamp")
                ) {
                    resultType = new MonotoneType(expression.getType());
                    reduced = expression.replaceArguments(reducedArgs);
                }

                if (name.startsWith("tumble_")
                        || name.startsWith("datediff_")
                        || name.startsWith("timestamp_diff_")) {
                    if (expression.arguments[1].is(DBSPLiteral.class)) {
                        resultType = new MonotoneType(expression.getType());
                        reduced = expression.replaceArguments(reducedArgs);
                    }
                }
            }
        }
        MonotoneExpression result = new MonotoneExpression(expression, resultType, reduced);
        this.set(expression, result);
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        this.resolver.apply(node);
        return super.apply(node);
    }

    @Override
    public String toString() {
        return super.toString() + ": " + this.operator + " " +
                Arrays.toString(this.parameterTypes);
    }
}
