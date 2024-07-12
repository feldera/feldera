package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.DeclarationValue;
import org.dbsp.sqlCompiler.compiler.visitors.inner.RepeatedExpressions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.TranslateVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Given a function (ClosureExpression) and the
 * monotonicity information of its parameters, this visitor
 * computes a MonotoneExpression corresponding to the function. */
public class MonotoneTransferFunctions extends TranslateVisitor<MonotoneExpression> {
    /** Description of the type of the argument of the analyzed function */
    public enum ArgumentKind {
        /** The argument represents a value in a ZSet.
         * The parameter type will be &k */
        ZSet,
        /** The argument represents a value in an IndexedZSet.
         * The parameter type will be (&k, &v) */
        IndexedZSet,
        /** The argument is used for join function.
         * The parameter type will be (&k, &l, &r). */
        Join;

        public static ArgumentKind fromType(DBSPType type) {
            if (type.is(DBSPTypeZSet.class))
                return ZSet;
            if (type.is(DBSPTypeIndexedZSet.class))
                return IndexedZSet;
            throw new UnsupportedException(type.getNode());
        }
    };

    IMaybeMonotoneType[] parameterTypes;
    /** Maps each declaration to its current value. */
    final DeclarationValue<MonotoneExpression> variables;
    /** Ids of expressions which are guaranteed to have constant values */
    final Set<Long> constantExpressions;
    /** Ids of expressions which are guaranteed to have constant and positive values
     * (They all must have numeric types) */
    final Set<Long> positiveExpressions;
    final ResolveReferences resolver;
    final ArgumentKind argumentKind;
    /** Operator where the analyzed closure originates from.
     * Only used for debugging. */
    final DBSPOperator operator;

    /** Create a visitor to analyze the monotonicity of a closure
     * @param reporter      Error reporter.
     * @param operator      Operator whose function is analyzed.
     * @param parameterTypes Monotonicity information for the function parameters.
     *                      This information comes from the operator that is
     *                      a source for this function.
     * @param argumentKind  Describes the arguments of the analyzed function. */
    public MonotoneTransferFunctions(IErrorReporter reporter,
                                     DBSPOperator operator,
                                     ArgumentKind argumentKind,
                                     IMaybeMonotoneType... parameterTypes) {
        super(reporter);
        this.operator = operator;
        this.parameterTypes = parameterTypes;
        for (IMaybeMonotoneType p: parameterTypes)
            assert !p.is(MonotoneClosureType.class);
        this.argumentKind = argumentKind;
        this.variables = new DeclarationValue<>();
        this.resolver = new ResolveReferences(reporter, false);
        this.constantExpressions = new HashSet<>();
        this.positiveExpressions = new HashSet<>();
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
        switch (this.argumentKind) {
            case IndexedZSet: {
                // Functions that iterate over IndexedZSets have the signature: |t: (&key, &value)| body.
                assert this.parameterTypes.length == 1;
                PartiallyMonotoneTuple tuple = this.parameterTypes[0].to(PartiallyMonotoneTuple.class);
                List<IMaybeMonotoneType> fields = Linq.map(tuple.fields, MonotoneRefType::new);
                this.parameterTypes =  new IMaybeMonotoneType[] { new PartiallyMonotoneTuple(fields, tuple.raw, tuple.mayBeNull) };
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
                break;
            }
            case Join:
            case ZSet: {
                // Functions with one on or more parameters that iterate over ZSets have the
                // signature |p0: &type0, p1: &type1| body.
                this.parameterTypes = Linq.map(this.parameterTypes, MonotoneRefType::new, IMaybeMonotoneType.class);
                projectedParameters = new DBSPParameter[projectedTypes.length];
                for (int i = 0; i < projectedTypes.length; i++)
                    projectedParameters[i] = new DBSPParameter(
                            expression.parameters[i].getName(), projectedTypes[i].ref());
                break;
            }
            default: {
                throw new UnimplementedException();
            }
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
            switch (this.argumentKind) {
                case IndexedZSet: {
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
                                            new DBSPRawTupleExpression(parameterFieldsToKeep))
                            ),
                            applyBody
                    );
                    applyParameters = new DBSPParameter[] { applyParameter };
                    break;
                }
                case Join: {
                    // The "apply" DBSP node that computes the limit values
                    // will have type a: &(K, L, R).  But the function generated so
                    // far has type |k: &K, l: &L, r: &R|.
                    //
                    // So we generate this code:
                    // |a: &(K, L, R)| -> R {
                    //    let k = &(*a).0;
                    //    let l = &(*a).1;
                    //    let r = &(*a).2;
                    //    <previous body using k, l, r>
                    // }
                    assert this.parameterTypes.length == 3;
                    DBSPParameter applyParameter = new DBSPParameter("a",
                            new DBSPTypeRawTuple(projectedTypes[0], projectedTypes[1], projectedTypes[2]).ref());
                    List<DBSPStatement> statements = new ArrayList<>();
                    for (int i = 0; i < 3; i++)
                        statements.add(new DBSPLetStatement(expression.parameters[i].getName(),
                                        applyParameter.asVariable().deref().field(i).borrow()));
                    applyBody = new DBSPBlockExpression(
                            statements,
                            applyBody
                    );
                    applyParameters = new DBSPParameter[] { applyParameter };
                    break;
                }
                case ZSet: {
                    applyParameters = projectedParameters;
                    break;
                }
                default: {
                    throw new UnimplementedException();
                }
            }
            DBSPClosureExpression closure = applyBody.closure(applyParameters);
            MonotoneClosureType cloType = new MonotoneClosureType(bodyValue.type,
                    expression.parameters,
                    applyParameters);
            MonotoneExpression result = new MonotoneExpression(expression, cloType, closure);
            this.set(expression, result);
            Logger.INSTANCE.belowLevel(this, 1)
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
        assert value == null || value.expression.getType().sameType(var.getType())
                : "Variable " + var.variable + "(" + var.getId() + ") type " + var.getType() +
                " does not match expected type in expression " + value.expression +
                "(" + value.expression.getId() + ")" +
                " with type " + value.expression.getType();
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
        PartiallyMonotoneTuple tuple = new PartiallyMonotoneTuple(
                Linq.list(types), expression.isRaw(), expression.getType().mayBeNull);
        DBSPExpression reduced = null;
        if (tuple.mayBeMonotone()) {
            MonotoneExpression[] monotoneFields = Linq.where(
                    fields, f -> f.getMonotoneType().mayBeMonotone(), MonotoneExpression.class);
            assert monotoneFields.length > 0;
            DBSPExpression[] monotoneComponents = Linq.map(
                    monotoneFields, MonotoneExpression::getReducedExpression, DBSPExpression.class);
            reduced = expression.isRaw() ?
                    new DBSPRawTupleExpression(monotoneComponents) :
                    new DBSPTupleExpression(
                            expression.getNode(), expression.getType().mayBeNull, monotoneComponents);
        }
        boolean allConstant = Linq.all(fields, f -> this.constantExpressions.contains(f.id));
        if (allConstant) {
            this.constantExpressions.add(expression.id);
        }
        MonotoneExpression result = new MonotoneExpression(expression, tuple, reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPLiteral expression) {
        MonotoneExpression result = new MonotoneExpression(
                expression, new MonotoneType(expression.getType()), expression);
        this.constantExpressions.add(expression.id);
        if (expression.is(IsNumericLiteral.class)) {
            if (!expression.to(DBSPLiteral.class).isNull &&
                    expression.to(IsNumericLiteral.class).gt0())
                this.positiveExpressions.add(expression.id);
        }
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPExpression expression) {
        // All other cases: result is not monotone.
        DBSPType type = expression.getType();
        IMaybeMonotoneType nmt = NonMonotoneType.nonMonotone(type);
        MonotoneExpression result = new MonotoneExpression(expression, nmt, null);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPCloneExpression expression) {
        MonotoneExpression source = this.get(expression.expression);
        DBSPExpression reduced = null;

        if (source.mayBeMonotone()) {
            reduced = new DBSPCloneExpression(expression.getNode(), source.getReducedExpression());
        }
        if (this.positiveExpressions.contains(source.id))
            this.positiveExpressions.add(expression.id);
        if (this.constantExpressions.contains(source.id))
            this.constantExpressions.add(expression.id);
        MonotoneExpression result = new MonotoneExpression(expression, source.getMonotoneType(), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPSomeExpression expression) {
        MonotoneExpression source = this.get(expression.expression);
        DBSPExpression reduced = null;

        if (source.mayBeMonotone()) {
            reduced = new DBSPSomeExpression(expression.getNode(), source.getReducedExpression());
        }
        if (this.positiveExpressions.contains(source.id))
            this.positiveExpressions.add(expression.id);
        if (this.constantExpressions.contains(source.id))
            this.constantExpressions.add(expression.id);
        MonotoneExpression result = new MonotoneExpression(
                expression, source.getMonotoneType().setMaybeNull(true), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPBlockExpression expression) {
        MonotoneExpression result;
        if (expression.lastExpression != null) {
            result = this.get(expression.lastExpression);
            if (this.constantExpressions.contains(expression.lastExpression.id))
                this.constantExpressions.add(expression.id);
            if (this.positiveExpressions.contains(expression.lastExpression.id))
                this.positiveExpressions.add(expression.id);
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

    @Override
    public void postorder(DBSPBinaryExpression expression) {
        MonotoneExpression left = this.get(expression.left);
        MonotoneExpression right = this.get(expression.right);
        DBSPExpression reduced = null;
        if (this.constantExpressions.contains(expression.left.id) &&
            this.constantExpressions.contains(expression.right.id))
            this.constantExpressions.add(expression.id);

        boolean lm = left.mayBeMonotone();
        boolean rm = right.mayBeMonotone();

        // Assume type is not monotone
        IMaybeMonotoneType resultType = new NonMonotoneType(expression.type);
        if (expression.operation == DBSPOpcode.ADD && lm && rm) {
            // The addition of two monotone expressions is monotone
            resultType = new MonotoneType(expression.type);
            reduced = expression.replaceSources(
                    left.getReducedExpression(), right.getReducedExpression());
        }
        if (expression.operation == DBSPOpcode.MAX && (lm || rm)) {
            // The result of MAX is monotone if either expression is monotone
            resultType = new MonotoneType(expression.type);
            if (!lm) {
                reduced = right.getReducedExpression()
                        // must preserve type
                        .cast(expression.getType());
            } else if (!rm) {
                reduced = left.getReducedExpression()
                        .cast(expression.getType());
            } else {
                reduced = expression.replaceSources(
                        left.getReducedExpression(),
                        right.getReducedExpression());
            }
        }
        // Some expressions are monotone if some of their operands are constant
        if (left.mayBeMonotone() && expression.operation == DBSPOpcode.SUB) {
            // Subtracting a constant from a monotone expression produces a monotone result
            if (this.constantExpressions.contains(expression.right.id)) {
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
                if (this.positiveExpressions.contains(expression.right.id)) {
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
        if (this.positiveExpressions.contains(expression.source.id) &&
                expression.type.is(IsNumericType.class))
            this.positiveExpressions.add(expression.id);
        if (this.constantExpressions.contains(expression.source.id))
            this.constantExpressions.add(expression.id);
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
        this.positiveExpressions.add(expression.id);
        if (this.constantExpressions.contains(expression.source.id))
            this.constantExpressions.add(expression.id);
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
        if (this.constantExpressions.contains(expression.source.id))
            this.constantExpressions.add(expression.id);
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
            if (this.positiveExpressions.contains(expression.source.id))
                this.positiveExpressions.add(expression.id);
        }
        if (this.constantExpressions.contains(expression.source.id))
            this.constantExpressions.add(expression.id);
        MonotoneExpression result = new MonotoneExpression(
                expression, source.copyMonotonicity(expression.getType()), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPApplyMethodExpression expression) {
        DBSPExpression reduced = null;
        MonotoneExpression[] arguments = Linq.map(expression.arguments, this::get, MonotoneExpression.class);
        MonotoneExpression self = this.get(expression.self);
        IMaybeMonotoneType resultType = NonMonotoneType.nonMonotone(expression.getType());
        boolean allArgsMonotone = Linq.all(arguments, MonotoneExpression::mayBeMonotone);
        if (allArgsMonotone && self.mayBeMonotone()) {
            DBSPExpression reducedSelf = self.getReducedExpression();
            if (expression.function.is(DBSPPathExpression.class)) {
                DBSPPathExpression path = expression.function.to(DBSPPathExpression.class);
                String name = path.toString();
                if (name.equals("to_bound")) {
                    // No arguments, and self is supposed to be a constant
                    resultType = new MonotoneType(expression.getType());
                    reduced = expression.replaceArguments(reducedSelf);
                    if (this.constantExpressions.contains(expression.self.id)) {
                        this.constantExpressions.add(expression.id);
                    }
                }
            }
        }

        MonotoneExpression result = new MonotoneExpression(expression, resultType, reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        // Monotone functions applied to monotone arguments.
        MonotoneExpression[] arguments = Linq.map(expression.arguments, this::get, MonotoneExpression.class);
        boolean allArgsMonotone = Linq.all(arguments, MonotoneExpression::mayBeMonotone);
        DBSPExpression reduced = null;
        IMaybeMonotoneType resultType = new NonMonotoneType(expression.getType());
        if (allArgsMonotone) {
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
                this.argumentKind + " " +
                Arrays.toString(this.parameterTypes);
    }
}
