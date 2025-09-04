package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsTimeRelatedType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

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
    }

    static class ExpressionSet {
        final Set<Long> ids;

        ExpressionSet() {
            this.ids = new HashSet<>();
        }

        void add(DBSPExpression expression) {
            this.ids.add(expression.id);
        }

        boolean contains(DBSPExpression expression) {
            return this.ids.contains(expression.id);
        }
    }

    IMaybeMonotoneType[] parameterTypes;
    /** Maps each declaration to its current value. */
    final DeclarationValue<MonotoneExpression> variables;
    /** Monotone expressions which are guaranteed to have constant values */
    final ExpressionSet constantExpressions;
    /** Monotone expressions which are guaranteed to have constant and positive values
     * (They all must have numeric types) */
    final ExpressionSet positiveExpressions;
    final ResolveReferences resolver;
    final ArgumentKind argumentKind;
    /** Operator where the analyzed closure originates from.
     * Only used for debugging. */
    final DBSPSimpleOperator operator;

    /** Create a visitor to analyze the monotonicity of a closure
     * @param compiler      Compiler.
     * @param operator      Operator whose function is analyzed.
     * @param parameterTypes Monotonicity information for the function parameters.
     *                      This information comes from the operator that is
     *                      a source for this function.
     * @param argumentKind  Describes the arguments of the analyzed function. */
    public MonotoneTransferFunctions(DBSPCompiler compiler,
                                     DBSPSimpleOperator operator,
                                     ArgumentKind argumentKind,
                                     IMaybeMonotoneType... parameterTypes) {
        super(compiler);
        this.operator = operator;
        this.parameterTypes = parameterTypes;
        for (IMaybeMonotoneType p : parameterTypes)
            Utilities.enforce(!p.is(MonotoneClosureType.class));
        this.argumentKind = argumentKind;
        this.variables = new DeclarationValue<>();
        this.resolver = new ResolveReferences(compiler, false);
        this.constantExpressions = new ExpressionSet();
        this.positiveExpressions = new ExpressionSet();
        switch (argumentKind) {
            case IndexedZSet:
            case ZSet:
                Utilities.enforce(parameterTypes.length == 1);
                break;
            case Join:
                Utilities.enforce(parameterTypes.length == 3);
                break;
            default:
                throw new InternalCompilerError("unreachable");
        }
    }

    @Override
    public VisitDecision preorder(DBSPFlatmap expression) {
        Utilities.enforce(this.parameterTypes.length == 1);
        Utilities.enforce(this.argumentKind == ArgumentKind.ZSet);
        // Synthesize a DBSPClosure expression that is much simpler than the Flatmap,
        // but carries only the fields that are copied ad-literam in the output
        // and analyze that expression.

        // This logic parallels the one from LowerCircuitVisitor
        DBSPVariablePath param = expression.inputRowType.ref().var();
        List<DBSPExpression> outputFields = new ArrayList<>();
        for (int index : expression.leftInputIndexes) {
            DBSPExpression field = param.deepCopy().deref().field(index);
            outputFields.add(field);
        }

        // If collection element is a tuple type, all fields are inlined.
        DBSPTypeTupleBase elementTupleType = expression.getCollectionElementType().as(DBSPTypeTupleBase.class);
        if (expression.ordinalityIndexType != null) {
            outputFields.add(new NoExpression(expression.ordinalityIndexType));
        } else {
            if (expression.rightProjections != null) {
                for (DBSPClosureExpression clo : expression.rightProjections) {
                    // TODO: this is very conservative
                    outputFields.add(new NoExpression(clo.getResultType()));
                }
            } else if (elementTupleType != null) {
                for (DBSPType elem : elementTupleType.tupFields) {
                    outputFields.add(new NoExpression(elem));
                }
            } else {
                outputFields.add(new NoExpression(expression.getCollectionElementType()));
            }
        }
        if (expression.ordinalityIndexType != null)
            outputFields.add(new NoExpression(expression.ordinalityIndexType));
        outputFields = expression.shuffle.shuffle(outputFields);
        DBSPExpression tuple = new DBSPTupleExpression(outputFields, false);
        DBSPType resultType = expression.getType().to(DBSPTypeFunction.class).resultType;
        Utilities.enforce(tuple.getType().sameType(resultType),
                "Flatmap result type " + resultType + " does not match computed type " + tuple.getType());
        DBSPClosureExpression closure = tuple.closure(param);
        // This is the same as this.apply(closure)
        this.resolver.apply(closure);
        this.preorder(closure);
        // The result applies to the original expression
        MonotoneExpression result = this.maybeGet(closure);
        this.maybeSet(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        if (!this.context.isEmpty())
            // This means that we are analyzing a closure within another closure.
            throw new InternalCompilerError("Didn't expect nested closures", expression);

        // Must be the outermost call of the visitor.
        DBSPType[] projectedTypes = Linq.map(
                Objects.requireNonNull(this.parameterTypes), IMaybeMonotoneType::getProjectedType, DBSPType.class);

        DBSPParameter[] projectedParameters;
        switch (this.argumentKind) {
            case IndexedZSet: {
                // Functions that iterate over IndexedZSets have the signature: |t: (&key, &value)| body.
                Utilities.enforce(this.parameterTypes.length == 1);
                PartiallyMonotoneTuple tuple = this.parameterTypes[0].to(PartiallyMonotoneTuple.class);
                List<IMaybeMonotoneType> fields = Linq.map(tuple.fields, MonotoneRefType::new);
                this.parameterTypes = new IMaybeMonotoneType[]{new PartiallyMonotoneTuple(fields, tuple.raw, tuple.mayBeNull)};
                DBSPType paramType;
                DBSPType[] tupleFields = Linq.map(projectedTypes[0].to(DBSPTypeTupleBase.class).tupFields,
                        DBSPType::ref, DBSPType.class);
                if (tuple.raw) {
                    paramType = new DBSPTypeRawTuple(tupleFields);
                } else {
                    paramType = new DBSPTypeTuple(tupleFields);
                }
                projectedParameters = new DBSPParameter[]{
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
                throw new InternalCompilerError("Unreachable");
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
        new RepeatedExpressions(this.compiler, true, true).apply(expression.body);
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
                    Utilities.enforce(!parameterFieldsToKeep.isEmpty());
                    applyBody = new DBSPBlockExpression(
                            Linq.list(
                                    new DBSPLetStatement(expression.parameters[0].getName(),
                                            new DBSPRawTupleExpression(parameterFieldsToKeep))
                            ),
                            applyBody
                    );
                    applyParameters = new DBSPParameter[]{applyParameter};
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
                    Utilities.enforce(this.parameterTypes.length == 3);
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
                    applyParameters = new DBSPParameter[]{applyParameter};
                    break;
                }
                case ZSet: {
                    applyParameters = projectedParameters;
                    break;
                }
                default: {
                    throw new InternalCompilerError("unreachable");
                }
            }
            DBSPClosureExpression closure = applyBody.closure(applyParameters);
            MonotoneClosureType cloType = new MonotoneClosureType(bodyValue.type,
                    expression.parameters,
                    applyParameters);
            MonotoneExpression result = new MonotoneExpression(expression, cloType, closure);
            this.set(expression, result);
            Logger.INSTANCE.belowLevel(this, 1)
                    .appendSupplier(() -> "MonotoneExpression for " + expression + " is " + result)
                    .newline();
        }
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPVariablePath var) {
        IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(var);
        MonotoneExpression value = this.variables.get(declaration);
        this.maybeSet(var, value);
        if (value == null)
            return;
        Utilities.enforce(value.expression.getType().sameType(var.getType()),
                "Variable " + var.variable + "(" + var.getId() + ") type " + var.getType() +
                " does not match expected type in expression " + value.expression +
                "(" + value.expression.getId() + ")" +
                " with type " + value.expression.getType());
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
    public void postorder(DBSPBorrowExpression expression) {
        MonotoneExpression value = this.get(expression.expression);
        IMaybeMonotoneType sourceType = value.type;
        DBSPExpression reduced = null;
        IMaybeMonotoneType type = NonMonotoneType.nonMonotone(expression.type);
        if (sourceType.mayBeMonotone()) {
            reduced = value.getReducedExpression().borrow();
            type = new MonotoneRefType(sourceType);
        }
        MonotoneExpression result = new MonotoneExpression(expression, type, reduced);
        if (this.constantExpressions.contains(expression.expression))
            this.constantExpressions.add(expression);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPDerefExpression expression) {
        MonotoneExpression value = this.get(expression.expression);
        IMaybeMonotoneType type = value.type;
        DBSPExpression reduced = null;
        if (type.mayBeMonotone())
            reduced = value.getReducedExpression().deref();
        MonotoneExpression result = new MonotoneExpression(expression, type.to(MonotoneRefType.class).base, reduced);
        if (this.constantExpressions.contains(expression.expression))
            this.constantExpressions.add(expression);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPBaseTupleExpression expression) {
        if (expression.fields == null) {
            // A constant tuple with NULL value
            this.maybeSet(expression, null);
            return;
        }
        DBSPExpression reduced = null;
        MonotoneExpression[] fields = Linq.map(expression.fields, this::get, MonotoneExpression.class);
        IMaybeMonotoneType[] types = Linq.map(fields, MonotoneExpression::getMonotoneType, IMaybeMonotoneType.class);
        PartiallyMonotoneTuple tuple = new PartiallyMonotoneTuple(
                Linq.list(types), expression.isRaw(), expression.getType().mayBeNull);
        if (tuple.mayBeMonotone()) {
            MonotoneExpression[] monotoneFields = Linq.where(
                    fields, f -> f.getMonotoneType().mayBeMonotone(), MonotoneExpression.class);
            Utilities.enforce(monotoneFields.length > 0 || fields.length == 0);
            DBSPExpression[] monotoneComponents = Linq.map(
                    monotoneFields, MonotoneExpression::getReducedExpression, DBSPExpression.class);
            DBSPTypeTuple type = new DBSPTypeTuple(CalciteObject.EMPTY,
                    expression.getType().mayBeNull,
                    Linq.list(Linq.map(monotoneComponents, DBSPExpression::getType, DBSPType.class)));
            reduced = expression.isRaw() ?
                    new DBSPRawTupleExpression(monotoneComponents) :
                    new DBSPTupleExpression(expression.getNode(), type, monotoneComponents);
        }
        boolean allConstant = Linq.all(expression.fields, this.constantExpressions::contains);
        if (allConstant) {
            this.constantExpressions.add(expression);
        }
        MonotoneExpression result = new MonotoneExpression(expression, tuple, reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPLiteral expression) {
        if (!MonotoneTransferFunctions.typeCanBeMonotone(expression.getType())) {
            IMaybeMonotoneType nmt = NonMonotoneType.nonMonotone(expression.getType());
            MonotoneExpression result = new MonotoneExpression(expression, nmt, null);
            this.set(expression, result);
            return;
        }
        MonotoneExpression result = new MonotoneExpression(
                expression, new MonotoneType(expression.getType()), expression);
        this.constantExpressions.add(expression);
        if (expression.is(IsNumericLiteral.class)) {
            if (!expression.to(DBSPLiteral.class).isNull() &&
                    expression.to(IsNumericLiteral.class).gt0())
                this.positiveExpressions.add(expression);
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
        if (this.positiveExpressions.contains(expression.expression))
            this.positiveExpressions.add(expression);
        if (this.constantExpressions.contains(expression.expression))
            this.constantExpressions.add(expression);
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
        if (this.positiveExpressions.contains(expression.expression))
            this.positiveExpressions.add(expression);
        if (this.constantExpressions.contains(expression.expression))
            this.constantExpressions.add(expression);
        MonotoneExpression result = new MonotoneExpression(
                expression, source.getMonotoneType().withMaybeNull(true), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPUnwrapExpression expression) {
        MonotoneExpression source = this.get(expression.expression);
        DBSPExpression reduced = null;

        if (source.mayBeMonotone()) {
            reduced = new DBSPUnwrapExpression(source.getReducedExpression());
        }
        if (this.positiveExpressions.contains(expression.expression))
            this.positiveExpressions.add(expression);
        if (this.constantExpressions.contains(expression.expression))
            this.constantExpressions.add(expression);
        IMaybeMonotoneType monoType = source.getMonotoneType().withMaybeNull(false);
        MonotoneExpression result = new MonotoneExpression(
                expression, monoType, reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPBlockExpression expression) {
        MonotoneExpression result;
        if (expression.lastExpression != null) {
            result = this.get(expression.lastExpression);
            if (this.constantExpressions.contains(expression.lastExpression))
                this.constantExpressions.add(expression);
            if (this.positiveExpressions.contains(expression.lastExpression))
                this.positiveExpressions.add(expression);
        } else {
            result = new MonotoneExpression(expression, new NonMonotoneType(expression.type), null);
        }
        this.set(expression, result);
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression expression) {
        // We do this in preorder, because we have to set the
        // variable value after visiting the initializer.
        this.push(expression);
        expression.initializer.accept(this);
        MonotoneExpression value = this.get(expression.initializer);
        this.variables.put(expression, value);

        expression.consumer.accept(this);
        MonotoneExpression result = this.get(expression.consumer);
        if (this.constantExpressions.contains(expression.consumer))
            this.constantExpressions.add(expression);
        if (this.positiveExpressions.contains(expression.consumer))
            this.positiveExpressions.add(expression);
        this.pop(expression);
        this.set(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPLetExpression expression) {
        MonotoneExpression value = this.get(expression.initializer);
        this.variables.put(expression, value);

        MonotoneExpression result = this.get(expression.consumer);
        if (this.constantExpressions.contains(expression.consumer))
            this.constantExpressions.add(expression);
        if (this.positiveExpressions.contains(expression.consumer))
            this.positiveExpressions.add(expression);
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
        if (this.constantExpressions.contains(expression.left) &&
            this.constantExpressions.contains(expression.right))
            this.constantExpressions.add(expression);

        boolean lm = left.mayBeMonotone();
        boolean rm = right.mayBeMonotone();

        // Assume type is not monotone
        IMaybeMonotoneType resultType = new NonMonotoneType(expression.type);
        if ((expression.opcode == DBSPOpcode.ADD || expression.opcode == DBSPOpcode.TS_ADD)
                && lm && rm) {
            // The addition of two monotone expressions is monotone
            resultType = new MonotoneType(expression.type);
            reduced = expression.replaceSources(
                    left.getReducedExpression(), right.getReducedExpression());
        }
        if (expression.opcode == DBSPOpcode.MAX && (lm || rm)) {
            // The result of MAX is monotone if either expression is monotone
            resultType = new MonotoneType(expression.type);
            if (!lm) {
                reduced = right.getReducedExpression()
                        // must preserve type
                        .cast(expression.getNode(), expression.getType(), false);
            } else if (!rm) {
                reduced = left.getReducedExpression()
                        .cast(expression.getNode(), expression.getType(), false);
            } else {
                reduced = expression.replaceSources(
                        left.getReducedExpression(),
                        right.getReducedExpression());
            }
        } else if (expression.opcode == DBSPOpcode.MIN && lm && rm) {
            // The result of MIN is monotone if both expressions are monotone
            resultType = new MonotoneType(expression.type);
            reduced = expression.replaceSources(
                    left.getReducedExpression(),
                    right.getReducedExpression());
        }
        // Some expressions are monotone if some of their operands are constant
        if (left.mayBeMonotone() &&
                (expression.opcode == DBSPOpcode.SUB ||
                        expression.opcode == DBSPOpcode.TS_SUB)) {
            // Subtracting a constant from a monotone expression produces a monotone result
            if (this.constantExpressions.contains(expression.right)) {
                resultType = left.copyMonotonicity(expression.type);
                reduced = expression.replaceSources(
                        left.getReducedExpression(), right.getReducedExpression());
            }
        }
        if (left.mayBeMonotone() &&
                (expression.opcode == DBSPOpcode.DIV ||
                        expression.opcode == DBSPOpcode.MUL ||
                        expression.opcode == DBSPOpcode.INTERVAL_MUL ||
                        expression.opcode == DBSPOpcode.INTERVAL_DIV)) {
            // Multiplying or dividing a monotone expression by
            // a positive constant produces a monotone result
            // TODO: multiplication is commutative.
            if (expression.right.is(DBSPLiteral.class)) {
                if (this.positiveExpressions.contains(expression.right)) {
                    if (expression.right.to(IsNumericLiteral.class).gt0()) {
                        Utilities.enforce(right.getReducedExpression() == expression.right);
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

    static public boolean typeCanBeMonotone(DBSPType type) {
        return type.is(IsNumericType.class) ||
                type.is(IsTimeRelatedType.class) ||
                type.is(DBSPTypeBool.class);
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        MonotoneExpression source = this.get(expression.source);
        DBSPExpression reduced = null;

        // Casts always preserve monotonicity in SQL, but only if the
        // result type can represent monotone values.
        boolean outputTypeMayBeMonotone = typeCanBeMonotone(expression.getType());
        boolean isMonotone = source.mayBeMonotone() && outputTypeMayBeMonotone;
        IMaybeMonotoneType resultType;
        if (isMonotone) {
            reduced = expression.replaceSource(source.getReducedExpression());
            resultType = source.copyMonotonicity(expression.getType());
        } else {
            resultType = NonMonotoneType.nonMonotone(expression.type);
        }
        if (this.positiveExpressions.contains(expression.source) &&
                expression.type.is(IsNumericType.class))
            this.positiveExpressions.add(expression);
        if (this.constantExpressions.contains(expression.source))
            this.constantExpressions.add(expression);
        MonotoneExpression result = new MonotoneExpression(expression, resultType, reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPUnsignedWrapExpression expression) {
        MonotoneExpression source = this.get(expression.source);
        DBSPExpression reduced = null;
        if (source.mayBeMonotone()) {
            reduced = expression.replaceSource(source.getReducedExpression());
        }
        this.positiveExpressions.add(expression);
        if (this.constantExpressions.contains(expression.source))
            this.constantExpressions.add(expression);
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
        if (this.constantExpressions.contains(expression.source))
            this.constantExpressions.add(expression);
        MonotoneExpression result = new MonotoneExpression(
                expression, source.copyMonotonicity(expression.getType()), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPUnaryExpression expression) {
        MonotoneExpression source = this.get(expression.source);
        DBSPExpression reduced = null;
        if ((expression.opcode == DBSPOpcode.UNARY_PLUS ||
                expression.opcode == DBSPOpcode.TYPEDBOX) &&
            source.mayBeMonotone()) {
            reduced = expression.replaceSource(source.getReducedExpression());
            if (this.positiveExpressions.contains(expression.source))
                this.positiveExpressions.add(expression);
        }
        if (this.constantExpressions.contains(expression.source))
            this.constantExpressions.add(expression);
        MonotoneExpression result = new MonotoneExpression(
                expression, source.copyMonotonicity(expression.getType()), reduced);
        this.set(expression, result);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        // Monotone functions applied to monotone arguments.
        MonotoneExpression[] arguments = Linq.map(expression.arguments, this::get, MonotoneExpression.class);
        boolean allArgsMonotone = Linq.all(arguments, MonotoneExpression::mayBeMonotone);
        boolean allArgsConstant = Linq.all(expression.arguments, this.constantExpressions::contains);
        DBSPExpression reduced = null;
        IMaybeMonotoneType resultType = new NonMonotoneType(expression.getType());
        boolean isDeterministic = true;
        if (allArgsMonotone || allArgsConstant) {
            DBSPExpression[] reducedArgs = Linq.map(
                    arguments, MonotoneExpression::getReducedExpression, DBSPExpression.class);
            String name = expression.getFunctionName();
            if (name != null) {
                isDeterministic = !name.equals("now");
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
                        name.startsWith("extract_millennium_") ||
                        name.startsWith("extract_century_") ||
                        name.startsWith("extract_epoch_") ||
                        name.startsWith("extract_hour_Time_") ||
                        name.startsWith("dateadd_") ||
                        name.equals("hop_start_timestamp") ||
                        name.startsWith("to_bound_") ||
                        name.startsWith("date_trunc_") ||
                        name.startsWith("time_trunc_") ||
                        name.startsWith("timestamp_trunc_") ||
                        name.equals("now") // Only when called from ImplementNow
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
            if (allArgsConstant && isDeterministic) {
                this.constantExpressions.add(expression);
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
