package org.dbsp.sqlCompiler.compiler.visitors.outer.intern;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalIncrementExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeInterned;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.RAW_TUPLE;

/** Inner visitor that rewrites expressions by
 * propagating interned values from sources through expressions and replaces
 * an interned value with unintern(value) when necessary. */
public class InternInner extends ExpressionTranslator {
    public static final String uninternFunction = "unintern";
    public static final String internFunction = "intern";
    public static final DBSPType nullableString = DBSPTypeString.varchar(true);

    final DBSPType[] parameterTypes;
    final Map<DBSPParameter, DBSPType> outerParameters;
    @Nullable
    ReferenceMap refMap;
    final Set<DBSPStringLiteral> globalStrings;
    /** If true add string literals to globalStrings and replace with an intern(literal) call. */
    final boolean internConstants;
    /** If true unintern immediately every expression with intered type */
    final boolean uninternEverything;

    public InternInner(DBSPCompiler compiler, boolean internConstants,
                       boolean uninternEverything, DBSPType... parameterTypes) {
        super(compiler);
        if (parameterTypes.length == 0)
            throw new InternalCompilerError("No parameter types provided");
        this.parameterTypes = parameterTypes;
        this.internConstants = internConstants;
        this.uninternEverything = uninternEverything;
        this.outerParameters = new HashMap<>();
        this.refMap = null;
        this.globalStrings = new HashSet<>();
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        if (node.is(DBSPExpression.class))
            node = node.to(DBSPExpression.class).ensureTree(this.compiler);
        return super.apply(node);
    }

    DBSPClosureExpression convert(DBSPClosureExpression expression) {
        // clearing is needed because all the closures in the same aggregate list
        // share the same parameter name
        this.clear();
        DBSPClosureExpression map = expression.ensureTree(this.compiler).to(DBSPClosureExpression.class);
        ResolveReferences resolveReferences = new ResolveReferences(this.compiler, false);
        resolveReferences.apply(map);
        this.refMap = resolveReferences.reference;
        map.accept(this);
        return this.get(map).to(DBSPClosureExpression.class);
    }

    private final Map<DBSPExpression, DBSPExpression> saved = new HashMap<>();

    private void save(DBSPExpression expression, DBSPExpression replacement) {
        this.map(expression, replacement);
        Utilities.putNew(this.saved, expression, replacement);
    }

    private void restore() {
        for (var entry: this.saved.entrySet()) {
            this.map(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public VisitDecision preorder(DBSPAggregateList list) {
        Utilities.enforce(this.parameterTypes.length == 1);
        this.refMap = new ReferenceMap();
        // Make up a parameter for the row variable
        DBSPParameter rowVarParam = list.rowVar.asParameter();
        Utilities.putNew(this.outerParameters, rowVarParam, this.parameterTypes[0]);
        this.refMap.declare(list.rowVar, rowVarParam);
        for (IAggregate agg: list.aggregates) {
            List<DBSPParameter> params = agg.getRowVariableReferences();
            for (DBSPParameter param: params)
                Utilities.putNew(this.outerParameters, param, this.parameterTypes[0]);
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPFlatmap flatmap) {
        Utilities.enforce(this.parameterTypes.length == 1);
        DBSPClosureExpression closure = flatmap.collectionExpression
                .ensureTree(this.compiler)
                .to(DBSPClosureExpression.class);
        Utilities.enforce(closure.parameters.length == 1);
        Utilities.putNew(this.outerParameters, closure.parameters[0], this.parameterTypes[0]);
        ResolveReferences resolver = new ResolveReferences(this.compiler, false);
        resolver.apply(closure);
        this.refMap = resolver.reference;
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(LinearAggregate aggregate) {
        DBSPClosureExpression map = this.convert(aggregate.map);
        LinearAggregate result = new LinearAggregate(aggregate.getNode(), map,
                aggregate.postProcess, aggregate.emptySetResult);
        this.save(aggregate, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(NonLinearAggregate aggregate) {
        DBSPClosureExpression increment = this.convert(aggregate.increment);
        DBSPExpression result = new NonLinearAggregate(aggregate.getNode(),
                aggregate.zero, increment, aggregate.postProcess, aggregate.emptySetResult, aggregate.semigroup);
        this.save(aggregate, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFold fold) {
        Utilities.enforce(fold.increment.parameters.length == 3);
        // Preserve types for parameters 0 and 2 of the fold
        InternInner interner = new InternInner(this.compiler, this.internConstants, true,
                fold.increment.parameters[0].getType(), this.parameterTypes[0], fold.increment.parameters[2].getType());
        DBSPClosureExpression increment = interner.convert(fold.increment);
        DBSPFold result = new DBSPFold(fold.getNode(), fold.semigroup, fold.zero, increment, fold.postProcess);
        this.map(fold, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(MinMaxAggregate aggregate) {
        DBSPClosureExpression increment = this.convert(aggregate.increment);
        // Make a new visitor just for the aggregated value; that one may not call
        // any functions on the aggregated values, so we force it to unintern it (uninternEverything = true)
        InternInner always = new InternInner(this.compiler, false, true, this.parameterTypes);
        DBSPClosureExpression aggregatedValue = always.convert(aggregate.comparedValue);
        MinMaxAggregate result = new MinMaxAggregate(aggregate.getNode(), aggregate.zero,
                increment, aggregate.emptySetResult, aggregate.semigroup,
                aggregatedValue, aggregate.postProcess, aggregate.operation);
        this.save(aggregate, result);
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPAggregateList list) {
        this.restore();
        DBSPVariablePath rowVar = new DBSPVariablePath(list.rowVar.variable, this.parameterTypes[0]);
        List<IAggregate> implementations =
                Linq.map(list.aggregates, c -> {
                    IDBSPInnerNode result = this.getE(c);
                    return result.to(IAggregate.class);
                });
        DBSPAggregateList result = new DBSPAggregateList(
                list.getNode(), rowVar.to(DBSPVariablePath.class), implementations);
        this.set(list, result);
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        if (this.context.isEmpty()) {
            // outermost closure
            ResolveReferences resolveReferences = new ResolveReferences(this.compiler, false);
            resolveReferences.apply(expression);
            this.refMap = resolveReferences.reference;
            Utilities.enforce(expression.parameters.length == this.parameterTypes.length,
                    () -> "Parameter count " + this.parameterTypes.length +
                            " does not match closure parameter count " + expression.parameters.length);
            for (int i = 0; i < expression.parameters.length; i++) {
                DBSPType type = this.parameterTypes[i];
                if (type.sameType(expression.parameters[i].getType()))
                    continue;
                Utilities.putNew(this.outerParameters, expression.parameters[i], this.parameterTypes[i]);
            }
        }
        return super.preorder(expression);
    }

    @Override
    public void postorder(DBSPClosureExpression expression) {
        DBSPExpression body = this.getE(expression.body);
        DBSPParameter[] parameters = Linq.map(
                expression.parameters, p -> this.get(p).to(DBSPParameter.class), DBSPParameter.class);
        this.map(expression, new DBSPClosureExpression(expression.getNode(), body, parameters));
    }

    @Override
    public void postorder(DBSPParameter parameter) {
        if (this.outerParameters.containsKey(parameter)) {
            DBSPType type = Utilities.getExists(this.outerParameters, parameter);
            DBSPParameter newParam = new DBSPParameter(parameter.name, type);
            this.set(parameter, newParam);
        } else {
            this.set(parameter, parameter);
        }
    }

    public static DBSPExpression callUnintern(DBSPExpression interned, DBSPType originalType) {
        return new DBSPApplyExpression(
                interned.getNode(), uninternFunction, InternInner.nullableString, interned)
                .cast(interned.getNode(), originalType, false);
    }

    public DBSPExpression callUninternRecursive(DBSPExpression interned, DBSPType originalType) {
        // This is similar to ExpressionCompiler.expandTuple
        DBSPType internedType = interned.getType();
        boolean needsRewrite = RewriteInternedFields.HasInternedTypes.check(this.compiler, internedType);
        if (!needsRewrite)
            return interned;
        CalciteObject node = interned.getNode();
        switch (internedType.code) {
            case INTERNED_STRING: return callUnintern(interned, originalType);
            case TUPLE, RAW_TUPLE: {
                Utilities.enforce(internedType.code == originalType.code);
                DBSPTypeTupleBase tuple = originalType.to(DBSPTypeTupleBase.class);
                DBSPExpression[] fields = new DBSPExpression[tuple.size()];
                DBSPExpression safeSource = interned.unwrapIfNullable();
                for (int i = 0; i < tuple.size(); i++) {
                    fields[i] = callUninternRecursive(safeSource.field(i).simplify(), tuple.getFieldType(i));
                }
                DBSPExpression convertedTuple;
                if (originalType.code == RAW_TUPLE) {
                    convertedTuple = new DBSPRawTupleExpression(node, originalType.to(DBSPTypeRawTuple.class), fields);
                } else {
                    convertedTuple = new DBSPTupleExpression(node, originalType.to(DBSPTypeTuple.class), fields);
                }
                if (originalType.mayBeNull) {
                    if (!interned.getType().mayBeNull) {
                        return convertedTuple;
                    } else {
                        DBSPExpression condition = interned.is_null();
                        DBSPExpression positive = originalType.none();
                        return new DBSPIfExpression(node, condition, positive, convertedTuple);
                    }
                } else {
                    // This will panic at runtime if the source tuple is null
                    return convertedTuple;
                }
            }
            default: return interned;
        }
    }

    public static DBSPExpression callIntern(DBSPExpression argument) {
        argument = argument.cast(argument.getNode(), nullableString, false);
        return new DBSPApplyExpression(internFunction, DBSPTypeInterned.INSTANCE, argument);
    }

    DBSPExpression uninternIfNecessary(DBSPExpression original) {
        DBSPExpression replacement = this.getE(original);
        DBSPType originalType = original.getType();
        return this.callUninternRecursive(replacement, originalType);
    }

    DBSPExpression[] uninternIfNecessary(DBSPExpression[] original) {
        return Linq.map(original, this::uninternIfNecessary, DBSPExpression.class);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        DBSPExpression function = this.getE(expression.function);
        DBSPExpression[] newArgs = this.uninternIfNecessary(expression.arguments);
        DBSPExpression result = new DBSPApplyExpression(function, expression.getType(), newArgs);
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPApplyMethodExpression expression) {
        DBSPExpression function = this.getE(expression.function);
        DBSPExpression self = this.getE(expression.self);
        DBSPExpression[] newArgs = this.uninternIfNecessary(expression.arguments);
        DBSPExpression result = new DBSPApplyMethodExpression(function, expression.getType(), self, newArgs);
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPAssignmentExpression expression) {
        DBSPExpression left = this.getE(expression.left);
        DBSPExpression result = new DBSPAssignmentExpression(left, this.uninternIfNecessary(expression.right));
        this.map(expression, result);
    }

    Pair<DBSPExpression, DBSPExpression> uninternBothOrNone(
            DBSPExpression left, DBSPExpression right,
            DBSPType originalLeftType, DBSPType originalRightType) {
        DBSPTypeCode lCode = left.getType().code;
        DBSPTypeCode rCode = right.getType().code;
        if (lCode == DBSPTypeCode.TUPLE || lCode == DBSPTypeCode.RAW_TUPLE) {
            Utilities.enforce(lCode == rCode);
            DBSPTypeTupleBase lTuple = originalLeftType.to(DBSPTypeTupleBase.class);
            DBSPTypeTupleBase rTuple = originalRightType.to(DBSPTypeTupleBase.class);
            Utilities.enforce(lTuple.size() == rTuple.size());
            DBSPExpression[] lExpr = new DBSPExpression[lTuple.size()];
            DBSPExpression[] rExpr = new DBSPExpression[lTuple.size()];
            for (int i = 0; i < lTuple.size(); i++) {
                var fields = this.uninternBothOrNone(
                        left.field(i).simplify(), right.field(i).simplify(),
                        lTuple.getFieldType(i), rTuple.getFieldType(i));
                lExpr[i] = fields.left.applyCloneIfNeeded();
                rExpr[i] = fields.right.applyCloneIfNeeded();
            }
            return Pair.of(lTuple.makeTuple(lExpr), lTuple.makeTuple(rExpr));
        }
        if (lCode == DBSPTypeCode.INTERNED_STRING && rCode != DBSPTypeCode.INTERNED_STRING) {
            left = callUnintern(left, originalLeftType);
        }
        if (rCode == DBSPTypeCode.INTERNED_STRING && lCode != DBSPTypeCode.INTERNED_STRING) {
            right = callUnintern(right, originalRightType);
        }
        return Pair.of(left, right);
    }

    @Override
    public void postorder(DBSPFieldExpression expression) {
        if (!this.uninternEverything) {
            super.postorder(expression);
            return;
        }
        DBSPExpression result = this.getE(expression.expression).field(expression.fieldNo);
        if (result.getType().code == DBSPTypeCode.INTERNED_STRING) {
            result = callUnintern(result.applyClone(), expression.getType());
        }
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPBinaryExpression expression) {
        switch (expression.opcode) {
            case LT, GT, LTE, GTE, MAX, MIN, CONCAT, SQL_INDEX, MAP_INDEX, AGG_MAX,
                 AGG_MIN, AGG_MAX1, AGG_MIN1, AGG_GTE, AGG_LTE, CONTROLLED_FILTER_GTE: {
                DBSPExpression left = this.uninternIfNecessary(expression.left);
                DBSPExpression right = this.uninternIfNecessary(expression.right);
                DBSPExpression result = expression.replaceSources(left, right);
                this.map(expression, result);
                return;
            }
            case EQ, NEQ, IS_DISTINCT: {
                // Do not replace if both are interned
                Pair<DBSPExpression, DBSPExpression> pair = this.uninternBothOrNone(
                        this.getE(expression.left), this.getE(expression.right),
                        expression.left.getType(), expression.right.getType());
                boolean isNull = pair.left.getType().mayBeNull || pair.right.getType().mayBeNull;
                DBSPExpression result = new DBSPBinaryExpression(
                        expression.getNode(), expression.getType().withMayBeNull(isNull),
                        expression.opcode, pair.left, pair.right);
                if (!expression.getType().mayBeNull)
                    result = result.wrapBoolIfNeeded();
                this.map(expression, result);
                return;
            }
            default:
                break;
        }
        super.postorder(expression);
    }

    @Override
    public void postorder(DBSPBorrowExpression expression) {
        DBSPExpression source = this.uninternIfNecessary(expression.expression);
        DBSPExpression result = source.borrow(expression.mut);
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        DBSPExpression newSource = this.getE(expression.source);
        if (newSource.getType().code == DBSPTypeCode.INTERNED_STRING &&
                expression.getType().sameTypeIgnoringNullability(expression.source.getType())) {
            // Special handling for casts that just change nullability
            // (add or remove); both become no-ops after interning
            this.map(expression, newSource);
        } else {
            DBSPExpression source = this.uninternIfNecessary(expression.source);
            DBSPExpression result = source.cast(expression.getNode(), expression.type, expression.safe);
            this.map(expression, result);
        }
    }

    @Override
    public void postorder(DBSPCloneExpression expression) {
        DBSPExpression source = this.getE(expression.expression);
        if (source.getType().sameType(DBSPTypeInterned.INSTANCE)) {
            this.map(expression, source.applyCloneIfNeeded());
        } else {
            super.postorder(expression);
        }
    }

    @Override
    public void postorder(DBSPConditionalIncrementExpression expression) {
        DBSPExpression left = this.uninternIfNecessary(expression.left);
        DBSPExpression right = this.uninternIfNecessary(expression.right);
        DBSPExpression condition = null;
        if (expression.condition != null)
            condition = this.uninternIfNecessary(expression.condition);
        DBSPExpression result = new DBSPConditionalIncrementExpression(expression.getNode(), expression.opcode,
                expression.getType(), left, right, condition);
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPConstructorExpression expression) {
        DBSPExpression function = this.uninternIfNecessary(expression.function);
        DBSPExpression[] args = this.uninternIfNecessary(expression.arguments);
        DBSPExpression result = new DBSPConstructorExpression(function, expression.getType(), args);
        this.map(expression, result);
    }

    @Override
    public void postorder(DBSPIfExpression expression) {
        if (expression.negative == null) {
            super.postorder(expression);
            return;
        }
        DBSPExpression condition = this.getE(expression.condition);
        Pair<DBSPExpression, DBSPExpression> pair = this.uninternBothOrNone(
                this.getE(expression.positive), this.getE(expression.negative),
                expression.positive.getType(), expression.negative.getType());
        DBSPExpression result = new DBSPIfExpression(expression.getNode(), condition, pair.left, pair.right);
        this.map(expression, result);
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression expression) {
        // This needs to be done preorder
        expression.initializer.accept(this);
        DBSPExpression initializer = this.getE(expression.initializer);
        DBSPVariablePath var = expression.variable;
        if (!initializer.getType().sameType(expression.variable.getType()))
            var = new DBSPVariablePath(expression.variable.variable, initializer.getType());
        this.map(expression.variable, var);
        expression.consumer.accept(this);
        DBSPExpression consumer = this.getE(expression.consumer);
        DBSPLetExpression result = new DBSPLetExpression(var, initializer, consumer);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPRawTupleExpression expression) {
        if (expression.fields != null) {
            DBSPExpression[] fields = this.get(expression.fields);
            DBSPExpression result;
            if (expression.getType().mayBeNull) {
                DBSPType[] types = Linq.map(fields, DBSPExpression::getType, DBSPType.class);
                DBSPTypeRawTuple type = new DBSPTypeRawTuple(types).withMayBeNull(true).to(DBSPTypeRawTuple.class);
                result = new DBSPRawTupleExpression(expression.getNode(), type, fields);
            } else {
                result = new DBSPRawTupleExpression(fields);
            }
            this.map(expression, result);
        } else {
            this.map(expression, expression.getType().none());
        }
    }

    @Override
    public void postorder(DBSPStringLiteral literal) {
        if (this.internConstants) {
            this.globalStrings.add(literal);
            DBSPExpression result = callIntern(literal);
            this.map(literal, result);
        } else {
            super.postorder(literal);
        }
    }

    @Override
    public void postorder(DBSPTupleExpression expression) {
        if (expression.fields != null) {
            DBSPExpression[] fields = this.get(expression.fields);
            DBSPExpression result = new DBSPTupleExpression(expression.getType().mayBeNull, fields);
            this.map(expression, result);
        } else {
            this.map(expression, expression.getType().none());
        }
    }

    @Override
    public void postorder(DBSPVariantExpression expression) {
        if (expression.isSqlNull || expression.value == null) {
            this.map(expression, expression);
            return;
        }
        DBSPExpression value = this.uninternIfNecessary(expression.value);
        this.map(expression, new DBSPVariantExpression(value, expression.type));
    }

    @Override
    public void postorder(DBSPVariablePath expression) {
        IDBSPDeclaration decl = Objects.requireNonNull(this.refMap).get(expression);
        Utilities.enforce(decl != null);
        DBSPExpression result = expression;
        DBSPType newType = null;
        if (decl.is(DBSPParameter.class)) {
            newType = this.outerParameters.get(decl.to(DBSPParameter.class));
        } else if (decl.is(DBSPLetExpression.class)) {
            DBSPLetExpression let = decl.to(DBSPLetExpression.class);
            DBSPVariablePath newVar = this.getE(let.variable).to(DBSPVariablePath.class);
            newType = newVar.getType();
        }
        if (newType != null && !newType.sameType(expression.getType()))
            result = new DBSPVariablePath(expression.variable, newType);
        this.map(expression, result);
    }
}