package org.dbsp.sqlCompiler.compiler.visitors.outer.intern;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalAggregateExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
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

/** Inner visitor that rewrites expressions by replacing strings with interned values */
public class InternInner extends ExpressionTranslator {
    public static final String uninternFunction = "unintern";
    public static final String internFunction = "intern";
    public static final DBSPType nullableString = DBSPTypeString.varchar(true);

    final DBSPType[] parameterTypes;
    final Map<DBSPParameter, DBSPType> outerParameters;
    @Nullable
    ReferenceMap refMap;
    final Set<DBSPStringLiteral> globalStrings;
    final boolean internConstants;

    public InternInner(DBSPCompiler compiler, boolean internConstants, DBSPType... parameterTypes) {
        super(compiler);
        if (parameterTypes.length == 0)
            throw new InternalCompilerError("No parameter types provided");
        this.parameterTypes = parameterTypes;
        this.internConstants = internConstants;
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
        this.clear();
        DBSPClosureExpression map = expression.ensureTree(this.compiler).to(DBSPClosureExpression.class);
        ResolveReferences resolveReferences = new ResolveReferences(this.compiler, false);
        resolveReferences.apply(map);
        this.refMap = resolveReferences.reference;
        map.accept(this);
        return this.get(map).to(DBSPClosureExpression.class);
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
        this.map(aggregate, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(NonLinearAggregate aggregate) {
        DBSPClosureExpression increment = this.convert(aggregate.increment);
        DBSPExpression result = new NonLinearAggregate(aggregate.getNode(),
                aggregate.zero, increment, aggregate.postProcess, aggregate.emptySetResult, aggregate.semigroup);
        this.map(aggregate, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(MinMaxAggregate aggregate) {
        DBSPClosureExpression increment = this.convert(aggregate.increment);
        DBSPClosureExpression aggregatedValue = this.convert(aggregate.aggregatedValue);
        MinMaxAggregate result = new MinMaxAggregate(aggregate.getNode(), aggregate.zero,
                increment, aggregate.emptySetResult, aggregate.semigroup,
                aggregatedValue, aggregate.isMin);
        this.map(aggregate, result);
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPAggregateList list) {
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
                    "Parameter type count does not match closure parameter count");
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
                interned.getNode(), uninternFunction, InternInner.nullableString, interned.applyCloneIfNeeded())
                .cast(interned.getNode(), originalType, false);
    }

    public static DBSPExpression callIntern(DBSPExpression argument) {
        argument = argument.cast(argument.getNode(), nullableString, false);
        return new DBSPApplyExpression(internFunction, DBSPTypeInterned.INSTANCE, argument);
    }

    DBSPExpression uninternIfNecessary(DBSPExpression original) {
        DBSPExpression replacement = this.getE(original);
        if (replacement.getType().code == DBSPTypeCode.INTERNED_STRING) {
            DBSPType originalType = original.getType();
            return callUnintern(replacement, originalType);
        }
        return replacement;
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
    public void postorder(DBSPBinaryExpression expression) {
        switch (expression.opcode) {
            case LT, GT, LTE, GTE, MAX, MIN, CONCAT, SQL_INDEX, MAP_INDEX, AGG_MAX,
                 AGG_MIN, AGG_GTE, AGG_LTE, CONTROLLED_FILTER_GTE: {
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
        DBSPExpression result = source.borrow();
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
    public void postorder(DBSPConditionalAggregateExpression expression) {
        DBSPExpression left = this.uninternIfNecessary(expression.left);
        DBSPExpression right = this.uninternIfNecessary(expression.right);
        DBSPExpression condition = null;
        if (expression.condition != null)
            condition = this.uninternIfNecessary(expression.condition);
        DBSPExpression result = new DBSPConditionalAggregateExpression(expression.getNode(), expression.opcode,
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
    public void postorder(DBSPLetExpression expression) {
        DBSPExpression initializer = this.getE(expression.initializer);
        if (initializer.getType().code == DBSPTypeCode.INTERNED_STRING) {
            DBSPVariablePath var = initializer.getType().var();
            DBSPExpression consumer = this.getE(expression.consumer);
            DBSPLetExpression result = new DBSPLetExpression(var, initializer, consumer);
            this.map(expression, result);
        } else {
            super.postorder(expression);
        }
    }

    @Override
    public void postorder(DBSPRawTupleExpression node) {
        if (node.fields != null) {
            DBSPExpression[] fields = this.get(node.fields);
            DBSPExpression result = new DBSPRawTupleExpression(fields);
            this.map(node, result);
        } else {
            this.map(node, node.getType().none());
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
    public void postorder(DBSPTupleExpression node) {
        if (node.fields != null) {
            DBSPExpression[] fields = this.get(node.fields);
            DBSPExpression result = new DBSPTupleExpression(fields);
            this.map(node, result);
        } else {
            this.map(node, node.getType().none());
        }
    }

    @Override
    public void postorder(DBSPVariantExpression expression) {
        if (expression.isSqlNull) {
            this.map(expression, expression);
            return;
        }
        Utilities.enforce(expression.value != null);
        DBSPExpression value = this.uninternIfNecessary(expression.value);
        this.map(expression, new DBSPVariantExpression(value, expression.type));
    }

    @Override
    public void postorder(DBSPVariablePath expression) {
        IDBSPDeclaration decl = Objects.requireNonNull(this.refMap).get(expression);
        Utilities.enforce(decl != null);
        DBSPExpression result = expression;
        if (decl.is(DBSPParameter.class)) {
            DBSPType newType = this.outerParameters.get(decl.to(DBSPParameter.class));
            if (newType != null)
                result = new DBSPVariablePath(expression.variable, newType);
        }
        this.map(expression, result);
    }
}