package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPKeywordLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPRealLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantNullLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeRuntimeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeBTreeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPComparatorType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeLazy;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeOption;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSemigroup;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for Inner visitors which rewrite expressions, types, and statements.
 * This class recurses over the structure of expressions, types, and statements
 * and if any fields have changed builds a new version of the object.  Classes
 * that extend this should override the preorder methods and ignore the postorder
 * methods. */
public abstract class InnerRewriteVisitor
        extends InnerVisitor
        implements IWritesLogs {
    protected final boolean force;

    protected InnerRewriteVisitor(DBSPCompiler compiler, boolean force) {
        super(compiler);
        this.force = force;
    }

    /** Result produced by the last preorder invocation. */
    @Nullable
    protected IDBSPInnerNode lastResult;

    IDBSPInnerNode getResult() {
        return Objects.requireNonNull(this.lastResult);
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        this.startVisit(node);
        node.accept(this);
        this.endVisit();
        return this.getResult();
    }

    /**
     * Replace the 'old' IR node with the 'newOp' IR node if
     * any of its fields differs. */
    protected void map(IDBSPInnerNode old, IDBSPInnerNode newOp) {
        // noinspection ConstantValue
        Utilities.enforce(newOp != null);
        if ((old == newOp) || (!this.force && old.sameFields(newOp))) {
            // Ignore new op.
            this.lastResult = old;
            return;
        }

        Logger.INSTANCE.belowLevel(this, 1)
                .appendSupplier(this::toString)
                .append(":")
                .appendSupplier(old::toString)
                .append(" -> ")
                .appendSupplier(newOp::toString)
                .newline();
        this.lastResult = newOp;
    }

    protected void map(DBSPType type, DBSPType newType) {
        // Specialized implementation of 'map' for types
        if (type == newType ||
                type.sameFields(newType) ||
                type.sameType(newType)) {
            newType = type;
        }
        this.map(type.to(IDBSPInnerNode.class), newType.to(IDBSPInnerNode.class));
    }

    @Override
    public VisitDecision preorder(IDBSPInnerNode node) {
        this.map(node, node);
        return VisitDecision.STOP;
    }

    protected DBSPExpression getResultExpression() {
        IDBSPInnerNode result = this.getResult();
        return result.to(DBSPExpression.class);
    }

    protected DBSPType getResultType() { return this.getResult().to(DBSPType.class); }

    @Nullable
    protected DBSPExpression transformN(@Nullable DBSPExpression expression) {
        if (expression == null)
            return null;
        return this.transform(expression);
    }

    protected DBSPExpression transform(DBSPExpression expression) {
        expression.accept(this);
        return this.getResultExpression();
    }

    protected DBSPExpression[] transform(DBSPExpression[] expressions) {
        return Linq.map(expressions, this::transform, DBSPExpression.class);
    }

    protected DBSPStatement transform(DBSPStatement statement) {
        statement.accept(this);
        return this.getResult().to(DBSPStatement.class);
    }

    protected DBSPType transform(DBSPType type) {
        type.accept(this);
        return this.getResultType();
    }

    protected DBSPType[] transform(DBSPType[] expressions) {
        return Linq.map(expressions, this::transform, DBSPType.class);
    }

    /////////////////////// Types ////////////////////////////////

    @Override
    public VisitDecision preorder(DBSPTypeAny type) {
        this.map(type, type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeBaseType type) {
        this.map(type, type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeFunction type) {
        this.push(type);
        DBSPType resultType = this.transform(type.resultType);
        DBSPType[] argTypes = this.transform(type.parameterTypes);
        this.pop(type);
        DBSPType result = new DBSPTypeFunction(resultType, argTypes);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeIndexedZSet type) {
        this.push(type);
        DBSPType keyType = this.transform(type.keyType);
        DBSPType elementType = this.transform(type.elementType);
        this.pop(type);
        DBSPType result = new DBSPTypeIndexedZSet(type.getNode(), keyType, elementType);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRawTuple type) {
        this.push(type);
        DBSPType[] elements = this.transform(type.tupFields);
        this.pop(type);
        DBSPType result = new DBSPTypeRawTuple(elements);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeTuple type) {
        this.push(type);
        DBSPType[] elements = this.transform(type.tupFields);
        this.pop(type);
        DBSPType result = new DBSPTypeTuple(type.getNode(), type.mayBeNull, type.originalStruct, elements);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRef type) {
        this.push(type);
        DBSPType field = this.transform(type.type);
        this.pop(type);
        DBSPType result = new DBSPTypeRef(field, type.mutable, type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeVariant type) {
        this.push(type);
        this.pop(type);
        DBSPType result = DBSPTypeVariant.create(type.getNode(), type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeUser type) {
        this.push(type);
        DBSPType[] args = this.transform(type.typeArgs);
        this.pop(type);
        DBSPType result = new DBSPTypeUser(type.getNode(), type.code, type.name, type.mayBeNull, args);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeSemigroup type) {
        this.push(type);
        DBSPType[] args = this.transform(type.typeArgs);
        this.pop(type);
        Utilities.enforce(args.length % 2 == 0);
        DBSPType[] semigroupTypes = new DBSPType[args.length / 2];
        DBSPType[] elementTypes = new DBSPType[args.length / 2];
        for (int i = 0; i < args.length / 2; i++) {
            semigroupTypes[i] = args[i];
            elementTypes[i] = args[i  + args.length / 2];
        }
        DBSPType result = new DBSPTypeSemigroup(semigroupTypes, elementTypes);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeOption type) {
        this.push(type);
        Utilities.enforce(type.typeArgs.length == 1);
        DBSPType arg = this.transform(type.typeArgs[0]);
        this.pop(type);
        DBSPType result = new DBSPTypeOption(arg);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeMonthsInterval type) {
        this.push(type);
        this.pop(type);
        DBSPType result = new DBSPTypeMonthsInterval(type.getNode(), type.units, type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeMillisInterval type) {
        this.push(type);
        this.pop(type);
        DBSPType result = new DBSPTypeMillisInterval(type.getNode(), type.units, type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeDecimal type) {
        this.push(type);
        this.pop(type);
        DBSPType result = new DBSPTypeDecimal(type.getNode(), type.getPrecision(), type.scale, type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRuntimeDecimal type) {
        this.push(type);
        this.pop(type);
        DBSPType result = new DBSPTypeRuntimeDecimal(type.getNode(), type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStream type) {
        this.push(type);
        DBSPType elementType = this.transform(type.elementType);
        this.pop(type);
        DBSPType result = new DBSPTypeStream(elementType, type.outerCircuit);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct.Field field) {
        this.push(field);
        DBSPType type = this.transform(field.type);
        DBSPTypeStruct.Field result = new DBSPTypeStruct.Field(
                field.getNode(), field.name, field.index, type);
        this.pop(field);
        this.map(field, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct type) {
        this.push(type);
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        for (DBSPTypeStruct.Field f: type.fields.values()) {
            f.accept(this);
            DBSPTypeStruct.Field field = this.getResult().to(DBSPTypeStruct.Field.class);
            fields.add(field);
        }
        this.pop(type);
        DBSPType result = new DBSPTypeStruct(type.getNode(), type.name, type.sanitizedName, fields, type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeArray type) {
        this.push(type);
        DBSPType elementType = this.transform(type.getElementType());
        this.pop(type);
        DBSPType result = new DBSPTypeArray(elementType, type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeLazy type) {
        this.push(type);
        DBSPType elementType = this.transform(type.typeArgs[0]);
        DBSPType result = new DBSPTypeLazy(elementType);
        this.pop(type);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeVec type) {
        this.push(type);
        DBSPType elementType = this.transform(type.getElementType());
        this.pop(type);
        DBSPType result = new DBSPTypeVec(elementType, type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPComparatorType type) {
        this.map(type, type);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeMap type) {
        this.push(type);
        DBSPType keyType = this.transform(type.getKeyType());
        DBSPType valueType = this.transform(type.getValueType());
        this.pop(type);
        DBSPType result = new DBSPTypeMap(keyType, valueType, type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeBTreeMap type) {
        this.push(type);
        DBSPType keyType = this.transform(type.getKeyType());
        DBSPType valueType = this.transform(type.getValueType());
        this.pop(type);
        DBSPType result = new DBSPTypeBTreeMap(keyType, valueType, type.mayBeNull);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeWithCustomOrd type) {
        this.push(type);
        DBSPType keyType = this.transform(type.getDataType());
        DBSPType comparatorType = this.transform(type.getComparatorType());
        this.pop(type);
        DBSPType result = new DBSPTypeWithCustomOrd(type.getNode(), keyType, comparatorType);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeZSet type) {
        this.push(type);
        DBSPType elementType = this.transform(type.elementType);
        this.pop(type);
        DBSPType result = new DBSPTypeZSet(type.getNode(), elementType);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    /////////////////////// Expressions //////////////////////////

    @Override
    public VisitDecision preorder(DBSPBoolLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPBoolLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDateLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPDateLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDecimalLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPDecimalLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDoubleLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPDoubleLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRealLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPRealLiteral(
                expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariantExpression expression) {
        this.push(expression);
        DBSPExpression value = this.transformN(expression.value);
        this.pop(expression);
        DBSPExpression result;
        if (expression.isSqlNull) {
            result = DBSPVariantExpression.sqlNull(expression.getType().mayBeNull);
        } else {
            result = new DBSPVariantExpression(value, expression.type.mayBeNull);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariantNullLiteral expression) {
        this.push(expression);
        this.pop(expression);
        this.map(expression, expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPGeoPointConstructor expression) {
        this.push(expression);
        @Nullable DBSPExpression left = this.transformN(expression.left);
        @Nullable DBSPExpression right = this.transformN(expression.right);
        DBSPType type = this.transform(expression.type);
        this.pop(expression);
        DBSPExpression result = new DBSPGeoPointConstructor(expression.getNode(), left, right, type);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI16Literal expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPI16Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI32Literal expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPI32Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI64Literal expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPI64Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI128Literal expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPI128Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntervalMillisLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPIntervalMillisLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntervalMonthsLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPIntervalMonthsLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPISizeLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPISizeLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPKeywordLiteral expression) {
        this.map(expression, expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPNullLiteral expression) {
        this.push(expression);
        this.pop(expression);
        DBSPExpression result = DBSPNullLiteral.INSTANCE;
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPStringLiteral(
                expression.getNode(), type, expression.value, expression.charset);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPBinaryLiteral(
                expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStrLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPStrLiteral(expression.getNode(), type, expression.value, expression.raw);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimeLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPTimeLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimestampLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPTimestampLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU8Literal expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPU8Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU16Literal expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPU16Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU32Literal expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPU32Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU64Literal expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPU64Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU128Literal expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPU128Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUSizeLiteral expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPUSizeLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPArrayExpression expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        List<DBSPExpression> data = null;
        if (expression.data != null)
            data = Linq.map(expression.data, this::transform);
        this.pop(expression);
        DBSPExpression result = new DBSPArrayExpression(expression.getNode(), type, data);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPMapExpression expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        List<DBSPExpression> keys = null;
        List<DBSPExpression> values = null;
        if (expression.keys != null) {
            keys = Linq.map(expression.keys, this::transform);
            Utilities.enforce(expression.values != null);
            values = Linq.map(expression.values, this::transform);
        }
        this.pop(expression);
        DBSPExpression result = new DBSPMapExpression(type.to(DBSPTypeMap.class), keys, values);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPZSetExpression expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        DBSPTypeZSet zType = type.to(DBSPTypeZSet.class);
        DBSPZSetExpression result =
                DBSPZSetExpression.emptyWithElementType(zType.getElementType());
        for (Map.Entry<DBSPExpression, Long> entry: expression.data.entrySet()) {
            DBSPExpression row = this.transform(entry.getKey());
            result.append(row, entry.getValue());
        }
        this.pop(expression);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFlatmap expression) {
        this.push(expression);
        DBSPTypeTuple inputElementType = this.transform(expression.inputRowType).to(DBSPTypeTuple.class);
        DBSPType indexType = null;
        if (expression.ordinalityIndexType != null)
            indexType = this.transform(expression.ordinalityIndexType);
        DBSPClosureExpression collectionExpression = this.transform(expression.collectionExpression)
                .to(DBSPClosureExpression.class);
        List<DBSPClosureExpression> rightProjections = null;
        if (expression.rightProjections != null)
            rightProjections = Linq.map(expression.rightProjections,
                    e -> this.transform(e).to(DBSPClosureExpression.class));
        this.pop(expression);
        DBSPExpression result = new DBSPFlatmap(expression.getNode(),
                inputElementType, collectionExpression,
                expression.leftInputIndexes, rightProjections, indexType, expression.shuffle);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPQuestionExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        this.pop(expression);
        DBSPExpression result = source.question();
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
        this.push(expression);
        DBSPExpression[] arguments = this.transform(expression.arguments);
        DBSPExpression function = this.transform(expression.function);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPApplyExpression(function, type, arguments);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyMethodExpression expression) {
        this.push(expression);
        DBSPExpression[] arguments = this.transform(expression.arguments);
        DBSPExpression function = this.transform(expression.function);
        DBSPExpression self = this.transform(expression.self);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPApplyMethodExpression(function, type, self, arguments);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAssignmentExpression expression) {
        this.push(expression);
        DBSPExpression left = this.transform(expression.left);
        DBSPExpression right = this.transform(expression.right);
        this.pop(expression);
        DBSPExpression result = new DBSPAssignmentExpression(left, right);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryExpression expression) {
        this.push(expression);
        DBSPExpression left = this.transform(expression.left);
        DBSPExpression right = this.transform(expression.right);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPBinaryExpression(expression.getNode(), type,
                    expression.opcode, left, right);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression expression) {
        this.push(expression);
        List<DBSPStatement> body = Linq.map(expression.contents, this::transform);
        DBSPExpression last = this.transformN(expression.lastExpression);
        this.pop(expression);
        DBSPExpression result = new DBSPBlockExpression(body, last);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBorrowExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = source.borrow(expression.mut);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLazyCellExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = new DBSPLazyCellExpression(source);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSomeExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = new DBSPSomeExpression(expression.getNode(), source);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = source.cast(expression.getNode(), type, expression.safe);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIsNullExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = source.is_null();
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        this.push(expression);
        DBSPParameter[] parameters = Linq.map(
                expression.parameters, p -> {
                    p.accept(this);
                    return this.getResult().to(DBSPParameter.class);
                }, DBSPParameter.class);
        DBSPExpression body = this.transform(expression.body);
        this.pop(expression);
        DBSPExpression result = body.closure(parameters);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldComparatorExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        this.pop(expression);
        DBSPExpression result = new DBSPFieldComparatorExpression(
                    expression.getNode(), source.to(DBSPComparatorExpression.class),
                    expression.fieldNo, expression.ascending, expression.nullsFirst);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPEqualityComparatorExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.comparator);
        this.pop(expression);
        DBSPExpression result = new DBSPEqualityComparatorExpression(
                expression.getNode(), source.to(DBSPComparatorExpression.class));
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDirectComparatorExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        this.pop(expression);
        DBSPExpression result = new DBSPDirectComparatorExpression(
                expression.getNode(), source.to(DBSPComparatorExpression.class),
                expression.ascending);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPNoComparatorExpression expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.tupleType);
        this.pop(expression);
        DBSPExpression result = new DBSPNoComparatorExpression(expression.getNode(), type);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCustomOrdExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        DBSPExpression comparator = this.transform(expression.comparator);
        this.pop(expression);
        DBSPExpression result = new DBSPCustomOrdExpression(expression.getNode(), source, comparator);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnwrapCustomOrdExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = new DBSPUnwrapCustomOrdExpression(source);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDerefExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = new DBSPDerefExpression(source);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression expression) {
        this.push(expression);
        DBSPExpression initializer = this.transform(expression.initializer);
        DBSPExpression consumer = this.transform(expression.consumer);
        this.pop(expression);
        DBSPExpression result = new DBSPLetExpression(expression.variable, initializer, consumer);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCustomOrdField expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = new DBSPCustomOrdField(source, expression.fieldNo);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnwrapExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = new DBSPUnwrapExpression(source);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPEnumValue expression) {
        this.map(expression, expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = source.field(expression.fieldNo);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnsignedWrapExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        this.pop(expression);
        DBSPExpression result = new DBSPUnsignedWrapExpression(
                expression.getNode(), source, expression.ascending, expression.nullsLast);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnsignedUnwrapExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        this.pop(expression);
        DBSPExpression result = new DBSPUnsignedUnwrapExpression(
                expression.getNode(), source,
                expression.getType(), expression.ascending, expression.nullsLast);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPForExpression expression) {
        this.push(expression);
        DBSPExpression iterated = this.transform(expression.iterated);
        DBSPExpression body = this.transform(expression.block);
        DBSPBlockExpression block;
        if (body.is(DBSPBlockExpression.class))
            block = body.to(DBSPBlockExpression.class);
        else
            block = new DBSPBlockExpression(Linq.list(), body);
        this.pop(expression);
        DBSPExpression result = new DBSPForExpression(expression.variable, iterated, block);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConditionalAggregateExpression expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.type);
        DBSPExpression left = this.transform(expression.left);
        DBSPExpression right = this.transform(expression.right);
        DBSPExpression cond = this.transformN(expression.condition);
        this.pop(expression);
        DBSPExpression result = new DBSPConditionalAggregateExpression(
                expression.getNode(), expression.opcode, type, left, right, cond);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIfExpression expression) {
        this.push(expression);
        DBSPExpression cond = this.transform(expression.condition);
        DBSPExpression positive = this.transform(expression.positive);
        DBSPExpression negative = this.transformN(expression.negative);
        this.pop(expression);
        DBSPExpression result = new DBSPIfExpression(expression.getNode(), cond, positive, negative);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPathExpression expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPPathExpression(type, expression.path);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPQualifyTypeExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        DBSPType[] types = this.transform(expression.types);
        this.pop(expression);
        DBSPExpression result = new DBSPQualifyTypeExpression(source, types);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRawTupleExpression expression) {
        this.push(expression);
        DBSPExpression[] fields = null;
        if (expression.fields != null)
            fields = this.transform(expression.fields);
        this.pop(expression);
        DBSPExpression result;
        if (fields != null)
            result = new DBSPRawTupleExpression(fields);
        else
            result = new DBSPRawTupleExpression(expression.getType().to(DBSPTypeRawTuple.class));
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSortExpression expression) {
        this.push(expression);
        DBSPExpression comparator = this.transform(expression.comparator);
        DBSPType elementType = this.transform(expression.elementType);
        @Nullable DBSPExpression limit = this.transformN(expression.limit);
        this.pop(expression);
        DBSPExpression result = new DBSPSortExpression(expression.getNode(), elementType, comparator, limit);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstructorExpression expression) {
        this.push(expression);
        DBSPExpression function = this.transform(expression.function);
        DBSPExpression[] arguments = this.transform(expression.arguments);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPConstructorExpression(function, type, arguments);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression expression) {
        DBSPExpression[] fields = null;
        this.push(expression);
        if (expression.fields != null)
            fields = this.transform(expression.fields);
        this.pop(expression);
        DBSPExpression result;
        if (fields == null)
            result = expression.getType().none();
        else {
            DBSPTypeTuple originalType = expression.getTypeAsTuple();
            DBSPTypeTuple type = new DBSPTypeTuple(
                    originalType.getNode(), originalType.mayBeNull, originalType.originalStruct,
                    Linq.map(fields, DBSPExpression::getType, DBSPType.class));
            if (type.sameType(originalType))
                type = originalType;
            result = new DBSPTupleExpression(expression.getNode(), type, fields);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnaryExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.source);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPUnaryExpression(expression.getNode(), type,
                    expression.opcode, source);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStaticExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.initializer);
        this.pop(expression);
        DBSPExpression result = new DBSPStaticExpression(expression.getNode(), source);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPReturnExpression expression) {
        this.push(expression);
        DBSPExpression argument = this.transform(expression.argument);
        this.pop(expression);
        DBSPExpression result = new DBSPReturnExpression(expression.getNode(), argument);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPWindowBoundExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.representation);
        this.pop(expression);
        DBSPExpression result = new DBSPWindowBoundExpression(expression.getNode(),
                expression.isPreceding, source);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        DBSPExpression result = new DBSPCloneExpression(expression.getNode(), source);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath expression) {
        this.push(expression);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);
        DBSPExpression result = new DBSPVariablePath(expression.variable, type);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    /////////////////// statements

    @Override
    public VisitDecision preorder(DBSPExpressionStatement statement) {
        this.push(statement);
        DBSPExpression expression = this.transform(statement.expression);
        this.pop(statement);
        DBSPStatement result = expression.toStatement();
        this.map(statement, result);
        return VisitDecision.STOP;
    }

    public VisitDecision preorder(DBSPLetStatement statement) {
        this.push(statement);
        DBSPExpression init = this.transformN(statement.initializer);
        DBSPType type = this.transform(statement.type);
        this.pop(statement);
        DBSPStatement result;
        if (init != null)
            result = new DBSPLetStatement(statement.variable, init);
        else
            result = new DBSPLetStatement(statement.variable, type, statement.mutable);
        this.map(statement, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPComment comment) {
        this.map(comment, comment);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFunctionItem item) {
        this.push(item);
        item.function.accept(this);
        DBSPFunction function = this.getResult().to(DBSPFunction.class);
        this.pop(item);
        DBSPFunctionItem result = new DBSPFunctionItem(function);
        this.map(item, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStructItem item) {
        this.push(item);
        DBSPType type = this.transform(item.type);
        this.pop(item);
        DBSPItem result = new DBSPStructItem(type.to(DBSPTypeStruct.class), item.metadata);
        this.map(item, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStaticItem item) {
        this.push(item);
        DBSPExpression expression = this.transform(item.expression);
        this.pop(item);
        DBSPItem result = new DBSPStaticItem(expression.to(DBSPStaticExpression.class));
        this.map(item, result);
        return VisitDecision.STOP;
    }

    /// Other objects

    @Override
    public VisitDecision preorder(DBSPParameter parameter) {
        this.push(parameter);
        DBSPType type = this.transform(parameter.type);
        this.pop(parameter);
        DBSPParameter result = new DBSPParameter(parameter.name, type);
        this.map(parameter, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFunction function) {
        this.push(function);
        DBSPType returnType = this.transform(function.returnType);
        DBSPExpression body = this.transformN(function.body);
        List<DBSPParameter> parameters =
                Linq.map(function.parameters, p -> {
                    p.accept(this);
                    return Objects.requireNonNull(this.lastResult).to(DBSPParameter.class);
                });
        this.pop(function);
        DBSPFunction result = new DBSPFunction(function.getNode(),
                function.name, parameters, returnType, body, function.annotations);
        this.map(function, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(NonLinearAggregate implementation) {
        this.push(implementation);
        DBSPExpression zero = this.transform(implementation.zero);
        DBSPExpression increment = this.transform(implementation.increment);
        @Nullable DBSPExpression postProcess = this.transformN(implementation.postProcess);
        DBSPExpression emptySetResult = this.transform(implementation.emptySetResult);
        DBSPTypeUser semiGroup = this.transform(implementation.semigroup).to(DBSPTypeUser.class);
        this.pop(implementation);

        NonLinearAggregate result = new NonLinearAggregate(
                implementation.getNode(), zero,
                increment.to(DBSPClosureExpression.class),
                postProcess != null ? postProcess.to(DBSPClosureExpression.class) : null,
                emptySetResult, semiGroup);
        result.validate();
        this.map(implementation, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(MinMaxAggregate implementation) {
        this.push(implementation);
        DBSPExpression zero = this.transform(implementation.zero);
        DBSPExpression increment = this.transform(implementation.increment);
        DBSPExpression emptySetResult = this.transform(implementation.emptySetResult);
        DBSPExpression aggregatedValue = this.transform(implementation.aggregatedValue);
        DBSPTypeUser semiGroup = this.transform(implementation.semigroup).to(DBSPTypeUser.class);
        this.pop(implementation);

        NonLinearAggregate result = new MinMaxAggregate(
                implementation.getNode(), zero,
                increment.to(DBSPClosureExpression.class),
                emptySetResult, semiGroup, aggregatedValue.to(DBSPClosureExpression.class),
                implementation.isMin);
        result.validate();
        this.map(implementation, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(LinearAggregate implementation) {
        this.push(implementation);
        DBSPExpression map = this.transform(implementation.map);
        DBSPExpression postProcess = this.transform(implementation.postProcess);
        DBSPExpression emptySetResult = this.transform(implementation.emptySetResult);
        this.pop(implementation);

        LinearAggregate result = new LinearAggregate(
                implementation.getNode(), map.to(DBSPClosureExpression.class),
                postProcess.to(DBSPClosureExpression.class),
                emptySetResult);
        result.validate();
        this.map(implementation, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregateList aggregate) {
        this.push(aggregate);
        DBSPExpression rowVar = this.transform(aggregate.rowVar);
        List<IAggregate> implementations =
                Linq.map(aggregate.aggregates, c -> {
                    IDBSPInnerNode result = this.transform(c);
                    return result.to(IAggregate.class);
                });
        this.pop(aggregate);
        DBSPAggregateList result = new DBSPAggregateList(
                aggregate.getNode(), rowVar.to(DBSPVariablePath.class), implementations);
        this.map(aggregate, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFold fold) {
        this.push(fold);
        DBSPExpression zero = this.transform(fold.zero);
        DBSPClosureExpression increment = this.transform(fold.increment).to(DBSPClosureExpression.class);
        DBSPClosureExpression postProcessing = this.transform(fold.postProcess).to(DBSPClosureExpression.class);
        DBSPTypeUser semiGroup = this.transform(fold.semigroup).to(DBSPTypeUser.class);
        this.pop(fold);
        DBSPFold result = new DBSPFold(fold.getNode(), semiGroup, zero, increment, postProcessing);
        this.map(fold, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPMinMax aggregator) {
        this.push(aggregator);
        @Nullable
        DBSPClosureExpression postProcessing = (DBSPClosureExpression) this.transformN(aggregator.postProcessing);
        DBSPMinMax result = new DBSPMinMax(
                aggregator.getNode(), aggregator.getType(), postProcessing, aggregator.aggregation);
        this.map(aggregator, result);
        return VisitDecision.STOP;
    }

    /** Given a visitor for inner nodes returns a visitor
     * that optimizes an entire circuit. */
    public CircuitRewriter circuitRewriter(boolean processDeclarations) {
        return new CircuitRewriter(this.compiler, this, processDeclarations);
    }
}
