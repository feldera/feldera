package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPFloatLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPGeoPointLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPKeywordLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPConstItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.IModule;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Base class for Inner visitors which rewrite expressions, types, and statements.
 * This class recurses over the structure of expressions, types, and statements
 * and if any fields have changed builds a new version of the object.  Classes
 * that extend this should override the preorder methods and ignore the postorder
 * methods.
 */
public abstract class InnerRewriteVisitor
        extends InnerVisitor
        implements Function<IDBSPInnerNode, IDBSPInnerNode>, IModule {
    protected InnerRewriteVisitor(IErrorReporter reporter) {
        super(reporter,true);
    }

    /**
     * Result produced by the last preorder invocation.
     */
    @Nullable
    protected IDBSPInnerNode lastResult;

    IDBSPInnerNode getResult() {
        return Objects.requireNonNull(this.lastResult);
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode dbspNode) {
        this.startVisit();
        dbspNode.accept(this);
        this.endVisit();
        return this.getResult();
    }

    /**
     * Replace the 'old' IR node with the 'newOp' IR node.
     * @param old     only used for debugging.
     */
    protected void map(IDBSPInnerNode old, IDBSPInnerNode newOp) {
        if (old != newOp)
            Logger.INSTANCE.from(this, 1)
                    .append(this.toString())
                    .append(":")
                    .append((Supplier<String>) old::toString)
                    .append(" -> ")
                    .append((Supplier<String>) newOp::toString)
                    .newline();
        this.lastResult = newOp;
    }

    @Override
    public VisitDecision preorder(IDBSPInnerNode node) {
        this.map(node, node);
        return VisitDecision.STOP;
    }

    protected DBSPExpression getResultExpression() {
        return this.getResult().to(DBSPExpression.class);
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

    @Nullable
    protected DBSPType transformN(@Nullable DBSPType type) {
        if (type == null)
            return null;
        return this.transform(type);
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
        DBSPType resultType = this.transformN(type.resultType);
        DBSPType[] argTypes = this.transform(type.argumentTypes);
        DBSPType result = type;
        if (!DBSPType.sameType(type.resultType, resultType) ||
                !DBSPType.sameTypes(type.argumentTypes, argTypes))
            result = new DBSPTypeFunction(resultType, argTypes);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeIndexedZSet type) {
        DBSPType keyType = this.transform(type.keyType);
        DBSPType elementType = this.transform(type.elementType);
        DBSPType weightType = this.transform(type.weightType);
        DBSPType result = type;
        if (!type.keyType.sameType(keyType)
                || !type.elementType.sameType(elementType)
                || !type.weightType.sameType(weightType))
            result = new DBSPTypeIndexedZSet(type.getNode(), keyType, elementType, weightType);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRawTuple type) {
        DBSPType[] elements = this.transform(type.tupFields);
        DBSPType result = type;
        if (!DBSPType.sameTypes(type.tupFields, elements))
            result = new DBSPTypeRawTuple(elements);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeTuple type) {
        DBSPType[] elements = this.transform(type.tupFields);
        DBSPType result = type;
        if (!DBSPType.sameTypes(type.tupFields, elements))
            result = new DBSPTypeTuple(elements);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRef type) {
        DBSPType field = this.transform(type.type);
        DBSPType result = type;
        if (!DBSPType.sameType(type.type, field))
            result = new DBSPTypeRef(field, type.mutable);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeUser type) {
        DBSPType[] args = this.transform(type.typeArgs);
        DBSPType result = type;
        if (!DBSPType.sameTypes(type.typeArgs, args))
            result = new DBSPTypeUser(type.getNode(), type.name, type.mayBeNull, args);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStream type) {
        DBSPType elementType = this.transform(type.elementType);
        DBSPType result = type;
        if (!DBSPType.sameType(type.elementType, elementType))
            result = new DBSPTypeStream(elementType);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct type) {
        List<DBSPTypeStruct.Field> fields = Linq.map(
                type.args, f -> new DBSPTypeStruct.Field(f.getNode(), f.name, this.transform(f.type)));
        DBSPType result = type;
        if (Linq.different(type.args, fields))
            result = new DBSPTypeStruct(type.getNode(), type.name, fields);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeVec type) {
        DBSPType elementType = this.transform(type.getElementType());
        DBSPType result = type;
        if (!DBSPType.sameType(type.getElementType(), elementType))
            result = new DBSPTypeVec(elementType);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeZSet type) {
        DBSPType elementType = this.transform(type.elementType);
        DBSPType weightType = this.transform(type.weightType);
        DBSPType result = type;
        if (!elementType.sameType(type.elementType) ||
            !weightType.sameType(type.weightType))
            result = new DBSPTypeZSet(type.getNode(), elementType, weightType);
        this.map(type, result);
        return VisitDecision.STOP;
    }

    /////////////////////// Expressions //////////////////////////

    @Override
    public VisitDecision preorder(DBSPBoolLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPBoolLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDateLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPDateLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDecimalLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPDecimalLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDoubleLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPDoubleLiteral(expression.getNode(), type, expression.value, expression.raw);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFloatLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPFloatLiteral(expression.getNode(), type, expression.value, expression.raw);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPGeoPointLiteral expression) {
        @Nullable DBSPExpression left = this.transformN(expression.left);
        @Nullable DBSPExpression right = this.transformN(expression.right);
        DBSPExpression result = expression;
        if (!DBSPExpression.same(expression.left, left) ||
            !DBSPExpression.same(expression.right, right)) {
            result = new DBSPGeoPointLiteral(expression.getNode(), left, right);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI16Literal expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPI16Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI32Literal expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPI32Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI64Literal expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPI64Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntervalMillisLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPIntervalMillisLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntervalMonthsLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPIntervalMonthsLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPISizeLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (type != expression.getNonVoidType())
            result = new DBSPISizeLiteral(expression.getNode(), type, expression.value);
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
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()))
            result = new DBSPNullLiteral(expression.getNode(), type, null);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()))
            result = new DBSPStringLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStrLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()))
            result = new DBSPStrLiteral(expression.getNode(), type, expression.value, expression.raw);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimeLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()))
            result = new DBSPTimeLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimestampLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()))
            result = new DBSPTimestampLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU32Literal expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()))
            result = new DBSPU32Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU64Literal expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()))
            result = new DBSPU64Literal(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUSizeLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()))
            result = new DBSPUSizeLiteral(expression.getNode(), type, expression.value);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVecLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        List<DBSPExpression> data = null;
        if (expression.data != null)
            data = Linq.map(expression.data, this::transform);
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()) ||
            Linq.different(data, expression.data))
            result = new DBSPVecLiteral(expression.getNode(), type, data);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPZSetLiteral expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        Map<DBSPExpression, Long> newData = new HashMap<>();
        boolean changed = false;
        for (Map.Entry<DBSPExpression, Long> entry: expression.data.data.entrySet()) {
            DBSPExpression row = this.transform(entry.getKey());
            changed = changed || (row != entry.getKey());
            newData.put(row, entry.getValue());
        }
        DBSPType elementType = this.transform(expression.data.elementType);

        DBSPExpression result = expression;
        if (type != expression.getNonVoidType() ||
                elementType != expression.data.elementType ||
                changed) {
            DBSPZSetLiteral.Contents newContents = new DBSPZSetLiteral.Contents(newData, elementType);
            result = new DBSPZSetLiteral(expression.getNode(), type, newContents);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFlatmap expression) {
        DBSPTypeTuple inputElementType = this.transform(expression.inputElementType).to(DBSPTypeTuple.class);
        @Nullable DBSPType indexType = this.transformN(expression.indexType);
        DBSPExpression result = expression;
        if (!inputElementType.sameType(expression.inputElementType) ||
                !DBSPType.sameType(indexType, expression.indexType))
            result = new DBSPFlatmap(expression.getNode(), inputElementType,
                    expression.collectionFieldIndex, expression.outputFieldIndexes,
                    indexType);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAsExpression expression) {
        DBSPExpression source = this.transform(expression.source);
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (source != expression.source || !type.sameType(expression.getNonVoidType())) {
            result = new DBSPAsExpression(source, type);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
        DBSPExpression[] arguments = this.transform(expression.arguments);
        DBSPExpression function = this.transform(expression.function);
        @Nullable DBSPType type = this.transformN(expression.getType());
        DBSPExpression result = expression;
        if (function != expression.function || Linq.different(arguments, expression.arguments)
            || !DBSPType.sameType(type, expression.getType()))
            result = new DBSPApplyExpression(function, type, arguments);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyMethodExpression expression) {
        DBSPExpression[] arguments = this.transform(expression.arguments);
        DBSPExpression function = this.transform(expression.function);
        DBSPExpression self = this.transform(expression.self);
        @Nullable DBSPType type = this.transformN(expression.getType());
        DBSPExpression result = expression;
        if (function != expression.function
                || self != expression.self
                || !DBSPType.sameType(type, expression.getType())
                || Linq.different(arguments, expression.arguments))
            result = new DBSPApplyMethodExpression(function, type, self, arguments);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAssignmentExpression expression) {
        DBSPExpression left = this.transform(expression.left);
        DBSPExpression right = this.transform(expression.right);
        DBSPExpression result = expression;
        if (left != expression.left || right != expression.right)
            result = new DBSPAssignmentExpression(left, right);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryExpression expression) {
        DBSPExpression left = this.transform(expression.left);
        DBSPExpression right = this.transform(expression.right);
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (left != expression.left || right != expression.right
                || !type.sameType(expression.getNonVoidType()))
            result = new DBSPBinaryExpression(expression.getNode(), type,
                    expression.operation, left, right, expression.primitive);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression expression) {
        List<DBSPStatement> body = Linq.map(expression.contents, this::transform);
        DBSPExpression last = this.transformN(expression.lastExpression);
        DBSPExpression result = expression;
        if (Linq.different(body, expression.contents) || last != expression.lastExpression) {
            result = new DBSPBlockExpression(body, last);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBorrowExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression) {
            result = source.borrow(expression.mut);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSomeExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression) {
            result = new DBSPSomeExpression(expression.getNode(), source);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }


    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
        DBSPExpression source = this.transform(expression.source);
        DBSPType type = this.transform(expression.destinationType);
        DBSPExpression result = expression;
        if (source != expression.source || !type.sameType(expression.destinationType)) {
            result = source.cast(type);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIsNullExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression) {
            result = source.is_null();
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        DBSPExpression body = this.transform(expression.body);
        DBSPParameter[] parameters = Linq.map(
                expression.parameters, p -> {
                    p.accept(this);
                    return this.getResult().to(DBSPParameter.class);
                }, DBSPParameter.class);
        DBSPExpression result = expression;
        if (body != expression.body || Linq.different(parameters, expression.parameters))
            result = body.closure(parameters);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIndexExpression expression) {
        DBSPExpression array = this.transform(expression.array);
        DBSPExpression index = this.transform(expression.index);
        DBSPExpression result = expression;
        if (array != expression.array || index != expression.index)
            result = new DBSPIndexExpression(expression.getNode(), array, index, expression.startsAtOne);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldComparatorExpression expression) {
        DBSPExpression source = this.transform(expression.source);
        DBSPExpression result = expression;
        if (source != expression.source)
            result = new DBSPFieldComparatorExpression(
                    expression.getNode(), source.to(DBSPComparatorExpression.class),
                    expression.fieldNo, expression.ascending);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPNoComparatorExpression expression) {
        DBSPType type = this.transform(expression.tupleType);
        DBSPExpression result = expression;
        if (type != expression.tupleType)
            result = new DBSPNoComparatorExpression(expression.getNode(), type);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDerefExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression)
            result = new DBSPDerefExpression(source);
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
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression)
            result = source.field(expression.fieldNo);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPForExpression expression) {
        DBSPExpression iterated = this.transform(expression.iterated);
        DBSPExpression body = this.transform(expression.block);
        DBSPBlockExpression block;
        if (body.is(DBSPBlockExpression.class))
            block = body.to(DBSPBlockExpression.class);
        else
            block = new DBSPBlockExpression(Linq.list(), body);
        DBSPExpression result = expression;
        if (iterated != expression.iterated ||
                block != expression.block) {
            result = new DBSPForExpression(expression.pattern, iterated, block);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIfExpression expression) {
        DBSPExpression cond = this.transform(expression.condition);
        DBSPExpression positive = this.transform(expression.positive);
        DBSPExpression negative = this.transform(expression.negative);
        DBSPExpression result = expression;
        if (cond != expression.condition || positive != expression.positive || negative != expression.negative) {
            result = new DBSPIfExpression(expression.getNode(), cond, positive, negative);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPMatchExpression expression) {
        List<DBSPExpression> caseExpressions =
                Linq.map(expression.cases, c -> this.transform(c.result));
        DBSPExpression matched = this.transform(expression.matched);
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getNonVoidType()) || matched != expression.matched ||
                Linq.different(caseExpressions, Linq.map(expression.cases, c -> c.result))) {
            List<DBSPMatchExpression.Case> newCases = Linq.zipSameLength(expression.cases, caseExpressions,
                    (c0, e) -> new DBSPMatchExpression.Case(c0.against, e));
            result = new DBSPMatchExpression(matched, newCases, type);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPathExpression expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!expression.getNonVoidType().sameType(type))
            result = new DBSPPathExpression(type, expression.path);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPQualifyTypeExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPType[] types = this.transform(expression.types);
        DBSPExpression result = expression;
        if (source != expression.expression || DBSPType.sameTypes(types, expression.types)) {
            result = new DBSPQualifyTypeExpression(source, types);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRangeExpression expression) {
        @Nullable DBSPExpression left = this.transformN(expression.left);
        @Nullable DBSPExpression right = this.transformN(expression.right);
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (left != expression.left || right != expression.right
                || !type.sameType(expression.getNonVoidType()))
            result = new DBSPRangeExpression(left, right,
                    expression.endInclusive, type);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRawTupleExpression expression) {
        DBSPExpression[] fields = this.transform(expression.fields);
        DBSPExpression result = expression;
        if (Linq.different(fields, expression.fields)) {
            result = new DBSPRawTupleExpression(fields);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSortExpression expression) {
        DBSPExpression comparator = this.transform(expression.comparator);
        DBSPType elementType = this.transform(expression.elementType);
        DBSPExpression result = expression;
        if (comparator != expression.comparator || !elementType.sameType(expression.elementType))
            result = new DBSPSortExpression(expression.getNode(), elementType, comparator.to(DBSPComparatorExpression.class));
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStructExpression expression) {
        DBSPExpression function = this.transform(expression.function);
        DBSPExpression[] arguments = this.transform(expression.arguments);
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (Linq.different(arguments, expression.arguments) || !type.sameType(expression.getNonVoidType())) {
            result = new DBSPStructExpression(function, type, arguments);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression expression) {
        DBSPExpression[] fields = this.transform(expression.fields);
        DBSPExpression result = expression;
        if (Linq.different(fields, expression.fields)) {
            result = new DBSPTupleExpression(fields);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnaryExpression expression) {
        DBSPExpression source = this.transform(expression.source);
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (source != expression.source || !type.sameType(expression.getNonVoidType())) {
            result = new DBSPUnaryExpression(expression.getNode(), type,
                    expression.operation, source);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression) {
            result = new DBSPCloneExpression(expression.getNode(), source);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath expression) {
        DBSPType type = this.transform(expression.getNonVoidType());
        DBSPExpression result = expression;
        if (!type.sameType(expression.getType())) {
            result = new DBSPVariablePath(expression.variable, type);
        }
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    /////////////////// statements

    @Override
    public VisitDecision preorder(DBSPExpressionStatement statement) {
        DBSPExpression expression = this.transform(statement.expression);
        DBSPStatement result = statement;
        if (expression != statement.expression)
            result = new DBSPExpressionStatement(expression);
        this.map(statement, result);
        return VisitDecision.STOP;
    }

    public VisitDecision preorder(DBSPLetStatement statement) {
        DBSPExpression init = this.transformN(statement.initializer);
        DBSPType type = this.transform(statement.type);
        DBSPStatement result = statement;
        if (init != statement.initializer || !type.sameType(statement.type)) {
            if (init != null)
                result = new DBSPLetStatement(statement.variable, init);
            else
                result = new DBSPLetStatement(statement.variable, type, statement.mutable);
        }
        this.map(statement, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPComment comment) {
        this.map(comment, comment);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstItem item) {
        DBSPType type = this.transform(item.type);
        @Nullable DBSPExpression expression = this.transformN(item.expression);
        DBSPConstItem result = item;
        if (!type.sameType(item.type) || expression != item.expression)
            result = new DBSPConstItem(item.name, type, expression);
        this.map(item, result);
        return VisitDecision.STOP;
    }

    /// Other objects

    @Override
    public VisitDecision preorder(DBSPParameter parameter) {
        DBSPType type = this.transform(parameter.type);
        DBSPParameter result = parameter;
        if (type != parameter.type)
            result = new DBSPParameter(parameter.pattern, type);
        this.map(parameter, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFunction function) {
        @Nullable DBSPType returnType = this.transformN(function.returnType);
        DBSPExpression body = this.transform(function.body);
        List<DBSPParameter> parameters =
                Linq.map(function.parameters, p -> this.apply(p).to(DBSPParameter.class));
        DBSPFunction result = function;
        if (!DBSPType.sameType(returnType, function.returnType) ||
            body != function.body || Linq.different(parameters, function.parameters)) {
            result = new DBSPFunction(function.name, parameters, returnType, body, function.annotations);
        }
        this.map(function, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregate.Implementation implementation) {
        DBSPExpression zero = this.transform(implementation.zero);
        DBSPExpression increment = this.transform(implementation.increment);
        @Nullable DBSPExpression postProcess = this.transformN(implementation.postProcess);
        DBSPExpression emptySetResult = this.transform(implementation.emptySetResult);
        DBSPType semiGroup = this.transform(implementation.semigroup);

        DBSPAggregate.Implementation result = implementation;
        if (zero != implementation.zero
                || increment != implementation.increment
                || postProcess != implementation.postProcess
                || emptySetResult != implementation.emptySetResult
                || semiGroup != implementation.semigroup) {
            result = new DBSPAggregate.Implementation(implementation.operator, zero,
                    increment.to(DBSPClosureExpression.class),
                    postProcess != null ? postProcess.to(DBSPClosureExpression.class) : null,
                    emptySetResult, semiGroup);
        }
        this.map(implementation, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregate aggregate) {
        DBSPExpression rowVar = this.transform(aggregate.rowVar);
        DBSPAggregate.Implementation[] implementations =
                Linq.map(aggregate.components, c -> {
                            IDBSPInnerNode result = this.apply(c);
                            return result.to(DBSPAggregate.Implementation.class);
                        },
                        DBSPAggregate.Implementation.class);
        DBSPAggregate result = aggregate;
        if (rowVar != aggregate.rowVar ||
            Linq.different(implementations, aggregate.components)) {
            result = new DBSPAggregate(aggregate.getNode(), rowVar.to(DBSPVariablePath.class), implementations);
        }
        this.map(aggregate, result);
        return VisitDecision.STOP;
    }

    /**
     * Given a visitor for inner nodes returns a visitor
     * that optimizes an entire circuit.
     */
    public CircuitRewriter circuitRewriter() {
        return new CircuitRewriter(this.errorReporter, this);
    }
}
