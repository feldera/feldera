/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyBaseExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalAggregateExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdField;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEnumValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQuestionExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPFPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPKeywordLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUuidLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVariantNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPPathSegment;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.pattern.DBSPPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPConstItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructWithHelperItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.aggregate.AggregateBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeFP;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeo;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeoPoint;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeISize;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeReal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeStr;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUuid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeBTreeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeOption;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeResult;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSemigroup;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.IHasId;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Depth-first traversal of an DBSPInnerNode hierarchy. */
@SuppressWarnings({"SameReturnValue, EmptyMethod", "unused"})
public abstract class InnerVisitor implements IRTransform, IWritesLogs, IHasId, ICompilerComponent {
    final long id;
    static long crtId = 0;
    public final DBSPCompiler compiler;
    protected final List<IDBSPInnerNode> context;

    public InnerVisitor(DBSPCompiler compiler) {
        this.id = crtId++;
        this.compiler = compiler;
        this.context = new ArrayList<>();
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
    }

    @Override
    public long getId() {
        return this.id;
    }

    // A chance for subclasses to do something for every expression visited
    public void visitingExpression(DBSPExpression expression) {}

    public void push(IDBSPInnerNode node) {
        this.context.add(node);
        if (node.is(DBSPExpression.class)) {
            this.visitingExpression(node.to(DBSPExpression.class));
        }
    }

    public void pop(IDBSPInnerNode node) {
        IDBSPInnerNode last = Utilities.removeLast(this.context);
        if (node != last)
            throw new InternalCompilerError("Corrupted visitor context: popping " + node
                    + " instead of " + last, node);
    }

    @Nullable
    public IDBSPInnerNode getParent() {
        if (this.context.isEmpty())
            return null;
        return Utilities.last(this.context);
    }

    /** Override to initialize before visiting any node. */
    public void startVisit(IDBSPInnerNode node) {
        Logger.INSTANCE.belowLevel(this, 4)
                .append("Starting ")
                .appendSupplier(this::toString)
                .append(" at ")
                .append(node);
    }

    /** Override to finish after visiting all nodes. */
    public void endVisit() {}

    /************************* PREORDER *****************************/

    // preorder methods return 'true' when normal traversal is desired,
    // and 'false' when the traversal should stop right away at the current node.
    // base classes
    public VisitDecision preorder(IDBSPInnerNode ignored) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPAggregate node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(AggregateBase node) {
        return this.preorder((IDBSPInnerNode) node);
    }
    public VisitDecision preorder(NonLinearAggregate node) {
        return this.preorder((AggregateBase) node);
    }

    public VisitDecision preorder(LinearAggregate node) {
        return this.preorder((AggregateBase) node);
    }

    public VisitDecision preorder(DBSPExpression node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(DBSPStatement node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(DBSPItem node) {
        return this.preorder((DBSPStatement) node);
    }

    public VisitDecision preorder(DBSPTypeStruct.Field node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(DBSPType node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(DBSPTypeBaseType node) {
        return this.preorder((DBSPType) node);
    }

    public VisitDecision preorder(DBSPTypeDate node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeTime node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeMillisInterval node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeMonthsInterval node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeGeo node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeGeoPoint node) {
        return this.preorder((DBSPTypeGeo) node);
    }

    public VisitDecision preorder(DBSPFunction node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(DBSPPath node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(DBSPPattern node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(DBSPParameter node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    // Statements

    public VisitDecision preorder(DBSPExpressionStatement node) {
        return this.preorder((DBSPStatement) node);
    }

    public VisitDecision preorder(DBSPComment node) {
        return this.preorder((DBSPStatement) node);
    }

    public VisitDecision preorder(DBSPLetStatement node) {
        return this.preorder((DBSPStatement) node);
    }

    public VisitDecision preorder(DBSPConstItem node) {
        return this.preorder((DBSPItem) node);
    }

    public VisitDecision preorder(DBSPFunctionItem node) {
        return this.preorder((DBSPItem) node);
    }

    public VisitDecision preorder(DBSPStructItem node) {
        return this.preorder((DBSPItem) node);
    }

    public VisitDecision preorder(DBSPStructWithHelperItem node) {
        return this.preorder((DBSPItem) node);
    }

    // Various

    public VisitDecision preorder(DBSPPathSegment node) {
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(DBSPSimplePathSegment node) {
        return this.preorder((DBSPPathSegment) node);
    }

    // Types

    public VisitDecision preorder(DBSPTypeFP node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeReal node) {
        return this.preorder((DBSPTypeFP) node);
    }

    public VisitDecision preorder(DBSPTypeDouble node) {
        return this.preorder((DBSPTypeFP) node);
    }

    public VisitDecision preorder(DBSPTypeISize node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeStruct node) {
        return this.preorder((DBSPType) node);
    }

    public VisitDecision preorder(DBSPTypeString node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeBinary node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeUSize node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeTupleBase node) {
        return this.preorder((DBSPType) node);
    }

    public VisitDecision preorder(DBSPTypeRawTuple node) {
        return this.preorder((DBSPTypeTupleBase) node);
    }

    public VisitDecision preorder(DBSPTypeTuple node) {
        return this.preorder((DBSPTypeTupleBase) node);
    }

    public VisitDecision preorder(DBSPTypeStr node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeTimestamp node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeInteger node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeDecimal node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeNull node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeVoid node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeFunction node) {
        return this.preorder((DBSPType) node);
    }

    public VisitDecision preorder(DBSPTypeBool node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeUuid node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    public VisitDecision preorder(DBSPTypeStream node) {
        return this.preorder((DBSPType) node);
    }

    public VisitDecision preorder(DBSPTypeUser node) {
        return this.preorder((DBSPType) node);
    }

    public VisitDecision preorder(DBSPTypeIndexedZSet node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeZSet node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeArray node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeVec node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeOption node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeMap node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeBTreeMap node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeWithCustomOrd node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeResult node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeTypedBox node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeSemigroup node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeAny node) {
        return this.preorder((DBSPType) node);
    }

    public VisitDecision preorder(DBSPTypeRef node) {
        return this.preorder((DBSPType) node);
    }

    public VisitDecision preorder(DBSPTypeVariant node) {
        return this.preorder((DBSPTypeBaseType) node);
    }

    // Patterns

    public VisitDecision preorder(DBSPIdentifierPattern node) {
        return this.preorder((DBSPPattern) node);
    }

    // Expressions

    public VisitDecision preorder(DBSPFlatmap node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPSortExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPComparatorExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPNoComparatorExpression node) {
        return this.preorder((DBSPComparatorExpression) node);
    }

    public VisitDecision preorder(DBSPFieldComparatorExpression node) {
        return this.preorder((DBSPComparatorExpression) node);
    }

    public VisitDecision preorder(DBSPDirectComparatorExpression node) {
        return this.preorder((DBSPComparatorExpression) node);
    }

    public VisitDecision preorder(DBSPCustomOrdExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPUnwrapCustomOrdExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPConstructorExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPConditionalAggregateExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPBorrowExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPCastExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPCloneExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPSomeExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPIsNullExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPClosureExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPQualifyTypeExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPBinaryExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPEnumValue node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPDerefExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPLetExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPCustomOrdField node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPUnwrapExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPPathExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPForExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPUnaryExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPStaticExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPBaseTupleExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPTupleExpression node) {
        return this.preorder((DBSPBaseTupleExpression) node);
    }

    public VisitDecision preorder(DBSPRawTupleExpression node) {
        return this.preorder((DBSPBaseTupleExpression) node);
    }

    public VisitDecision preorder(DBSPFieldExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPIfExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPBlockExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPApplyBaseExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPApplyMethodExpression node) {
        return this.preorder((DBSPApplyBaseExpression) node);
    }

    public VisitDecision preorder(DBSPApplyExpression node) {
        return this.preorder((DBSPApplyBaseExpression) node);
    }

    public VisitDecision preorder(DBSPAssignmentExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPQuestionExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPVariablePath node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(NoExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPUnsignedWrapExpression node) { return this.preorder((DBSPExpression) node); }

    public VisitDecision preorder(DBSPUnsignedUnwrapExpression node) { return this.preorder((DBSPExpression) node); }

    public VisitDecision preorder(DBSPWindowBoundExpression node) { return this.preorder((DBSPExpression) node); }

    // Literals
    public VisitDecision preorder(DBSPLiteral node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPNullLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPVariantExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPVariantNullLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPArrayExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPMapExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPTimestampLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPDateLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPTimeLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPIntervalMillisLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPIntervalMonthsLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPFPLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPRealLiteral node) {
        return this.preorder((DBSPFPLiteral) node);
    }

    public VisitDecision preorder(DBSPUSizeLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPZSetExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPIndexedZSetExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPStrLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPDecimalLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPStringLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPBinaryLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPIntLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPI8Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPI16Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPI32Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPU16Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPU32Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPI64Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPI128Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPU64Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPU128Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPBoolLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPUuidLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPDoubleLiteral node) {
        return this.preorder((DBSPFPLiteral) node);
    }

    public VisitDecision preorder(DBSPKeywordLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPISizeLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPGeoPointConstructor node) {
        return this.preorder((DBSPExpression) node);
    }

    /*************************** POSTORDER *****************************/

    @SuppressWarnings("EmptyMethod")
    public void postorder(IDBSPInnerNode ignored) {}

    public void postorder(DBSPAggregate node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(AggregateBase node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(NonLinearAggregate node) {
        this.postorder((AggregateBase) node);
    }

    public void postorder(LinearAggregate node) {
        this.postorder((AggregateBase) node);
    }

    public void postorder(DBSPExpression node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPStatement node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPType node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPTypeBaseType node) {
        this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeDate node)  {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeTime node)  {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeMillisInterval node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeGeo node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeGeoPoint node) {
        this.postorder((DBSPTypeGeo) node);
    }

    public void postorder(DBSPFunction node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPPath node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPPattern node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPParameter node) {
        this.postorder((IDBSPInnerNode) node);
    }

    // Statements

    public void postorder(DBSPItem node) {
        this.postorder((DBSPStatement) node);
    }

    public void postorder(DBSPConstItem node) {
        this.postorder((DBSPItem) node);
    }

    public void postorder(DBSPFunctionItem node) {
        this.postorder((DBSPItem) node);
    }

    public void postorder(DBSPStructItem node) {
        this.postorder((DBSPItem) node);
    }

    public void postorder(DBSPStructWithHelperItem node) {
        this.postorder((DBSPItem) node);
    }

    public void postorder(DBSPExpressionStatement node) {
        this.postorder((DBSPStatement) node);
    }

    public void postorder(DBSPComment node) {
        this.postorder((DBSPStatement) node);
    }

    public void postorder(DBSPLetStatement node) {
        this.postorder((DBSPStatement) node);
    }

    // Various

    public void postorder(DBSPPathSegment node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPSimplePathSegment node) {
        this.postorder((DBSPPathSegment) node);
    }

    // Types

    public void postorder(DBSPTypeFP node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeReal node) {
        this.postorder((DBSPTypeFP) node);
    }

    public void postorder(DBSPTypeDouble node) {
        this.postorder((DBSPTypeFP) node);
    }

    public void postorder(DBSPTypeISize node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeStruct node) {
        this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeStr node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeString node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeBinary node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeUSize node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeTupleBase node) {
        this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeTuple node) {
        this.postorder((DBSPTypeTupleBase) node);
    }

    public void postorder(DBSPTypeRawTuple node) {
        this.postorder((DBSPTypeTupleBase) node);
    }

    public void postorder(DBSPTypeTimestamp node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeInteger node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeDecimal node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeNull node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeVoid node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeFunction node) {
        this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeBool node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeUuid node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeStream node) {
        this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeUser node) {
        this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeIndexedZSet node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeZSet node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeOption node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeArray node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeVec node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeMap node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeBTreeMap node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeWithCustomOrd node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeResult node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeTypedBox node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeSemigroup node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeAny node) {
        this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeRef node) {
        this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeVariant node) {
        this.postorder((DBSPTypeBaseType) node);
    }

    // Patterns

    public void postorder(DBSPIdentifierPattern node) {
        this.postorder((DBSPPattern) node);
    }

    // Expressions

    public void postorder(DBSPFlatmap node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPSortExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPComparatorExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPNoComparatorExpression node) {
        this.postorder((DBSPComparatorExpression) node);
    }

    public void postorder(DBSPFieldComparatorExpression node) {
        this.postorder((DBSPComparatorExpression) node);
    }

    public void postorder(DBSPDirectComparatorExpression node) {
        this.postorder((DBSPComparatorExpression) node);
    }

    public void postorder(DBSPCustomOrdExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPUnwrapCustomOrdExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPConstructorExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPConditionalAggregateExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBorrowExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPCastExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPCloneExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPSomeExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPIsNullExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPClosureExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPQualifyTypeExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBinaryExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPEnumValue node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPDerefExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPLetExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPCustomOrdField node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPUnwrapExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPPathExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPForExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPUnaryExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPStaticExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBaseTupleExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPTupleExpression node) {
        this.postorder((DBSPBaseTupleExpression) node);
    }

    public void postorder(DBSPRawTupleExpression node) {
        this.postorder((DBSPBaseTupleExpression) node);
    }

    public void postorder(DBSPFieldExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPIfExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBlockExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPApplyBaseExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPApplyExpression node) {
        this.postorder((DBSPApplyBaseExpression) node);
    }

    public void postorder(DBSPApplyMethodExpression node) {
        this.postorder((DBSPApplyBaseExpression) node);
    }

    public void postorder(DBSPAssignmentExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPQuestionExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPVariablePath node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(NoExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPUnsignedWrapExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPUnsignedUnwrapExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPWindowBoundExpression node) {
        this.postorder((DBSPExpression) node);
    }

    // Literals
    public void postorder(DBSPLiteral node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPTimestampLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPDateLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPTimeLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPIntervalMillisLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPNullLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPVariantExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPVariantNullLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPArrayExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPMapExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPFPLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPRealLiteral node) {
        this.postorder((DBSPFPLiteral) node);
    }

    public void postorder(DBSPUSizeLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPIndexedZSetExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPZSetExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPStrLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPDecimalLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPStringLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPBinaryLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPIntLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPI8Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPI16Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPI32Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPU16Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPU32Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPI64Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPI128Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPU64Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPU128Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPBoolLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPUuidLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPDoubleLiteral node) {
        this.postorder((DBSPFPLiteral) node);
    }

    public void postorder(DBSPKeywordLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPISizeLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPGeoPointConstructor node)  {
        this.postorder((DBSPExpression) node);
    }

    @Override
    public String toString() {
        return this.id + " " + this.getClass().getSimpleName();
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        this.startVisit(node);
        node.accept(this);
        this.endVisit();
        return node;
    }

    public CircuitRewriter getCircuitVisitor(boolean processDeclarations) {
        return new CircuitRewriter(this.compiler(), this, processDeclarations);
    }
}
