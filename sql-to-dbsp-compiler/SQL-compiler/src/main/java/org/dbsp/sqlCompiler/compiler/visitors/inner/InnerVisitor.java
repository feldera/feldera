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

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyBaseExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAsExpression;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEnumValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQuestionExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPFPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPGeoPointLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIndexedZSetLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPKeywordLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
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
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
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
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeResult;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSemigroup;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Depth-first traversal of an DBSPInnerNode hierarchy.
 */
@SuppressWarnings({"SameReturnValue, EmptyMethod", "unused"})
public abstract class InnerVisitor implements IRTransform, IWritesLogs {
    protected final IErrorReporter errorReporter;
    protected final List<IDBSPInnerNode> context;

    public InnerVisitor(IErrorReporter reporter) {
        this.errorReporter = reporter;
        this.context = new ArrayList<>();
    }

    public void push(IDBSPInnerNode node) {
        this.context.add(node);
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

    /**
     * Override to initialize before visiting any node.
     */
    public void startVisit(IDBSPInnerNode node) {
        Logger.INSTANCE.belowLevel(this, 4)
                .append("Starting ")
                .append(this.toString())
                .append(" at ")
                .append(node);
    }

    /**
     * Override to finish after visiting all nodes.
     */
    public void endVisit() {}

    public void traverse(IDBSPInnerNode node) {
        this.startVisit(node);
        node.accept(this);
        this.endVisit();
    }

    /************************* PREORDER *****************************/

    // preorder methods return 'true' when normal traversal is desired,
    // and 'false' when the traversal should stop right away at the current node.
    // base classes
    public VisitDecision preorder(IDBSPInnerNode ignored) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPAggregate node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPAggregate.Implementation node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPExpression node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPStatement node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPItem node) {
        return this.preorder(node.to(DBSPStatement.class));
    }

    public VisitDecision preorder(DBSPTypeStruct.Field node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPType node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPTypeBaseType node) {
        return this.preorder(node.to(DBSPType.class));
    }

    public VisitDecision preorder(DBSPTypeDate node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeTime node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeMillisInterval node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeMonthsInterval node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeGeo node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeGeoPoint node) {
        return this.preorder((DBSPTypeGeo) node);
    }

    public VisitDecision preorder(DBSPFunction node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPPath node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPPattern node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPParameter node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    // Statements
    
    public VisitDecision preorder(DBSPExpressionStatement node) {
        return this.preorder(node.to(DBSPStatement.class));
    }

    public VisitDecision preorder(DBSPComment node) {
        return this.preorder(node.to(DBSPStatement.class));
    }

    public VisitDecision preorder(DBSPLetStatement node) {
        return this.preorder(node.to(DBSPStatement.class));
    }

    public VisitDecision preorder(DBSPConstItem node) {
        return this.preorder(node.to(DBSPItem.class));
    }

    public VisitDecision preorder(DBSPFunctionItem node) {
        return this.preorder(node.to(DBSPItem.class));
    }

    public VisitDecision preorder(DBSPStructItem node) {
        return this.preorder(node.to(DBSPItem.class));
    }

    public VisitDecision preorder(DBSPStructWithHelperItem node) {
        return this.preorder(node.to(DBSPItem.class));
    }

    // Various
    
    public VisitDecision preorder(DBSPPathSegment node) {
        return this.preorder(node.to(IDBSPInnerNode.class));
    }

    public VisitDecision preorder(DBSPSimplePathSegment node) {
        return this.preorder(node.to(DBSPPathSegment.class));
    }

    // Types
    
    public VisitDecision preorder(DBSPTypeFP node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeReal node) {
        return this.preorder(node.to(DBSPTypeFP.class));
    }

    public VisitDecision preorder(DBSPTypeDouble node) {
        return this.preorder(node.to(DBSPTypeFP.class));
    }

    public VisitDecision preorder(DBSPTypeISize node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeStruct node) {
        return this.preorder(node.to(DBSPType.class));
    }

    public VisitDecision preorder(DBSPTypeString node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeBinary node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }
    
    public VisitDecision preorder(DBSPTypeUSize node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeTupleBase node) {
        return this.preorder(node.to(DBSPType.class));
    }

    public VisitDecision preorder(DBSPTypeRawTuple node) {
        return this.preorder(node.to(DBSPTypeTupleBase.class));
    }

    public VisitDecision preorder(DBSPTypeTuple node) {
        return this.preorder(node.to(DBSPTypeTupleBase.class));
    }

    public VisitDecision preorder(DBSPTypeStr node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeTimestamp node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeInteger node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeDecimal node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeNull node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeVoid node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeFunction node) {
        return this.preorder(node.to(DBSPType.class));
    }

    public VisitDecision preorder(DBSPTypeBool node) {
        return this.preorder(node.to(DBSPTypeBaseType.class));
    }

    public VisitDecision preorder(DBSPTypeStream node) {
        return this.preorder(node.to(DBSPType.class));
    }
    
    public VisitDecision preorder(DBSPTypeUser node) {
        return this.preorder(node.to(DBSPType.class));
    }

    public VisitDecision preorder(DBSPTypeIndexedZSet node) {
        return this.preorder(node.to(DBSPTypeUser.class));
    }

    public VisitDecision preorder(DBSPTypeZSet node) {
        return this.preorder(node.to(DBSPTypeUser.class));
    }

    public VisitDecision preorder(DBSPTypeVec node) {
        return this.preorder(node.to(DBSPTypeUser.class));
    }

    public VisitDecision preorder(DBSPTypeResult node) {
        return this.preorder(node.to(DBSPTypeUser.class));
    }

    public VisitDecision preorder(DBSPTypeTypedBox node) {
        return this.preorder(node.to(DBSPTypeUser.class));
    }

    public VisitDecision preorder(DBSPTypeSemigroup node) {
        return this.preorder(node.to(DBSPTypeUser.class));
    }

    public VisitDecision preorder(DBSPTypeAny node) {
        return this.preorder(node.to(DBSPType.class));
    }
    
    public VisitDecision preorder(DBSPTypeRef node) {
        return this.preorder(node.to(DBSPType.class));
    }

    // Patterns

    public VisitDecision preorder(DBSPIdentifierPattern node) {
        return this.preorder(node.to(DBSPPattern.class));
    }

    // Expressions

    public VisitDecision preorder(DBSPFlatmap node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPSortExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPComparatorExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPNoComparatorExpression node) {
        return this.preorder((DBSPComparatorExpression) node);
    }

    public VisitDecision preorder(DBSPFieldComparatorExpression node) {
        return this.preorder((DBSPComparatorExpression) node);
    }

    public VisitDecision preorder(DBSPConstructorExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPConditionalAggregateExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPBorrowExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPCastExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPCloneExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPSomeExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPIsNullExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPClosureExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPQualifyTypeExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPBinaryExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPEnumValue node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPDerefExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPUnwrapExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPPathExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPForExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPUnaryExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPBaseTupleExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPTupleExpression node) {
        return this.preorder(node.to(DBSPBaseTupleExpression.class));
    }

    public VisitDecision preorder(DBSPRawTupleExpression node) {
        return this.preorder(node.to(DBSPBaseTupleExpression.class));
    }

    public VisitDecision preorder(DBSPFieldExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPIfExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPBlockExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPApplyBaseExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPApplyMethodExpression node) {
        return this.preorder(node.to(DBSPApplyBaseExpression.class));
    }

    public VisitDecision preorder(DBSPApplyExpression node) {
        return this.preorder(node.to(DBSPApplyBaseExpression.class));
    }

    public VisitDecision preorder(DBSPAssignmentExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPQuestionExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPAsExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPVariablePath node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(NoExpression node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPUnsignedWrapExpression node) { return this.preorder(node.to(DBSPExpression.class)); }

    public VisitDecision preorder(DBSPUnsignedUnwrapExpression node) { return this.preorder(node.to(DBSPExpression.class)); }

    // Literals
    public VisitDecision preorder(DBSPLiteral node) {
        return this.preorder(node.to(DBSPExpression.class));
    }

    public VisitDecision preorder(DBSPNullLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }
    
    public VisitDecision preorder(DBSPVecLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPTimestampLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPDateLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPTimeLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPIntervalMillisLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPIntervalMonthsLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPFPLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPRealLiteral node) {
        return this.preorder((DBSPFPLiteral) node);
    }

    public VisitDecision preorder(DBSPUSizeLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPZSetLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPIndexedZSetLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPStrLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPDecimalLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPStringLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPBinaryLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPIntLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPI8Literal node) {
        return this.preorder(node.to(DBSPIntLiteral.class));
    }

    public VisitDecision preorder(DBSPI16Literal node) {
        return this.preorder(node.to(DBSPIntLiteral.class));
    }

    public VisitDecision preorder(DBSPI32Literal node) {
        return this.preorder(node.to(DBSPIntLiteral.class));
    }

    public VisitDecision preorder(DBSPU16Literal node) {
        return this.preorder(node.to(DBSPIntLiteral.class));
    }

    public VisitDecision preorder(DBSPU32Literal node) {
        return this.preorder(node.to(DBSPIntLiteral.class));
    }

    public VisitDecision preorder(DBSPI64Literal node) {
        return this.preorder(node.to(DBSPIntLiteral.class));
    }

    public VisitDecision preorder(DBSPI128Literal node) {
        return this.preorder(node.to(DBSPIntLiteral.class));
    }

    public VisitDecision preorder(DBSPU64Literal node) {
        return this.preorder(node.to(DBSPIntLiteral.class));
    }

    public VisitDecision preorder(DBSPU128Literal node) {
        return this.preorder(node.to(DBSPIntLiteral.class));
    }

    public VisitDecision preorder(DBSPBoolLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPDoubleLiteral node) {
        return this.preorder((DBSPFPLiteral) node);
    }

    public VisitDecision preorder(DBSPKeywordLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPISizeLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    public VisitDecision preorder(DBSPGeoPointLiteral node) {
        return this.preorder(node.to(DBSPLiteral.class));
    }

    /*************************** POSTORDER *****************************/

    @SuppressWarnings("EmptyMethod")
    public void postorder(IDBSPInnerNode ignored) {}

    public void postorder(DBSPAggregate node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    public void postorder(DBSPAggregate.Implementation node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    public void postorder(DBSPExpression node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    public void postorder(DBSPStatement node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    public void postorder(DBSPType node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    public void postorder(DBSPTypeBaseType node) {
        this.postorder(node.to(DBSPType.class));
    }

    public void postorder(DBSPTypeDate node)  {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeTime node)  {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeMillisInterval node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeGeo node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeGeoPoint node) {
        this.postorder(node.to(DBSPTypeGeo.class));
    }

    public void postorder(DBSPFunction node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    public void postorder(DBSPPath node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    public void postorder(DBSPPattern node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    public void postorder(DBSPParameter node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    // Statements

    public void postorder(DBSPItem node) {
        this.postorder(node.to(DBSPStatement.class));
    }

    public void postorder(DBSPConstItem node) {
        this.postorder(node.to(DBSPItem.class));
    }

    public void postorder(DBSPFunctionItem node) {
        this.postorder(node.to(DBSPItem.class));
    }

    public void postorder(DBSPStructItem node) {
        this.postorder(node.to(DBSPItem.class));
    }

    public void postorder(DBSPStructWithHelperItem node) {
        this.postorder(node.to(DBSPItem.class));
    }

    public void postorder(DBSPExpressionStatement node) {
        this.postorder(node.to(DBSPStatement.class));
    }

    public void postorder(DBSPComment node) {
        this.postorder(node.to(DBSPStatement.class));
    }

    public void postorder(DBSPLetStatement node) {
        this.postorder(node.to(DBSPStatement.class));
    }

    // Various

    public void postorder(DBSPPathSegment node) {
        this.postorder(node.to(IDBSPInnerNode.class));
    }

    public void postorder(DBSPSimplePathSegment node) {
        this.postorder(node.to(DBSPPathSegment.class));
    }

    // Types

    public void postorder(DBSPTypeFP node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeReal node) {
        this.postorder(node.to(DBSPTypeFP.class));
    }

    public void postorder(DBSPTypeDouble node) {
        this.postorder(node.to(DBSPTypeFP.class));
    }

    public void postorder(DBSPTypeISize node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeStruct node) {
        this.postorder(node.to(DBSPType.class));
    }

    public void postorder(DBSPTypeStr node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeString node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeBinary node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeUSize node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeTupleBase node) {
        this.postorder(node.to(DBSPType.class));
    }

    public void postorder(DBSPTypeTuple node) {
        this.postorder(node.to(DBSPTypeTupleBase.class));
    }

    public void postorder(DBSPTypeRawTuple node) {
        this.postorder(node.to(DBSPTypeTupleBase.class));
    }

    public void postorder(DBSPTypeTimestamp node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeInteger node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeDecimal node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeNull node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeVoid node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeFunction node) {
        this.postorder(node.to(DBSPType.class));
    }

    public void postorder(DBSPTypeBool node) {
        this.postorder(node.to(DBSPTypeBaseType.class));
    }

    public void postorder(DBSPTypeStream node) {
        this.postorder(node.to(DBSPType.class));
    }

    public void postorder(DBSPTypeUser node) {
        this.postorder(node.to(DBSPType.class));
    }

    public void postorder(DBSPTypeIndexedZSet node) {
        this.postorder(node.to(DBSPTypeUser.class));
    }

    public void postorder(DBSPTypeZSet node) {
        this.postorder(node.to(DBSPTypeUser.class));
    }

    public void postorder(DBSPTypeVec node) {
        this.postorder(node.to(DBSPTypeUser.class));
    }

    public void postorder(DBSPTypeResult node) {
        this.postorder(node.to(DBSPTypeUser.class));
    }

    public void postorder(DBSPTypeTypedBox node) {
        this.postorder(node.to(DBSPTypeUser.class));
    }

    public void postorder(DBSPTypeSemigroup node) {
        this.postorder(node.to(DBSPTypeUser.class));
    }

    public void postorder(DBSPTypeAny node) {
        this.postorder(node.to(DBSPType.class));
    }

    public void postorder(DBSPTypeRef node) {
        this.postorder(node.to(DBSPType.class));
    }

    // Patterns

    public void postorder(DBSPIdentifierPattern node) {
        this.postorder(node.to(DBSPPattern.class));
    }

    // Expressions

    public void postorder(DBSPFlatmap node) {
        this.preorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPSortExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPComparatorExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPNoComparatorExpression node) {
        this.postorder((DBSPComparatorExpression) node);
    }

    public void postorder(DBSPFieldComparatorExpression node) {
        this.postorder((DBSPComparatorExpression) node);
    }

    public void postorder(DBSPConstructorExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPConditionalAggregateExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPBorrowExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPCastExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPCloneExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPSomeExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPIsNullExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPClosureExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPQualifyTypeExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPBinaryExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPEnumValue node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPDerefExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPUnwrapExpression node) {
        this.preorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPPathExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPForExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPUnaryExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPBaseTupleExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPTupleExpression node) {
        this.postorder(node.to(DBSPBaseTupleExpression.class));
    }

    public void postorder(DBSPRawTupleExpression node) {
        this.postorder(node.to(DBSPBaseTupleExpression.class));
    }

    public void postorder(DBSPFieldExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPIfExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPBlockExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPApplyExpression node) {
        this.postorder(node.to(DBSPApplyBaseExpression.class));
    }

    public void postorder(DBSPApplyMethodExpression node) {
        this.postorder(node.to(DBSPApplyBaseExpression.class));
    }

    public void postorder(DBSPAssignmentExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPQuestionExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPAsExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPVariablePath node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(NoExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPUnsignedWrapExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPUnsignedUnwrapExpression node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    // Literals
    public void postorder(DBSPLiteral node) {
        this.postorder(node.to(DBSPExpression.class));
    }

    public void postorder(DBSPTimestampLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPDateLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPTimeLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPIntervalMillisLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPNullLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPVecLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPFPLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPRealLiteral node) {
        this.postorder((DBSPFPLiteral) node);
    }

    public void postorder(DBSPUSizeLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPIndexedZSetLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPZSetLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPStrLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPDecimalLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPStringLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPBinaryLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPIntLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPI8Literal node) {
        this.postorder(node.to(DBSPIntLiteral.class));
    }

    public void postorder(DBSPI16Literal node) {
        this.postorder(node.to(DBSPIntLiteral.class));
    }

    public void postorder(DBSPI32Literal node) {
        this.postorder(node.to(DBSPIntLiteral.class));
    }

    public void postorder(DBSPU16Literal node) {
        this.postorder(node.to(DBSPIntLiteral.class));
    }

    public void postorder(DBSPU32Literal node) {
        this.postorder(node.to(DBSPIntLiteral.class));
    }

    public void postorder(DBSPI64Literal node) {
        this.postorder(node.to(DBSPIntLiteral.class));
    }

    public void postorder(DBSPI128Literal node) {
        this.postorder(node.to(DBSPIntLiteral.class));
    }

    public void postorder(DBSPU64Literal node) {
        this.postorder(node.to(DBSPIntLiteral.class));
    }

    public void postorder(DBSPU128Literal node) {
        this.postorder(node.to(DBSPIntLiteral.class));
    }

    public void postorder(DBSPBoolLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPDoubleLiteral node) {
        this.postorder((DBSPFPLiteral) node);
    }

    public void postorder(DBSPKeywordLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPISizeLiteral node) {
        this.postorder(node.to(DBSPLiteral.class));
    }

    public void postorder(DBSPGeoPointLiteral node)  {
        this.postorder(node.to(DBSPLiteral.class));
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        this.startVisit(node);
        node.accept(this);
        this.endVisit();
        return node;
    }

    public CircuitRewriter getCircuitVisitor() {
        return new CircuitRewriter(this.errorReporter, this);
    }
}
