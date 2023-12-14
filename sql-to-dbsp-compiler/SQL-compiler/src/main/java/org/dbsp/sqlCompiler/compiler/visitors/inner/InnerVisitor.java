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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPPathSegment;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.*;
import org.dbsp.sqlCompiler.ir.statement.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
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
    public void startVisit() {
        Logger.INSTANCE.belowLevel(this, 4)
                .append("Starting ")
                .append(this.toString());
    }

    /**
     * Override to finish after visiting all nodes.
     */
    public void endVisit() {}

    public void traverse(IDBSPInnerNode node) {
        this.startVisit();
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
        return this.preorder((IDBSPInnerNode) node);
    }

    public VisitDecision preorder(DBSPAggregate.Implementation node) {
        return this.preorder((IDBSPInnerNode) node);
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

    public VisitDecision preorder(DBSPTypeWeight node) {
        return this.preorder((DBSPTypeBaseType) node);
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

    public VisitDecision preorder(DBSPTypeVec node) {
        return this.preorder((DBSPTypeUser) node);
    }

    public VisitDecision preorder(DBSPTypeResult node) {
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

    public VisitDecision preorder(DBSPApplyMethodExpression node) {
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

    public VisitDecision preorder(DBSPApplyExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPAssignmentExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPAsExpression node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPVariablePath node) {
        return this.preorder((DBSPExpression) node);
    }

    // Literals
    public VisitDecision preorder(DBSPLiteral node) {
        return this.preorder((DBSPExpression) node);
    }

    public VisitDecision preorder(DBSPNullLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }
    
    public VisitDecision preorder(DBSPVecLiteral node) {
        return this.preorder((DBSPLiteral) node);
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

    public VisitDecision preorder(DBSPZSetLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    public VisitDecision preorder(DBSPIndexedZSetLiteral node) {
        return this.preorder((DBSPLiteral) node);
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

    public VisitDecision preorder(DBSPU32Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPI64Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPU64Literal node) {
        return this.preorder((DBSPIntLiteral) node);
    }

    public VisitDecision preorder(DBSPBoolLiteral node) {
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

    public VisitDecision preorder(DBSPGeoPointLiteral node) {
        return this.preorder((DBSPLiteral) node);
    }

    /*************************** POSTORDER *****************************/

    @SuppressWarnings("EmptyMethod")
    public void postorder(IDBSPInnerNode ignored) {}

    public void postorder(DBSPAggregate node) {
        this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPAggregate.Implementation node) {
        this.postorder((IDBSPInnerNode) node);
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

    public void postorder(DBSPTypeWeight node) {
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

    public void postorder(DBSPTypeVec node) {
        this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeResult node) {
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

    // Patterns

    public void postorder(DBSPIdentifierPattern node) {
        this.postorder((DBSPPattern) node);
    }

    // Expressions

    public void postorder(DBSPFlatmap node) {
        this.preorder((DBSPExpression) node);
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

    public void postorder(DBSPApplyMethodExpression node) {
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

    public void postorder(DBSPApplyExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPAssignmentExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPAsExpression node) {
        this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPVariablePath node) {
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

    public void postorder(DBSPVecLiteral node) {
        this.postorder((DBSPLiteral) node);
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

    public void postorder(DBSPIndexedZSetLiteral node) {
        this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPZSetLiteral node) {
        this.postorder((DBSPLiteral) node);
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

    public void postorder(DBSPU32Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPI64Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPU64Literal node) {
        this.postorder((DBSPIntLiteral) node);
    }

    public void postorder(DBSPBoolLiteral node) {
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

    public void postorder(DBSPGeoPointLiteral node)  {
        this.postorder((DBSPLiteral) node);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        this.startVisit();
        node.accept(this);
        this.endVisit();
        return node;
    }

    public CircuitRewriter getCircuitVisitor() {
        return new CircuitRewriter(this.errorReporter, this);
    }
}
