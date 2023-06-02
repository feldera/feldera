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

package org.dbsp.sqlCompiler.ir;

import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.visitors.CircuitDelegateVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPPathSegment;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPConstItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;

/**
 * Depth-first traversal of an DBSPInnerNode hierarchy.
 */
@SuppressWarnings("SameReturnValue, EmptyMethod")
public abstract class InnerVisitor {
    /// If true each visit call will visit by default the superclass.
    final boolean visitSuper;
    protected final IErrorReporter errorReporter;

    public InnerVisitor(IErrorReporter reporter, boolean visitSuper) {
        this.visitSuper = visitSuper;
        this.errorReporter = reporter;
    }

    /**
     * Override to initialize before visiting any node.
     */
    public void startVisit() {}

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
    public boolean preorder(IDBSPInnerNode ignored) {
        return true;
    }

    public boolean preorder(DBSPAggregate node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPAggregate.Implementation node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPExpression node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPStatement node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPItem node) {
        if (this.visitSuper) return this.preorder((DBSPStatement) node);
        else return true;
    }

    public boolean preorder(DBSPTypeStruct.Field node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPType node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPTypeBaseType node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeWeight node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeDate node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeTime node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeMillisInterval node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeMonthsInterval node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeGeo node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeGeoPoint node) {
        if (this.visitSuper) return this.preorder((DBSPTypeGeo) node);
        else return true;
    }

    public boolean preorder(DBSPFunction node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPPath node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPPattern node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPParameter node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    // Statements
    
    public boolean preorder(DBSPExpressionStatement node) {
        if (this.visitSuper) return this.preorder((DBSPStatement) node);
        else return true;
    }

    public boolean preorder(DBSPComment node) {
        if (this.visitSuper) return this.preorder((DBSPStatement) node);
        else return true;
    }

    public boolean preorder(DBSPLetStatement node) {
        if (this.visitSuper) return this.preorder((DBSPStatement) node);
        else return true;
    }

    public boolean preorder(DBSPConstItem node) {
        if (this.visitSuper) return this.preorder((DBSPItem) node);
        else return true;
    }

    // Various
    
    public boolean preorder(DBSPMatchExpression.Case node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPPathSegment node) {
        if (this.visitSuper) return this.preorder((IDBSPInnerNode) node);
        else return true;
    }

    public boolean preorder(DBSPSimplePathSegment node) {
        if (this.visitSuper) return this.preorder((DBSPPathSegment) node);
        else return true;
    }

    // Types
    
    public boolean preorder(DBSPTypeFP node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeFloat node) {
        if (this.visitSuper) return this.preorder((DBSPTypeFP) node);
        else return true;
    }

    public boolean preorder(DBSPTypeDouble node) {
        if (this.visitSuper) return this.preorder((DBSPTypeFP) node);
        else return true;
    }

    public boolean preorder(DBSPTypeISize node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeStruct node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeString node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }
    
    public boolean preorder(DBSPTypeUSize node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeTupleBase node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeRawTuple node) {
        if (this.visitSuper) return this.preorder((DBSPTypeTupleBase) node);
        else return true;
    }

    public boolean preorder(DBSPTypeTuple node) {
        if (this.visitSuper) return this.preorder((DBSPTypeTupleBase) node);
        else return true;
    }

    public boolean preorder(DBSPTypeStr node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeTimestamp node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeInteger node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeDecimal node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeNull node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeFunction node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeBool node) {
        if (this.visitSuper) return this.preorder((DBSPTypeBaseType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeStream node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }
    
    public boolean preorder(DBSPTypeUser node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeIndexedZSet node) {
        if (this.visitSuper) return this.preorder((DBSPTypeUser) node);
        else return true;
    }

    public boolean preorder(DBSPTypeZSet node) {
        if (this.visitSuper) return this.preorder((DBSPTypeUser) node);
        else return true;
    }

    public boolean preorder(DBSPTypeVec node) {
        if (this.visitSuper) return this.preorder((DBSPTypeUser) node);
        else return true;
    }

    public boolean preorder(DBSPTypeSemigroup node) {
        if (this.visitSuper) return this.preorder((DBSPTypeUser) node);
        else return true;
    }

    public boolean preorder(DBSPTypeAny node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }
    
    public boolean preorder(DBSPTypeRef node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    // Patterns
    public boolean preorder(DBSPTupleStructPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPTuplePattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPRefPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPWildcardPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPLiteralPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPIdentifierPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    // Expressions

    public boolean preorder(DBSPFlatmap node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPSortExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPIndexExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPComparatorExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPNoComparatorExpression node) {
        if (this.visitSuper) return this.preorder((DBSPComparatorExpression) node);
        else return true;
    }

    public boolean preorder(DBSPFieldComparatorExpression node) {
        if (this.visitSuper) return this.preorder((DBSPComparatorExpression) node);
        else return true;
    }

    public boolean preorder(DBSPStructExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPBorrowExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPCastExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPCloneExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPSomeExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPIsNullExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPClosureExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPQualifyTypeExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPMatchExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPBinaryExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPEnumValue node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPDerefExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPApplyMethodExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPPathExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPForExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPUnaryExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPTupleExpression node) {
        if (this.visitSuper) return this.preorder((DBSPBaseTupleExpression) node);
        else return true;
    }

    public boolean preorder(DBSPRawTupleExpression node) {
        if (this.visitSuper) return this.preorder((DBSPBaseTupleExpression) node);
        else return true;
    }

    public boolean preorder(DBSPFieldExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPIfExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPBlockExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPApplyExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPAssignmentExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPAsExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPVariablePath node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPRangeExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }
    
    // Literals
    public boolean preorder(DBSPLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPNullLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }
    
    public boolean preorder(DBSPVecLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPTimestampLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPDateLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPTimeLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPIntervalMillisLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPIntervalMonthsLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPFPLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPFloatLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPFPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPUSizeLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPZSetLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPStrLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPDecimalLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPStringLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPI16Literal node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPI32Literal node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPU32Literal node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPI64Literal node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPU64Literal node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPBoolLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPDoubleLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPFPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPKeywordLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPISizeLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPGeoPointLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    /*************************** POSTORDER *****************************/

    @SuppressWarnings("EmptyMethod")
    public void postorder(IDBSPInnerNode ignored) {}

    public void postorder(DBSPAggregate node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPAggregate.Implementation node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPExpression node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPStatement node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPType node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPTypeBaseType node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeWeight node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeDate node)  {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeTime node)  {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeMillisInterval node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeGeo node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeGeoPoint node) {
        if (this.visitSuper) this.postorder((DBSPTypeGeo) node);
    }

    public void postorder(DBSPFunction node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPPath node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPPattern node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPParameter node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    // Statements

    public void postorder(DBSPItem node) { if (this.visitSuper) this.postorder((DBSPStatement) node);}

    public void postorder(DBSPConstItem node) { if (this.visitSuper) this.postorder((DBSPItem) node);}

    public void postorder(DBSPExpressionStatement node) {
        if (this.visitSuper) this.postorder((DBSPStatement) node);
    }

    public void postorder(DBSPComment node) {
        if (this.visitSuper) this.postorder((DBSPStatement) node);
    }

    public void postorder(DBSPLetStatement node) {
        if (this.visitSuper) this.postorder((DBSPStatement) node);
    }

    // Various

    public void postorder(DBSPMatchExpression.Case node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPPathSegment node) {
        if (this.visitSuper) this.postorder((IDBSPInnerNode) node);
    }

    public void postorder(DBSPSimplePathSegment node) {
        if (this.visitSuper) this.postorder((DBSPPathSegment) node);
    }

    // Types

    public void postorder(DBSPTypeFP node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeFloat node) {
        if (this.visitSuper) this.postorder((DBSPTypeFP) node);
    }

    public void postorder(DBSPTypeDouble node) {
        if (this.visitSuper) this.postorder((DBSPTypeFP) node);
    }

    public void postorder(DBSPTypeISize node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeStruct node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeStr node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeString node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeUSize node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeTupleBase node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeTuple node) {
        if (this.visitSuper) this.postorder((DBSPTypeTupleBase) node);
    }

    public void postorder(DBSPTypeRawTuple node) {
        if (this.visitSuper) this.postorder((DBSPTypeTupleBase) node);
    }

    public void postorder(DBSPTypeTimestamp node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeInteger node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeDecimal node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeNull node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeFunction node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeBool node) {
        if (this.visitSuper) this.postorder((DBSPTypeBaseType) node);
    }

    public void postorder(DBSPTypeStream node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeUser node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeIndexedZSet node) {
        if (this.visitSuper) this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeZSet node) {
        if (this.visitSuper) this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeVec node) {
        if (this.visitSuper) this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeSemigroup node) {
        if (this.visitSuper) this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeAny node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeRef node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    // Patterns
    public void postorder(DBSPTupleStructPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPTuplePattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPRefPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPWildcardPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPLiteralPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPIdentifierPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    // Expressions

    public void postorder(DBSPFlatmap node) {
        if (this.visitSuper) this.preorder((DBSPExpression) node);
    }

    public void postorder(DBSPIndexExpression node) {
        if (this.visitSuper) this.preorder((DBSPExpression) node);
    }

    public void postorder(DBSPSortExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPComparatorExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPNoComparatorExpression node) {
        if (this.visitSuper) this.postorder((DBSPComparatorExpression) node);
    }

    public void postorder(DBSPFieldComparatorExpression node) {
        if (this.visitSuper) this.postorder((DBSPComparatorExpression) node);
    }

    public void postorder(DBSPStructExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBorrowExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPCastExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPCloneExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPSomeExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPIsNullExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPClosureExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPQualifyTypeExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPMatchExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBinaryExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPEnumValue node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPDerefExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPApplyMethodExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPPathExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPForExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPUnaryExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPTupleExpression node) {
        if (this.visitSuper) this.postorder((DBSPBaseTupleExpression) node);
    }

    public void postorder(DBSPRawTupleExpression node) {
        if (this.visitSuper) this.postorder((DBSPBaseTupleExpression) node);
    }

    public void postorder(DBSPFieldExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPIfExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBlockExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPApplyExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPAssignmentExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPAsExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPVariablePath node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPRangeExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    // Literals
    public void postorder(DBSPLiteral node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPTimestampLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPDateLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPTimeLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPIntervalMillisLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPNullLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPVecLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPFPLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPFloatLiteral node) {
        if (this.visitSuper) this.postorder((DBSPFPLiteral) node);
    }

    public void postorder(DBSPUSizeLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPZSetLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPStrLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPDecimalLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPStringLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPI16Literal node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPI32Literal node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPU32Literal node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPI64Literal node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPU64Literal node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPBoolLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPDoubleLiteral node) {
        if (this.visitSuper) this.postorder((DBSPFPLiteral) node);
    }

    public void postorder(DBSPKeywordLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPISizeLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPGeoPointLiteral node)  {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    public CircuitVisitor getCircuitVisitor() {
        return new CircuitDelegateVisitor(this.errorReporter, this);
    }
}
