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

package org.dbsp.sqlCompiler.compiler.frontend;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlRankFunction;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.ddl.SqlAttributeDefinition;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustSqlRuntimeLibrary;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.parser.PropertyList;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragment;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CalciteTableDescription;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateFunctionStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTypeStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DeclareViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DropTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.HasSchema;
import org.dbsp.sqlCompiler.compiler.frontend.statements.LatenessStatement;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlRemove;
import org.dbsp.sqlCompiler.compiler.frontend.statements.TableModifyStatement;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.aggregate.AggregateBase;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdField;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPMapLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructWithHelperItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.IHasZero;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.ICastable;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.IdShuffle;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator.TopKNumbering.*;
import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.SEMIGROUP;
import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

/**
 * The compiler is stateful: it compiles a sequence of SQL statements
 * defining tables and views.  The views must be defined in terms of
 * the previously defined tables and views.  Multiple views can be
 * compiled.  The result is a circuit which has an input for each table
 * and an output for each (non-local) view. */
public class CalciteToDBSPCompiler extends RelVisitor
        implements IWritesLogs, ICompilerComponent {
    // Result is deposited here
    private DBSPCircuit circuit;
    // Map each compiled RelNode operator to its DBSP implementation.
    final Map<RelNode, DBSPSimpleOperator> nodeOperator;
    final TableContents tableContents;
    final CompilerOptions options;
    final DBSPCompiler compiler;
    /** Keep track of position in RelNode tree */
    final List<RelNode> ancestors;
    final ProgramMetadata metadata;
    /** Recursive views, indexed by actual view name (not rewritten name) */
    final Map<ProgramIdentifier, DeclareViewStatement> recursiveViews = new HashMap<>();
    final Map<ProgramIdentifier, Map<ProgramIdentifier, ViewColumnMetadata>> viewMetadata = new HashMap<>();
    /** Current statement that is being compiled */
    @Nullable CreateViewStatement currentView = null;

    /**
     * Create a compiler that translated from calcite to DBSP circuits.
     * @param trackTableContents  If true this compiler will track INSERT and DELETE statements.
     * @param options             Options for compilation.
     * @param compiler            Parent compiler; used to report errors.
     */
    public CalciteToDBSPCompiler(boolean trackTableContents,
                                 CompilerOptions options, DBSPCompiler compiler,
                                 ProgramMetadata metadata) {
        this.circuit = new DBSPCircuit(compiler.metadata);
        this.compiler = compiler;
        this.nodeOperator = new HashMap<>();
        this.tableContents = new TableContents(compiler, trackTableContents);
        this.options = options;
        this.ancestors = new ArrayList<>();
        this.metadata = metadata;
    }

    private void addOperator(DBSPSimpleOperator operator) {
        this.circuit.addOperator(operator);
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
    }

    private DBSPType convertType(RelDataType dt, boolean asStruct) {
        return this.compiler.getTypeCompiler().convertType(dt, asStruct);
    }

    DBSPTypeZSet makeZSet(DBSPType type) {
        return TypeCompiler.makeZSet(type);
    }

    private static DBSPTypeIndexedZSet makeIndexedZSet(DBSPType keyType, DBSPType valueType) {
        return TypeCompiler.makeIndexedZSet(keyType, valueType);
    }

    /** Gets the circuit produced so far and starts a new one. */
    public DBSPCircuit getFinalCircuit() {
        DBSPCircuit result = this.circuit;
        this.circuit = new DBSPCircuit(this.compiler.metadata);
        return result;
    }

    /** This retrieves the operator that is an input.  If the operator may
     * produce multiset results and this is not desired (asMultiset = false),
     * a distinct operator is introduced in the circuit. */
    private DBSPSimpleOperator getInputAs(RelNode input, boolean asMultiset) {
        DBSPSimpleOperator op = this.getOperator(input);
        if (op.isMultiset && !asMultiset) {
            op = new DBSPStreamDistinctOperator(CalciteObject.create(input), op.outputPort());
            this.addOperator(op);
        }
        return op;
    }

    <T> boolean visitIfMatches(RelNode node, Class<T> clazz, Consumer<T> method) {
        T value = ICastable.as(node, clazz);
        if (value != null) {
            Logger.INSTANCE.belowLevel(this, 4)
                    .append("Processing ")
                    .append(node.toString())
                    .newline();
            method.accept(value);
            return true;
        }
        return false;
    }

    /**
     * Helper function for creating aggregates.
     * @param node         RelNode that generates this aggregate.
     * @param aggregates   Aggregates to implement.
     * @param groupCount   Number of groupBy variables.
     * @param inputRowType Type of input row.
     * @param resultType   Type of result produced.
     * @param linearAllowed If true linear aggregates are allowed (a linear aggregate can
     *                      also be implemented as a non-linear aggregate).
     */
    public static List<DBSPAggregate> createAggregates(DBSPCompiler compiler,
            RelNode node, List<AggregateCall> aggregates, DBSPTypeTuple resultType,
            DBSPType inputRowType, int groupCount, ImmutableBitSet groupKeys, boolean linearAllowed) {
        if (aggregates.isEmpty())
            return Linq.list();
        CalciteObject obj = CalciteObject.create(node);
        DBSPVariablePath rowVar = inputRowType.ref().var();
        // Each aggregate here will contain a sequence of compatible AggregateBase operations.
        List<DBSPAggregate> results = new ArrayList<>();
        int aggIndex = 0;
        // A list of compatible aggregates
        List<AggregateBase> currentGroup = new ArrayList<>();

        for (AggregateCall call: aggregates) {
            DBSPType resultFieldType = resultType.getFieldType(aggIndex + groupCount);
            AggregateCompiler aggCompiler = new AggregateCompiler(node,
                    compiler, call, resultFieldType, rowVar, groupKeys, linearAllowed);
            AggregateBase implementation = aggCompiler.compile();
            if (!currentGroup.isEmpty() && !Utilities.last(currentGroup).compatible(implementation)) {
                DBSPAggregate aggregate = new DBSPAggregate(obj, rowVar, currentGroup);
                results.add(aggregate);
                currentGroup = new ArrayList<>();
            }
            currentGroup.add(implementation);
            aggIndex++;
        }
        // Add all the left-over aggregates
        DBSPAggregate aggregate = new DBSPAggregate(obj, rowVar, currentGroup);
        results.add(aggregate);
        return results;
    }

    UnimplementedException decorrelateError(CalciteObject node) {
        return new UnimplementedException(
                "It looks like the compiler could not decorrelate this query.", 2555, node);
    }

    void visitCorrelate(LogicalCorrelate correlate) {
        // We decorrelate queries using Calcite's optimizer, which doesn't always work.
        // In particular, it won't decorrelate queries with unnest.
        // Here we check for unnest-type queries.  We assume that unnest queries
        // have a restricted plan of this form:
        // LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{...}])
        //    LeftSubquery (arbitrary)
        //    Uncollect
        //      LogicalProject(COL=[$cor0.ARRAY])  // uncollectInput
        //        LogicalValues(tuples=[[{ 0 }]])
        // or
        // LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{...}])
        //    LeftSubquery
        //    LogicalProject // rightProject
        //      Uncollect
        //        LogicalProject(COL=[$cor0.ARRAY])  // uncollectInput
        //          LogicalValues(tuples=[[{ 0 }]])
        // Instead of projecting and joining again we directly apply flatmap.
        // The translation for this is:
        // stream.flat_map({
        //   move |x: &Tuple2<Vec<i32>, Option<i32>>, | -> _ {
        //     let xA: Vec<i32> = x.0.clone();
        //     let xB: x.1.clone();
        //     x.0.clone().into_iter().map({
        //        move |e: i32, | -> Tuple3<Vec<i32>, Option<i32>, i32> {
        //            Tuple3::new(xA.clone(), xB.clone(), e)
        //        }
        //     })
        //  });
        CalciteObject node = CalciteObject.create(correlate);
        DBSPTypeTuple type = this.convertType(correlate.getRowType(), false).to(DBSPTypeTuple.class);
        if (correlate.getJoinType().isOuterJoin())
            throw this.decorrelateError(node);

        this.visit(correlate.getLeft(), 0, correlate);
        DBSPSimpleOperator left = this.getInputAs(correlate.getLeft(), true);
        DBSPTypeTuple leftElementType = left.getOutputZSetElementType().to(DBSPTypeTuple.class);

        RelNode correlateRight = correlate.getRight();
        Project rightProject = null;
        if (correlateRight instanceof Project) {
            rightProject = (Project) correlateRight;
            correlateRight = rightProject.getInput(0);
        }
        if (!(correlateRight instanceof Uncollect uncollect))
            throw this.decorrelateError(node);
        CalciteObject uncollectNode = CalciteObject.create(uncollect);
        RelNode uncollectInput = uncollect.getInput();
        if (!(uncollectInput instanceof LogicalProject project))
            throw this.decorrelateError(node);
        if (project.getProjects().size() != 1)
            throw this.decorrelateError(node);
        RexNode projection = project.getProjects().get(0);
        DBSPVariablePath dataVar = new DBSPVariablePath(leftElementType.ref());
        ExpressionCompiler eComp = new ExpressionCompiler(correlate, dataVar, this.compiler);
        DBSPClosureExpression arrayExpression = eComp.compile(projection).closure(dataVar);
        DBSPTypeTuple uncollectElementType = this.convertType(uncollect.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPType arrayElementType = arrayExpression.getResultType().to(DBSPTypeVec.class).getElementType();
        if (arrayElementType.mayBeNull)
            // This seems to be a bug in Calcite, we should not need to do this adjustment
            uncollectElementType = uncollectElementType.withMayBeNull(true).to(DBSPTypeTuple.class);

        DBSPType indexType = null;
        if (uncollect.withOrdinality) {
            // Index field is always last.
            indexType = uncollectElementType.getFieldType(uncollectElementType.size() - 1);
        }

        // Right projections are applied after uncollect
        List<DBSPClosureExpression> rightProjections = null;
        if (rightProject != null) {
            DBSPVariablePath eVar = new DBSPVariablePath(uncollectElementType.ref());
            final ExpressionCompiler eComp0 = new ExpressionCompiler(correlate, eVar, this.compiler);
            rightProjections = Linq.map(rightProject.getProjects(), e -> eComp0.compile(e).closure(eVar));
        }

        DBSPTypeFunction functionType = new DBSPTypeFunction(type, leftElementType.ref());
        DBSPFlatmap flatmap = new DBSPFlatmap(node, functionType, leftElementType, arrayExpression,
                Linq.range(0, leftElementType.size()),
                rightProjections, indexType, new IdShuffle());
        DBSPFlatMapOperator flatMap = new DBSPFlatMapOperator(uncollectNode,
                flatmap, TypeCompiler.makeZSet(type), left.outputPort());
        this.assignOperator(correlate, flatMap);
    }

    /** Given a DESCRIPTOR RexCall, return the reference to the single colum
     * that is referred by the DESCRIPTOR. */
    int getDescriptor(RexNode descriptor) {
        assert descriptor instanceof RexCall;
        RexCall call = (RexCall) descriptor;
        assert call.getKind() == SqlKind.DESCRIPTOR;
        assert call.getOperands().size() == 1;
        RexNode operand = call.getOperands().get(0);
        assert operand instanceof RexInputRef;
        RexInputRef ref = (RexInputRef) operand;
        return ref.getIndex();
    }

    void compileTumble(LogicalTableFunctionScan scan, RexCall call) {
        CalciteObject node = CalciteObject.create(scan);
        DBSPTypeTuple type = this.convertType(scan.getRowType(), false).to(DBSPTypeTuple.class);
        assert scan.getInputs().size() == 1;
        RelNode input = scan.getInput(0);
        DBSPTypeTuple inputRowType = this.convertType(input.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        // This is the same as a LogicalProject which adds two columns
        // If the timestamps are nullable, filter away null timestamps
        List<RexNode> operands = call.getOperands();
        int timestampIndex = this.getDescriptor(operands.get(0));
        DBSPType tsType = inputRowType.getFieldType(timestampIndex);
        if (tsType.mayBeNull) {
            DBSPVariablePath row = inputRowType.ref().var();
            DBSPExpression filter = row.deref().field(timestampIndex).is_null().not();
            opInput = new DBSPFilterOperator(node, filter.closure(row), opInput.outputPort());
            this.addOperator(opInput);
        }

        DBSPVariablePath row = inputRowType.ref().var();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(scan, row, this.compiler);
        assert call.operandCount() == 2 || call.operandCount() == 3;
        DBSPExpression interval = expressionCompiler.compile(operands.get(1));
        Simplify simplify = new Simplify(this.compiler.compiler());
        interval = simplify.apply(interval).to(DBSPExpression.class);
        if (interval.getType().is(DBSPTypeMonthsInterval.class)) {
            throw new UnsupportedException(
                    "Tumbling window intervals must be 'short' SQL intervals (days and lower)",
                    interval.getNode());
        }
        if (!interval.is(DBSPIntervalMillisLiteral.class)) {
            throw new UnsupportedException(
                    "Tumbling window interval must be a constant",
                    interval.getNode());
        }
        DBSPIntervalMillisLiteral intValue = interval.to(DBSPIntervalMillisLiteral.class);
        if (!intValue.gt0()) {
            throw new UnsupportedException(
                    "Tumbling window interval must be positive",
                    interval.getNode());
        }
        DBSPExpression start = null;
        if (call.operandCount() == 3)
            start = expressionCompiler.compile(operands.get(2));

        DBSPExpression[] results = new DBSPExpression[inputRowType.size() + 2];
        for (int i = 0; i < inputRowType.size(); i++) {
            results[i] = row.deref().field(i).applyCloneIfNeeded();
        }
        int nextIndex = inputRowType.size();

        List<DBSPExpression> tumbleArguments = new ArrayList<>();
        tumbleArguments.add(row.deref().field(timestampIndex).unwrapIfNullable());
        tumbleArguments.add(interval);
        if (start != null)
            tumbleArguments.add(start);
        DBSPType tumbleType = type.tupFields[nextIndex];
        results[nextIndex] = ExpressionCompiler.compilePolymorphicFunction(
                "tumble", node, tumbleType, tumbleArguments, 2, 3);
        results[nextIndex + 1] = ExpressionCompiler.makeBinaryExpression(node,
                tumbleType, DBSPOpcode.ADD, results[nextIndex], interval);

        DBSPClosureExpression func = new DBSPTupleExpression(results).closure(row);
        DBSPMapOperator result = new DBSPMapOperator(node, func, makeZSet(type), opInput.outputPort());
        this.assignOperator(scan, result);
    }

    void compileHop(LogicalTableFunctionScan scan, RexCall call) {
        CalciteObject node = CalciteObject.create(scan);
        DBSPTypeTuple type = this.convertType(scan.getRowType(), false).to(DBSPTypeTuple.class);
        assert scan.getInputs().size() == 1;
        RelNode input = scan.getInput(0);
        DBSPTypeTuple inputRowType = this.convertType(input.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        DBSPVariablePath row = inputRowType.ref().var();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(scan, row, this.compiler);
        List<RexNode> operands = call.getOperands();
        assert call.operandCount() == 3 || call.operandCount() == 4;
        int timestampIndex = this.getDescriptor(operands.get(0));
        // Need to do a few more checks, Calcite should be doing these really.
        DBSPExpression interval = expressionCompiler.compile(operands.get(1));
        if (!interval.getType().is(DBSPTypeMillisInterval.class)) {
            throw new UnsupportedException("Hopping window intervals must be 'short' SQL intervals (days and lower)",
                    interval.getNode());
        }
        DBSPExpression start;
        DBSPExpression size = expressionCompiler.compile(operands.get(2));
        if (!size.getType().is(DBSPTypeMillisInterval.class)) {
            throw new UnsupportedException("Hopping window intervals must be 'short' SQL intervals (days and lower)",
                    size.getNode());
        }
        if (call.operandCount() == 4)
            start = expressionCompiler.compile(operands.get(3));
        else
            start = new DBSPIntervalMillisLiteral(0, false);

        DBSPHopOperator hop = new DBSPHopOperator(
                node, timestampIndex, interval, start, size, makeZSet(type), opInput.outputPort());
        this.assignOperator(scan, hop);
    }

    void visitTableFunction(LogicalTableFunctionScan tf) {
        CalciteObject node = CalciteObject.create(tf);
        RexNode operation = tf.getCall();
        assert operation instanceof RexCall;
        RexCall call = (RexCall) operation;
        switch (call.getOperator().getName()) {
            case "HOP":
                this.compileHop(tf, call);
                return;
            case "TUMBLE":
                this.compileTumble(tf, call);
                return;
            default:
                break;
        }
        throw new UnimplementedException("Table function " + tf + " not yet implemented", node);
    }

    void visitUncollect(Uncollect uncollect) {
        // This represents an unnest:
        // flat_map(move |x| { x.0.into_iter().map(move |e| Tup1::new(e)) })
        // or, with ordinality:
        // flat_map(move |x| { x.0.into_iter().map(move |e, i| Tup2::new(e, i+1)) })
        CalciteObject node = CalciteObject.create(uncollect);
        DBSPType type = this.convertType(uncollect.getRowType(), false);
        RelNode input = uncollect.getInput();
        DBSPTypeTuple inputRowType = this.convertType(input.getRowType(), false).to(DBSPTypeTuple.class);
        // We expect this to be a single-element tuple whose type is a vector.
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        DBSPType indexType = null;
        if (uncollect.withOrdinality) {
            DBSPTypeTuple pair = type.to(DBSPTypeTuple.class);
            indexType = pair.getFieldType(1);
        }
        DBSPVariablePath data = new DBSPVariablePath(inputRowType.ref());
        DBSPClosureExpression getField0 = data.deref().field(0).closure(data);
        DBSPTypeFunction functionType = new DBSPTypeFunction(type, inputRowType.ref());
        DBSPFlatmap function = new DBSPFlatmap(node, functionType, inputRowType, getField0,
                Linq.list(), null, indexType, new IdShuffle());
        DBSPFlatMapOperator flatMap = new DBSPFlatMapOperator(node, function,
                TypeCompiler.makeZSet(type), opInput.outputPort());
        this.assignOperator(uncollect, flatMap);
    }

    /** Generate a list of the groupings that have to be evaluated for all aggregates */
    @SuppressWarnings("unused")
    List<ImmutableBitSet> planGroups(
            ImmutableBitSet groupSet,
            ImmutableList<ImmutableBitSet> groupSets) {
        // Decreasing order of cardinality
        List<ImmutableBitSet> ordered = new ArrayList<>(groupSets);
        ordered.sort(Comparator.comparingInt(ImmutableBitSet::cardinality));
        Collections.reverse(ordered);
        return new ArrayList<>(ordered);
    }

    /** Given a list of fields and a tuple t, generate a tuple expression that extracts
     * all these fields from t.
     * @param keyFields Fields of the variable that appear in the key
     * @param groupSet  All fields that correspond to the 'slice' type fields
     * @param t      Variable whose fields are used to create the key
     * @param slice  Type expected for the entire groupByKeys */
    DBSPTupleExpression generateKeyExpression(
            ImmutableBitSet keyFields, ImmutableBitSet groupSet,
            DBSPVariablePath t, DBSPTypeTuple slice) {
        assert groupSet.cardinality() == slice.size();
        DBSPExpression[] keys = new DBSPExpression[keyFields.cardinality()];
        int keyFieldIndex = 0;
        int groupSetIndex = 0;
        for (int index : groupSet) {
            if (keyFields.get(index)) {
                keys[keyFieldIndex] = t.deepCopy()
                        .deref()
                        .field(index)
                        .applyCloneIfNeeded()
                        .cast(slice.getFieldType(groupSetIndex));
                keyFieldIndex++;
            }
            groupSetIndex++;
        }
        return new DBSPTupleExpression(keys);
    }

    DBSPSimpleOperator joinAllAggregates(
            CalciteObject node, DBSPType groupKeyType, OutputPort indexedInput,
            List<DBSPAggregate> aggregates) {
        DBSPSimpleOperator result = null;
        for (DBSPAggregate agg : aggregates) {
            // We synthesize each aggregate and repeatedly join it with the previous result
            // TODO: Today the aggregate computes the fields in the order they are expected in the output,
            // but there are additional optimization opportunities if we reorder these fields.

            // The aggregate operator will not return a stream of type aggType, but a stream
            // with a type given by fd.defaultZero.
            DBSPTypeTuple typeFromAggregate = agg.getEmptySetResultType();
            DBSPTypeIndexedZSet aggregateType = makeIndexedZSet(groupKeyType, typeFromAggregate);

            DBSPSimpleOperator aggOp;
            if (agg.isLinear()) {
                // incremental-only operator
                DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, indexedInput);
                this.addOperator(diff);
                LinearAggregate linear = agg.asLinear(this.compiler());
                aggOp = new DBSPAggregateLinearPostprocessOperator(
                        node, aggregateType, linear.map, linear.postProcess, diff.outputPort());
                this.addOperator(aggOp);
                aggOp = new DBSPIntegrateOperator(node, aggOp.outputPort());
            } else {
                aggOp = new DBSPStreamAggregateOperator(
                        node, aggregateType, null, agg, indexedInput);
            }
            this.addOperator(aggOp);

            if (result == null) {
                result = aggOp;
            } else {
                // Create a join to combine the fields
                DBSPTypeTupleBase valueType = result.getOutputIndexedZSetType().elementType.to(DBSPTypeTuple.class);
                DBSPVariablePath left = valueType.ref().var();
                valueType = valueType.concat(typeFromAggregate);
                DBSPTypeIndexedZSet joinOutputType = makeIndexedZSet(groupKeyType, valueType);

                DBSPVariablePath key = groupKeyType.ref().var();
                DBSPVariablePath right = typeFromAggregate.ref().var();
                DBSPExpression body = new DBSPRawTupleExpression(key.deref().applyCloneIfNeeded(),
                        DBSPTupleExpression.flatten(
                                left.deref().applyCloneIfNeeded(), right.deref().applyCloneIfNeeded()));
                DBSPClosureExpression appendFields = body.closure(
                        key, left, right);

                result = new DBSPStreamJoinIndexOperator(
                        node, joinOutputType, appendFields, false, result.outputPort(), aggOp.outputPort());
                this.addOperator(result);
            }
        }
        return Objects.requireNonNull(result);
    }

    /** Implement one aggregate from a set of rollups described by a LogicalAggregate. */
    OutputPort implementOneAggregate(LogicalAggregate aggregate, ImmutableBitSet localKeys) {
        CalciteObject node = CalciteObject.create(aggregate);
        RelNode input = aggregate.getInput();
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        List<AggregateCall> aggregateCalls = aggregate.getAggCallList();

        DBSPType type = this.convertType(aggregate.getRowType(), false);
        DBSPTypeTuple tuple = type.to(DBSPTypeTuple.class);
        DBSPType inputRowType = this.convertType(input.getRowType(), false);
        DBSPVariablePath t = inputRowType.ref().var();
        DBSPTypeTuple keySlice = tuple.slice(0, aggregate.getGroupSet().cardinality());
        DBSPTupleExpression globalKeys = this.generateKeyExpression(
                aggregate.getGroupSet(), aggregate.getGroupSet(), t, keySlice);
        DBSPType[] aggTypes = Utilities.arraySlice(tuple.tupFields, aggregate.getGroupCount());
        DBSPTypeTuple aggType = new DBSPTypeTuple(aggTypes);

        DBSPTupleExpression localKeyExpression = this.generateKeyExpression(
                localKeys, aggregate.getGroupSet(), t, keySlice);
        DBSPTypeTuple type1 = inputRowType.to(DBSPTypeTuple.class);
        DBSPExpression[] expressions = new DBSPExpression[]{t.deref()};
        DBSPTupleExpression expr = DBSPTupleExpression.flatten(expressions);
        DBSPClosureExpression makeKeys =
                new DBSPRawTupleExpression(
                        localKeyExpression,
                        new DBSPTupleExpression(expr.getNode(), type1, Objects.requireNonNull(expr.fields))).closure(t);
        DBSPType localGroupType = localKeyExpression.getType();
        DBSPTypeIndexedZSet localGroupAndInput = makeIndexedZSet(localGroupType, inputRowType);
        DBSPSimpleOperator indexedInput = new DBSPMapIndexOperator(
                node, makeKeys, localGroupAndInput, opInput.outputPort());
        this.addOperator(indexedInput);

        DBSPSimpleOperator result;
        List<DBSPAggregate> aggregates = createAggregates(this.compiler(),
                aggregate, aggregateCalls, tuple, inputRowType,
                aggregate.getGroupCount(), localKeys, true);
        if (aggregates.isEmpty()) {
            // No aggregations: just apply distinct
            DBSPVariablePath var = localGroupAndInput.getKVRefType().var();
            DBSPExpression addEmpty = new DBSPRawTupleExpression(
                    var.field(0).deref().applyCloneIfNeeded(),
                    new DBSPTupleExpression());
            DBSPSimpleOperator ix2 = new DBSPMapIndexOperator(node, addEmpty.closure(var),
                    makeIndexedZSet(localGroupType, new DBSPTypeTuple()), indexedInput.outputPort());
            this.addOperator(ix2);
            result = new DBSPStreamDistinctOperator(node, ix2.outputPort());
            this.addOperator(result);
        } else {
            result = this.joinAllAggregates(node, localGroupType, indexedInput.outputPort(), aggregates);
        }

        // Adjust the key such that all local groups are converted to have the same keys as the
        // global group.  This is used as part of the rollup.
        if (!localKeys.equals(aggregate.getGroupSet())) {
            // Generate a new key where each field that is in the groupKeys but not in the local is a null.
            DBSPVariablePath reindexVar = result.getOutputIndexedZSetType().getKVRefType().var();
            DBSPExpression[] reindexFields = new DBSPExpression[aggregate.getGroupCount()];
            int localIndex = 0;
            int i = 0;
            for (int globalIndex : aggregate.getGroupSet()) {
                if (localKeys.get(globalIndex)) {
                    reindexFields[globalIndex] = reindexVar
                            .field(0)
                            .deref()
                            .field(localIndex)
                            .applyCloneIfNeeded();
                    localIndex++;
                } else {
                    assert i < reindexFields.length;
                    assert globalKeys.fields != null;
                    reindexFields[i] = DBSPLiteral.none(globalKeys.fields[i].getType());
                }
                i++;
            }
            DBSPExpression remap = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(reindexFields),
                    reindexVar.field(1).deref().applyCloneIfNeeded());
            result = new DBSPMapIndexOperator(
                    node, remap.closure(reindexVar),
                    new DBSPTypeIndexedZSet(remap.getType().to(DBSPTypeRawTuple.class)), result.outputPort());
            this.addOperator(result);
        }

        // Flatten the resulting set and adjust the types
        DBSPTypeTupleBase kvType = new DBSPTypeRawTuple(globalKeys.getType().ref(),
                result.getOutputIndexedZSetType().elementType.ref());
        DBSPVariablePath kv = kvType.var();
        DBSPExpression[] flattenFields = new DBSPExpression[aggregate.getGroupCount() + aggType.size()];
        for (int i = 0; i < aggregate.getGroupCount(); i++) {
            DBSPExpression cast = kv.deepCopy().field(0).deref().field(i).applyCloneIfNeeded()
                    .cast(tuple.getFieldType(i));
            flattenFields[i] = cast;
        }
        for (int i = 0; i < aggType.size(); i++) {
            DBSPExpression flattenField = kv.deepCopy().field(1).deref().field(i).applyCloneIfNeeded();
            // Here we correct from the type produced by the Folder (typeFromAggregate) to the
            // actual expected type aggType (which is the tuple of aggTypes).
            DBSPExpression cast = flattenField.cast(aggTypes[i]);
            flattenFields[aggregate.getGroupCount() + i] = cast;
        }

        DBSPExpression mapper = new DBSPTupleExpression(node, tuple, flattenFields).closure(kv);
        DBSPSimpleOperator map = new DBSPMapOperator(node, mapper, this.makeZSet(tuple), result.outputPort());
        this.addOperator(map);
        if (aggregate.getGroupCount() != 0 || aggregateCalls.isEmpty()) {
            return map.outputPort();
        }

        DBSPExpression emptySetResult =
                DBSPTupleExpression.flatten(Linq.map(aggregates, DBSPAggregate::getEmptySetResult));
        DBSPAggregateZeroOperator zero = new DBSPAggregateZeroOperator(node, emptySetResult, map.outputPort());
        this.addOperator(zero);
        return zero.outputPort();
    }

    /** Implement a LogicalAggregate.  The LogicalAggregate can contain a rollup,
     * described by a set of groups.  The aggregate is computed for each group,
     * and the results are combined. */
    void visitAggregate(LogicalAggregate aggregate) {
        CalciteObject node = CalciteObject.create(aggregate);
        List<ImmutableBitSet> plan = this.planGroups(
                aggregate.getGroupSet(), aggregate.getGroupSets());

        // Optimize for the case of
        // - no aggregates
        // - no rollup
        // This is just a DISTINCT call - this is how Calcite represents it.
        if (aggregate.getAggCallList().isEmpty() && plan.size() == 1) {
            // No aggregates, this is how DISTINCT is represented.
            RelNode input = aggregate.getInput();
            DBSPSimpleOperator opInput = this.getInputAs(input, true);
            DBSPSimpleOperator result = new DBSPStreamDistinctOperator(node, opInput.outputPort());
            this.assignOperator(aggregate, result);
        } else {
            // One aggregate for each group
            List<OutputPort> aggregates = Linq.map(plan, b -> this.implementOneAggregate(aggregate, b));
            // The result is the sum of all aggregates
            DBSPSimpleOperator sum = new DBSPSumOperator(node, aggregates);
            this.assignOperator(aggregate, sum);
        }
    }

    void visitScan(TableScan scan, boolean create) {
        CalciteObject node = CalciteObject.create(scan);

        RelOptTable tbl = scan.getTable();
        ProgramIdentifier tableName = null;
        if (tbl instanceof RelOptTableImpl impl) {
            CalciteTableDescription descr = impl.unwrap(CalciteTableDescription.class);
            if (descr != null)
                tableName = descr.getName();
        }
        if (tableName == null)
            tableName = Utilities.toIdentifier(scan.getTable().getQualifiedName());
        @Nullable
        DBSPSimpleOperator source = this.circuit.getInput(tableName);
        // The inputs should have been created while parsing the CREATE TABLE statements.
        if (source != null) {
            // Multiple queries can share an input.
            // Or the input may have been created by a CREATE TABLE statement.
            Utilities.putNew(this.nodeOperator, scan, source);
        } else {
            // Try a view if no table with this name exists.
            source = this.circuit.getView(tableName);
            if (source != null) {
                Utilities.putNew(this.nodeOperator, scan, source);
            } else {
                if (!create)
                    throw new InternalCompilerError("Could not find operator for table " + tableName, node);

                // Create external tables table
                JdbcTableScan jscan = (JdbcTableScan) scan;
                RelDataType tableRowType = jscan.jdbcTable.getRowType(this.compiler.frontend.typeFactory);
                DBSPTypeStruct originalRowType = this.convertType(tableRowType, true)
                        .to(DBSPTypeStruct.class)
                        .rename(tableName);
                DBSPType rowType = originalRowType.toTuple();
                HasSchema withSchema = new HasSchema(CalciteObject.EMPTY, tableName, tableRowType);
                this.metadata.addTable(withSchema);
                TableMetadata tableMeta = new TableMetadata(
                        tableName, Linq.map(withSchema.getColumns(), this::convertMetadata), Linq.list(),
                        false, false);
                source = new DBSPSourceMultisetOperator(
                        node, CalciteObject.EMPTY,
                        this.makeZSet(rowType), originalRowType,
                        tableMeta, tableName, null);
                this.addOperator(source);
                Utilities.putNew(this.nodeOperator, scan, source);
            }
        }
    }

    void assignOperator(RelNode rel, DBSPSimpleOperator op) {
        Utilities.putNew(this.nodeOperator, rel, op);
        this.addOperator(op);
    }

    DBSPSimpleOperator getOperator(RelNode node) {
        return Utilities.getExists(this.nodeOperator, node);
    }

    void visitProject(LogicalProject project) {
        CalciteObject node = CalciteObject.create(project);
        RelNode input = project.getInput();
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        DBSPType outputElementType = this.convertType(project.getRowType(), false);
        DBSPTypeTuple tuple = outputElementType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(project.getInput().getRowType(), false);
        DBSPVariablePath row = inputType.ref().var();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(project, row, this.compiler);

        DBSPExpression[] resultColumns = new DBSPExpression[tuple.size()];
        int index = 0;
        for (RexNode column : project.getProjects()) {
            if (column instanceof RexOver) {
                throw new UnsupportedException("Optimizer should have removed OVER expressions",
                        CalciteObject.create(project, column));
            } else {
                DBSPExpression exp = expressionCompiler.compile(column);
                DBSPType expectedType = tuple.getFieldType(index);
                if (!exp.getType().sameType(expectedType)) {
                    // Calcite's optimizations do not preserve types!
                    exp = exp.applyCloneIfNeeded().cast(expectedType);
                }
                resultColumns[index] = exp;
                index++;
            }
        }
        DBSPExpression exp = new DBSPTupleExpression(node, tuple, resultColumns);
        DBSPExpression mapFunc = new DBSPClosureExpression(node, exp, row.asParameter());
        DBSPMapOperator op = new DBSPMapOperator(
                node, mapFunc, this.makeZSet(outputElementType), opInput.outputPort());
        // No distinct needed - in SQL project may produce a multiset.
        this.assignOperator(project, op);
    }

    DBSPSimpleOperator castOutput(CalciteObject node, DBSPSimpleOperator operator, DBSPType outputElementType) {
        DBSPType inputElementType = operator.getOutputZSetElementType();
        if (inputElementType.sameType(outputElementType))
            return operator;
        DBSPExpression function = inputElementType.caster(outputElementType);
        DBSPSimpleOperator map = new DBSPMapOperator(
                node, function, this.makeZSet(outputElementType), operator.outputPort());
        this.addOperator(map);
        return map;
    }

    private void visitUnion(LogicalUnion union) {
        CalciteObject node = CalciteObject.create(union);
        RelDataType rowType = union.getRowType();
        DBSPType outputType = this.convertType(rowType, false);
        List<DBSPSimpleOperator> inputs = Linq.map(union.getInputs(), this::getOperator);
        // input type nullability may not match
        List<OutputPort> ports = Linq.map(inputs, o -> this.castOutput(node, o, outputType).outputPort());
        DBSPSumOperator sum = new DBSPSumOperator(node, ports);
        if (union.all) {
            this.assignOperator(union, sum);
        } else {
            this.addOperator(sum);
            DBSPStreamDistinctOperator d = new DBSPStreamDistinctOperator(node, sum.outputPort());
            this.assignOperator(union, d);
        }
    }

    private void visitMinus(LogicalMinus minus) {
        CalciteObject node = CalciteObject.create(minus);
        boolean first = true;
        RelDataType rowType = minus.getRowType();
        DBSPType outputType = this.convertType(rowType, false);
        List<OutputPort> inputs = new ArrayList<>();
        for (RelNode input : minus.getInputs()) {
            DBSPSimpleOperator opInput = this.getInputAs(input, false);
            if (!first) {
                DBSPSimpleOperator neg = new DBSPNegateOperator(node, opInput.outputPort());
                neg = this.castOutput(node, neg, outputType);
                this.addOperator(neg);
                inputs.add(neg.outputPort());
            } else {
                opInput = this.castOutput(node, opInput, outputType);
                inputs.add(opInput.outputPort());
            }
            first = false;
        }

        DBSPSumOperator sum = new DBSPSumOperator(node, inputs);
        if (minus.all) {
            this.assignOperator(minus, sum);
        } else {
            this.addOperator(sum);
            DBSPStreamDistinctOperator d = new DBSPStreamDistinctOperator(node, sum.outputPort());
            this.assignOperator(minus, d);
        }
    }

    void visitFilter(LogicalFilter filter) {
        // If this filter is already implemented, use it.
        // This comes from the implementation of Filter(Window)
        // that compiles to nested TopK, which is detected in Window.
        if (this.filterImplementation != null) {
            Utilities.putNew(this.nodeOperator, filter, this.filterImplementation);
            this.filterImplementation = null;
            return;
        }

        CalciteObject node = CalciteObject.create(filter);
        DBSPType type = this.convertType(filter.getRowType(), false);
        DBSPVariablePath t = type.ref().var();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(filter, t, this.compiler);
        DBSPExpression condition = expressionCompiler.compile(filter.getCondition());
        condition = ExpressionCompiler.wrapBoolIfNeeded(condition);
        condition = new DBSPClosureExpression(
                CalciteObject.create(filter, filter.getCondition()), condition, t.asParameter());
        DBSPSimpleOperator input = this.getOperator(filter.getInput());
        DBSPFilterOperator fop = new DBSPFilterOperator(node, condition, input.outputPort());
        this.assignOperator(filter, fop);
    }

    /** Apply a filter on a set of fields, keeping only rows that have non-null values in all these fields
     * @param join    Join node that requires filtering the inputs.
     * @param fields  List of fields to filter.
     * @param input   Join input node that is being filtered.
     * @return        An operator that performs the filtering.  If none of the fields are
     *                nullable, the original input is returned. */
    private DBSPSimpleOperator filterNonNullFields(Join join, List<Integer> fields, DBSPSimpleOperator input) {
        CalciteObject node = CalciteObject.create(join);
        DBSPTypeTuple rowType = input.getType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        boolean shouldFilter = Linq.any(fields, i -> rowType.tupFields[i].mayBeNull);
        if (!shouldFilter) return input;

        DBSPVariablePath var = rowType.ref().var();
        // Build a condition that checks whether any of the key fields is null.
        @Nullable
        DBSPExpression condition = null;
        for (int i = 0; i < rowType.size(); i++) {
            if (fields.contains(i)) {
                DBSPFieldExpression field = var.deref().field(i);
                DBSPExpression expr = field.is_null();
                if (condition == null)
                    condition = expr;
                else
                    condition = new DBSPBinaryExpression(
                            node, new DBSPTypeBool(CalciteObject.EMPTY, false), DBSPOpcode.OR, condition, expr);
            }
        }

        Objects.requireNonNull(condition);
        condition = condition.not();
        DBSPClosureExpression filterFunc = condition.closure(var);
        DBSPFilterOperator filter = new DBSPFilterOperator(node, filterFunc, input.outputPort());
        this.addOperator(filter);
        return filter;
    }

    private void visitJoin(LogicalJoin join) {
        CalciteObject node = CalciteObject.create(join);
        JoinRelType joinType = join.getJoinType();
        if (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI)
            throw new UnimplementedException("JOIN of type " + joinType + " not yet implemented", node);

        DBSPTypeTuple resultType = this.convertType(join.getRowType(), false).to(DBSPTypeTuple.class);
        if (join.getInputs().size() != 2)
            throw new InternalCompilerError("Unexpected join with " + join.getInputs().size() + " inputs", node);
        DBSPSimpleOperator left = this.getInputAs(join.getInput(0), true);
        DBSPSimpleOperator right = this.getInputAs(join.getInput(1), true);
        DBSPTypeTuple leftElementType = left.getType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);

        JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(
                leftElementType.to(DBSPTypeTuple.class).size(), this.compiler.getTypeCompiler());
        JoinConditionAnalyzer.ConditionDecomposition decomposition = analyzer.analyze(join, join.getCondition());
        // If any key field that is compared with = is nullable we need to filter the inputs;
        // this will make some key columns non-nullable
        DBSPSimpleOperator filteredLeft = this.filterNonNullFields(join,
                Linq.map(Linq.where(decomposition.comparisons, JoinConditionAnalyzer.EqualityTest::nonNull),
                        JoinConditionAnalyzer.EqualityTest::leftColumn), left);
        DBSPSimpleOperator filteredRight = this.filterNonNullFields(join,
                Linq.map(Linq.where(decomposition.comparisons, JoinConditionAnalyzer.EqualityTest::nonNull),
                        JoinConditionAnalyzer.EqualityTest::rightColumn), right);

        DBSPTypeTuple rightElementType = filteredRight.getType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);

        int leftColumns = leftElementType.size();
        int rightColumns = rightElementType.size();
        int totalColumns = leftColumns + rightColumns;
        DBSPTypeTuple leftResultType = resultType.slice(0, leftColumns);
        DBSPTypeTuple rightResultType = resultType.slice(leftColumns, leftColumns + rightColumns);
        // Map a left variable field that is used as key field into the corresponding key field index
        Map<Integer, Integer> lkf = new HashMap<>();
        Map<Integer, Integer> rkf = new HashMap<>();
        {
            int index = 0;
            for (var x : decomposition.comparisons) {
                Utilities.putNew(lkf, x.leftColumn(), index);
                Utilities.putNew(rkf, x.rightColumn(), index);
                index++;
            }
        }

        DBSPMapIndexOperator leftIndex, rightIndex;
        DBSPType keyType, leftTupleType, rightTupleType;
        {
            DBSPVariablePath l = leftElementType.ref().var();
            DBSPVariablePath r = rightElementType.ref().var();
            List<DBSPExpression> leftKeyFields = Linq.map(
                    decomposition.comparisons,
                    c -> l.deepCopy().deref().field(c.leftColumn()).applyCloneIfNeeded().cast(c.commonType()));
            List<DBSPExpression> rightKeyFields = Linq.map(
                    decomposition.comparisons,
                    c -> r.deepCopy().deref().field(c.rightColumn()).applyCloneIfNeeded().cast(c.commonType()));
            DBSPExpression leftKey = new DBSPTupleExpression(node, leftKeyFields);
            keyType = leftKey.getType();

            // Copy all the fields from the except the partition fields
            List<DBSPExpression> leftFields = new ArrayList<>();
            List<DBSPExpression> rightFields = new ArrayList<>();
            for (int i = 0; i < leftElementType.size(); i++) {
                if (lkf.containsKey(i))
                    continue;
                leftFields.add(l.deepCopy().deref().field(i).applyCloneIfNeeded());
            }
            for (int i = 0; i < rightElementType.size(); i++) {
                if (rkf.containsKey(i))
                    continue;
                rightFields.add(r.deepCopy().deref().field(i).applyCloneIfNeeded());
            }
            DBSPTupleExpression leftTuple = new DBSPTupleExpression(leftFields, false);
            DBSPTupleExpression rightTuple = new DBSPTupleExpression(rightFields, false);
            leftTupleType = leftTuple.getType();
            rightTupleType = rightTuple.getType();
            DBSPClosureExpression toLeftKey = new DBSPRawTupleExpression(leftKey, leftTuple)
                    .closure(l);
            leftIndex = new DBSPMapIndexOperator(
                    node, toLeftKey,
                    makeIndexedZSet(leftKey.getType(), leftTuple.getType()), false, filteredLeft.outputPort());
            this.addOperator(leftIndex);

            DBSPExpression rightKey = new DBSPTupleExpression(node, rightKeyFields);
            DBSPClosureExpression toRightKey = new DBSPRawTupleExpression(rightKey, rightTuple)
                    .closure(r);
            rightIndex = new DBSPMapIndexOperator(
                    node, toRightKey,
                    makeIndexedZSet(rightKey.getType(), rightTuple.getType()), false, filteredRight.outputPort());
            this.addOperator(rightIndex);
        }

        DBSPSimpleOperator joinResult;
        DBSPSimpleOperator inner;
        DBSPTypeTuple lrType;
        {
            DBSPVariablePath k = keyType.ref().var();
            DBSPVariablePath l0 = leftTupleType.ref().var();
            DBSPVariablePath r0 = rightTupleType.ref().var();

            List<DBSPExpression> joinFields = new ArrayList<>();
            int skipped = 0;
            for (int i = 0; i < leftElementType.size(); i++) {
                if (lkf.containsKey(i)) {
                    joinFields.add(k.deepCopy().deref().field(lkf.get(i)).applyCloneIfNeeded());
                    skipped++;
                } else {
                    joinFields.add(l0.deepCopy().deref().field(i - skipped).applyCloneIfNeeded());
                }
            }
            skipped = 0;
            for (int i = 0; i < rightElementType.size(); i++) {
                if (rkf.containsKey(i)) {
                    joinFields.add(k.deepCopy().deref().field(rkf.get(i)).applyCloneIfNeeded());
                    skipped++;
                } else {
                    joinFields.add(r0.deepCopy().deref().field(i - skipped).applyCloneIfNeeded());
                }
            }
            DBSPTupleExpression lr = new DBSPTupleExpression(joinFields, false);
            lrType = lr.getType().to(DBSPTypeTuple.class);
            @Nullable
            RexNode leftOver = decomposition.getLeftOver();
            DBSPExpression condition = null;
            DBSPExpression originalCondition = null;
            if (leftOver != null) {
                DBSPVariablePath t = lr.getType().ref().var();
                ExpressionCompiler expressionCompiler = new ExpressionCompiler(join, t, this.compiler);
                condition = expressionCompiler.compile(leftOver);
                if (condition.getType().mayBeNull)
                    condition = ExpressionCompiler.wrapBoolIfNeeded(condition);
                originalCondition = condition;
                condition = new DBSPClosureExpression(
                        CalciteObject.create(join, join.getCondition()), condition, t.asParameter());
            }

            DBSPClosureExpression makeTuple = lr.closure(k, l0, r0);
             joinResult = new DBSPStreamJoinOperator(node, this.makeZSet(lr.getType()),
                    makeTuple, left.isMultiset || right.isMultiset,
                     leftIndex.outputPort(), rightIndex.outputPort());

            // Save the result of the inner join here
            inner = joinResult;
            if (originalCondition != null) {
                // Apply additional filters
                DBSPBoolLiteral bLit = originalCondition.as(DBSPBoolLiteral.class);
                if (bLit == null || bLit.value == null || !bLit.value) {
                    // Technically if blit.value == null or !blit.value then
                    // the filter is false, and the result is empty.  But hopefully
                    // the calcite optimizer won't allow that.
                    this.addOperator(joinResult);
                    inner = new DBSPFilterOperator(node, condition, joinResult.outputPort());
                    joinResult = inner;
                }
                // if bLit it true we don't need to filter.
            }
        }

        if (!resultType.sameType(lrType)) {
            // For outer joins additional columns may become nullable.
            DBSPVariablePath t = new DBSPVariablePath(lrType.ref());
            DBSPExpression[] casts = new DBSPExpression[lrType.size()];
            for (int index = 0; index < lrType.size(); index++) {
                casts[index] = t.deepCopy().deref().field(index).applyCloneIfNeeded().cast(resultType.getFieldType(index));
            }
            DBSPTupleExpression allFields = new DBSPTupleExpression(casts);
            this.addOperator(joinResult);
            joinResult = new DBSPMapOperator(node, allFields.closure(t),
                    this.makeZSet(resultType), joinResult.outputPort());
        }

        // Handle outer joins
        DBSPSimpleOperator result = joinResult;
        DBSPVariablePath joinVar = lrType.ref().var();
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
            this.addOperator(result);
            // project the join on the left columns
            DBSPClosureExpression toLeftColumns =
                    DBSPTupleExpression.flatten(joinVar.deref())
                            .slice(0, leftColumns)
                            .pointwiseCast(leftResultType).closure(joinVar);
            DBSPSimpleOperator joinLeftColumns = new DBSPMapOperator(
                    node, toLeftColumns, this.makeZSet(leftResultType), inner.outputPort());
            this.addOperator(joinLeftColumns);
            DBSPSimpleOperator distJoin = new DBSPStreamDistinctOperator(node, joinLeftColumns.outputPort());
            this.addOperator(distJoin);

            // subtract from left relation
            DBSPSimpleOperator leftCast = left;
            DBSPVariablePath l1 = leftElementType.ref().var();
            if (!leftResultType.sameType(leftElementType)) {
                DBSPClosureExpression castLeft =
                    DBSPTupleExpression.flatten(l1.deref())
                            .pointwiseCast(leftResultType).closure(l1);
                leftCast = new DBSPMapOperator(node, castLeft, this.makeZSet(leftResultType), left.outputPort());
                this.addOperator(leftCast);
            }
            DBSPSimpleOperator sub = new DBSPSubtractOperator(node, leftCast.outputPort(), distJoin.outputPort());
            this.addOperator(sub);
            DBSPStreamDistinctOperator dist = new DBSPStreamDistinctOperator(node, sub.outputPort());
            this.addOperator(dist);

            // fill nulls in the right relation fields
            DBSPTupleExpression rEmpty = new DBSPTupleExpression(
                    Linq.map(rightElementType.tupFields,
                             et -> DBSPLiteral.none(et.withMayBeNull(true)), DBSPExpression.class));
            DBSPVariablePath lCasted = leftResultType.ref().var();
            DBSPClosureExpression leftRow = DBSPTupleExpression.flatten(lCasted.deref(), rEmpty).closure(
                    lCasted.asParameter());
            DBSPSimpleOperator expand = new DBSPMapOperator(node, leftRow, this.makeZSet(resultType), dist.outputPort());
            this.addOperator(expand);
            result = new DBSPSumOperator(node, result.outputPort(), expand.outputPort());
        }
        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
            this.addOperator(result);

            // project the join on the right columns
            DBSPClosureExpression toRightColumns =
                    DBSPTupleExpression.flatten(joinVar.deref())
                            .slice(leftColumns, totalColumns)
                            .pointwiseCast(rightResultType).closure(
                    joinVar);
            DBSPSimpleOperator joinRightColumns = new DBSPMapOperator(
                    node, toRightColumns, this.makeZSet(rightResultType), inner.outputPort());
            this.addOperator(joinRightColumns);
            DBSPSimpleOperator distJoin = new DBSPStreamDistinctOperator(node, joinRightColumns.outputPort());
            this.addOperator(distJoin);

            // subtract from right relation
            DBSPSimpleOperator rightCast = right;
            DBSPVariablePath r1 = rightElementType.ref().var();
            if (!rightResultType.sameType(rightElementType)) {
                DBSPClosureExpression castRight =
                        DBSPTupleExpression.flatten(r1.deref())
                                .pointwiseCast(rightResultType).closure(
                        r1);
                rightCast = new DBSPMapOperator(node, castRight, this.makeZSet(rightResultType), right.outputPort());
                this.addOperator(rightCast);
            }
            DBSPSimpleOperator sub = new DBSPSubtractOperator(node, rightCast.outputPort(), distJoin.outputPort());
            this.addOperator(sub);
            DBSPStreamDistinctOperator dist = new DBSPStreamDistinctOperator(node, sub.outputPort());
            this.addOperator(dist);

            // fill nulls in the left relation fields
            DBSPTupleExpression lEmpty = new DBSPTupleExpression(
                    Linq.map(leftElementType.tupFields,
                            et -> DBSPLiteral.none(et.withMayBeNull(true)), DBSPExpression.class));
            DBSPVariablePath rCasted = rightResultType.ref().var();
            DBSPClosureExpression rightRow =
                    DBSPTupleExpression.flatten(lEmpty, rCasted.deref()).closure(rCasted);
            DBSPSimpleOperator expand = new DBSPMapOperator(node,
                    rightRow, this.makeZSet(resultType), dist.outputPort());
            this.addOperator(expand);
            result = new DBSPSumOperator(node, result.outputPort(), expand.outputPort());
        }

        this.assignOperator(join, Objects.requireNonNull(result));
    }

    private void visitAsofJoin(LogicalAsofJoin join) {
        // This shares a lot of code with the LogicalJoin
        CalciteObject node = CalciteObject.create(join);
        JoinRelType joinType = join.getJoinType();
        if (joinType != JoinRelType.LEFT_ASOF)
            throw new UnimplementedException("Currently only LEFT ASOF joins are supported.", 2212, node);

        DBSPTypeTuple resultType = this.convertType(join.getRowType(), false).to(DBSPTypeTuple.class);
        if (join.getInputs().size() != 2)
            throw new InternalCompilerError("Unexpected join with " + join.getInputs().size() + " inputs", node);
        DBSPSimpleOperator left = this.getInputAs(join.getInput(0), true);
        DBSPSimpleOperator right = this.getInputAs(join.getInput(1), true);
        DBSPTypeTuple leftElementType = left.getType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);
        int leftSize = leftElementType.size();

        JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(
                leftElementType.to(DBSPTypeTuple.class).size(), this.compiler.getTypeCompiler());
        JoinConditionAnalyzer.ConditionDecomposition decomposition = analyzer.analyze(join, join.getCondition());
        @Nullable
        RexNode leftOver = decomposition.getLeftOver();
        assert leftOver == null;

        int leftTsIndex;  // Index of "timestamp" column in left input
        int rightTsIndex;
        SqlKind comparison;  // Comparison operation, with left table column always on the left
        boolean needsLeftCast = false;
        boolean needsRightCast = false;

        {
            // Analyze match condition.  We know it's always a simple comparison between
            // two columns (enforced by the Validator).
            RexNode matchCondition = join.getMatchCondition();
            assert matchCondition instanceof RexCall;
            RexCall call = (RexCall) matchCondition;
            comparison = call.getOperator().getKind();
            assert SqlKind.ORDER_COMPARISON.contains(comparison);
            List<RexNode> operands = call.getOperands();
            assert operands.size() == 2;
            RexNode leftCompared = operands.get(0);
            RexNode rightCompared = operands.get(1);
            RexNode lc = leftCompared;
            RexNode rc = rightCompared;

            if (leftCompared instanceof RexCall lrc) {
                assert lrc.getKind() == SqlKind.CAST;
                needsLeftCast = true;
                lc = lrc.getOperands().get(0);
            }

            if (rightCompared instanceof RexCall rrc) {
                assert rrc.getKind() == SqlKind.CAST;
                needsRightCast = true;
                rc = rrc.getOperands().get(0);
            }
            assert lc instanceof RexInputRef;
            assert rc instanceof RexInputRef;
            RexInputRef li = (RexInputRef) lc;
            RexInputRef ri = (RexInputRef) rc;

            if (li.getIndex() < leftSize) {
                leftTsIndex = li.getIndex();
                rightTsIndex = ri.getIndex() - leftSize;
            } else {
                leftTsIndex = ri.getIndex();
                rightTsIndex = li.getIndex() - leftSize;
                comparison = comparison.reverse();
            }

            // If the comparison involves casts we compute them in an extra map stage prior to the actual join
            if (needsLeftCast) {
                DBSPVariablePath l = leftElementType.ref().var();
                ExpressionCompiler compiler = new ExpressionCompiler(join, l, this.compiler);
                RexCall compared = (RexCall) leftCompared;
                leftCompared = compared.clone(compared.getType(), Linq.list(
                        new RexInputRef(leftTsIndex, compared.operands.get(0).getType())));
                DBSPExpression leftCast = compiler.compile(leftCompared);
                DBSPExpression addCast = DBSPTupleExpression.flatten(l.deref()).append(leftCast);
                left = new DBSPMapOperator(
                        node, addCast.closure(l), this.makeZSet(addCast.getType()), left.outputPort());
                this.addOperator(left);
                leftTsIndex = leftElementType.size();
            }
            if (needsRightCast) {
                DBSPTypeTuple rightElementType = right.getType().to(DBSPTypeZSet.class).elementType
                        .to(DBSPTypeTuple.class);
                DBSPVariablePath r = rightElementType.ref().var();
                ExpressionCompiler compiler = new ExpressionCompiler(join, r, this.compiler);
                RexCall compared = (RexCall) rightCompared;
                rightCompared = compared.clone(compared.getType(), Linq.list(
                        new RexInputRef(rightTsIndex, compared.operands.get(0).getType())));
                DBSPExpression rightCast = compiler.compile(rightCompared);
                DBSPExpression addCast = DBSPTupleExpression.flatten(r.deref()).append(rightCast);
                right = new DBSPMapOperator(
                        node, addCast.closure(r), this.makeZSet(addCast.getType()), right.outputPort());
                this.addOperator(right);
                rightTsIndex = rightElementType.size();
            }
        }

        // Throw away all rows in the right collection that have a NULL in a join key column
        // or in the timestamp column.  They can never surface in the join output.
        List<JoinConditionAnalyzer.EqualityTest> comparisons = decomposition.comparisons;
        List<Integer> rightFilteredColumns = Linq.map(comparisons, JoinConditionAnalyzer.EqualityTest::rightColumn);
        rightFilteredColumns.add(rightTsIndex);
        DBSPSimpleOperator filteredRight = this.filterNonNullFields(join, rightFilteredColumns, right);
        DBSPTypeTuple rightElementType = filteredRight.getType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);

        // We don't filter nulls in the left column, because this may be a left join,
        // and thus the resulting types may be nullable.
        // The 'commonType' in 'comparisons' are never null.
        for (int i = 0; i < comparisons.size(); i++) {
            JoinConditionAnalyzer.EqualityTest test = comparisons.get(i);
            DBSPType leftType = leftElementType.tupFields[i];
            DBSPType useType = test.commonType().withMayBeNull(leftType.mayBeNull);
            comparisons.set(i, test.withType(useType));
        }

        DBSPSimpleOperator rightIndex, leftIndex;
        DBSPTypeTuple keyType;
        {
            // Index both inputs
            DBSPVariablePath l = leftElementType.ref().var();
            DBSPVariablePath r = rightElementType.ref().var();
            List<DBSPExpression> leftKeyFields = Linq.map(
                    comparisons,
                    c -> l.deepCopy().deref().field(c.leftColumn()).applyCloneIfNeeded().cast(c.commonType()));
            List<DBSPExpression> rightKeyFields = Linq.map(
                    comparisons,
                    c -> r.deepCopy().deref().field(c.rightColumn()).applyCloneIfNeeded().cast(c.commonType()));
            DBSPExpression leftKey = new DBSPTupleExpression(node, leftKeyFields);
            keyType = leftKey.getType().to(DBSPTypeTuple.class);
            DBSPExpression rightKey = new DBSPTupleExpression(node, rightKeyFields);

            // Index left input
            DBSPTupleExpression tuple = DBSPTupleExpression.flatten(l.deref());
            DBSPComparatorExpression leftComparator =
                    new DBSPNoComparatorExpression(node, leftElementType)
                            .field(leftTsIndex, true);
            DBSPExpression wrapper = new DBSPCustomOrdExpression(node, tuple, leftComparator);
            DBSPClosureExpression toLeftKey =
                    new DBSPRawTupleExpression(leftKey, wrapper)
                    .closure(l);
            leftIndex = new DBSPMapIndexOperator(
                    node, toLeftKey,
                    makeIndexedZSet(leftKey.getType(), wrapper.getType()), false, left.outputPort());
            this.addOperator(leftIndex);

            // Index right input
            DBSPComparatorExpression rightComparator =
                    new DBSPNoComparatorExpression(node, rightElementType)
                            .field(rightTsIndex, true);
            wrapper = new DBSPCustomOrdExpression(node,
                    DBSPTupleExpression.flatten(r.deref()), rightComparator);
            DBSPClosureExpression toRightKey = new DBSPRawTupleExpression(rightKey, wrapper)
                    .closure(r);
            rightIndex = new DBSPMapIndexOperator(
                    node, toRightKey,
                    makeIndexedZSet(rightKey.getType(), wrapper.getType()),
                    false, filteredRight.outputPort());
            this.addOperator(rightIndex);

            // ASOF joins are only incremental, so need to differentiate the inputs
            rightIndex = new DBSPDifferentiateOperator(node, rightIndex.outputPort());
            this.addOperator(rightIndex);
            leftIndex = new DBSPDifferentiateOperator(node, leftIndex.outputPort());
            this.addOperator(leftIndex);
        }

        DBSPType wrappedLeftType = new DBSPTypeWithCustomOrd(node, leftElementType);
        DBSPType wrappedRightType = new DBSPTypeWithCustomOrd(node, rightElementType);

        DBSPVariablePath leftVar = wrappedLeftType.ref().var();
        DBSPVariablePath rightVar = wrappedRightType.ref().var();

        DBSPExpression leftTS = new DBSPUnwrapCustomOrdExpression(leftVar.deref())
                .field(leftTsIndex);
        DBSPExpression rightTS = new DBSPUnwrapCustomOrdExpression(rightVar.deref())
                .field(rightTsIndex);

        DBSPType leftTSType = leftTS.getType();
        DBSPType rightTSType = rightTS.getType();
        // Rust expects both timestamps to have the same type
        boolean nullable = leftTSType.mayBeNull || rightTSType.mayBeNull;
        DBSPType commonTSType = leftTSType.withMayBeNull(nullable);
        assert commonTSType.sameType(rightTSType.withMayBeNull(nullable));

        DBSPClosureExpression leftTimestamp = leftTS.cast(commonTSType).closure(leftVar);
        DBSPClosureExpression rightTimestamp = rightTS.cast(commonTSType).closure(rightVar);

        // Currently not used
        DBSPComparatorExpression comparator = new DBSPNoComparatorExpression(node, leftTimestamp.getResultType());
        boolean ascending = comparison == SqlKind.GREATER_THAN || comparison == SqlKind.GREATER_THAN_OR_EQUAL;
        if (comparison != SqlKind.GREATER_THAN_OR_EQUAL) {
            // Not yet supported by DBSP
            throw new UnimplementedException(
                    "Currently the only MATCH_CONDITION comparison supported by ASOF joins is '>='", 2212, node);
        }
        comparator = new DBSPDirectComparatorExpression(node, comparator, ascending);

        DBSPVariablePath k = keyType.ref().var();
        DBSPVariablePath l0 = wrappedLeftType.ref().var();
        // Signature of the function is (k: &K, l: &L, r: Option<&R>)
        DBSPVariablePath r0 = wrappedRightType.ref().withMayBeNull(true).var();
        List<DBSPExpression> lrFields = new ArrayList<>();
        // Don't forget to drop the added field
        for (int i = 0; i < leftElementType.size() - (needsLeftCast ? 1 : 0); i++)
            lrFields.add(new DBSPUnwrapCustomOrdExpression(l0.deepCopy().deref()).field(i));
        for (int i = 0; i < rightElementType.size() - (needsRightCast ? 1 : 0); i++)
            lrFields.add(new DBSPCustomOrdField(r0.deepCopy(), i));
        DBSPTupleExpression lr = new DBSPTupleExpression(lrFields, false);
        DBSPClosureExpression makeTuple = lr.closure(k, l0, r0);
        DBSPSimpleOperator result = new DBSPAsofJoinOperator(node, this.makeZSet(resultType),
                makeTuple, leftTimestamp, rightTimestamp, comparator,
                left.isMultiset || right.isMultiset, join.getJoinType().isOuterJoin(),
                leftIndex.outputPort(), rightIndex.outputPort());
        this.addOperator(result);

        result = new DBSPIntegrateOperator(node, result.outputPort());
        this.assignOperator(join, Objects.requireNonNull(result));
    }

    private void visitCollect(Collect collect) {
        CalciteObject node = CalciteObject.create(collect);
        DBSPTypeTuple type = this.convertType(collect.getRowType(), false).to(DBSPTypeTuple.class);
        assert collect.getInputs().size() == 1;
        assert type.size() == 1;
        RelNode input = collect.getInput(0);
        DBSPTypeTuple inputRowType = this.convertType(input.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        DBSPVariablePath row = inputRowType.ref().var();
        AggregateBase agg;

        // Index by the empty tuple
        DBSPExpression indexingFunction = new DBSPRawTupleExpression(
                new DBSPTupleExpression(),
                        new DBSPTupleExpression(DBSPTypeTuple.flatten(row.deref()), false));
        DBSPSimpleOperator index = new DBSPMapIndexOperator(node, indexingFunction.closure(row),
                new DBSPTypeIndexedZSet(node, new DBSPTypeTuple(), inputRowType), opInput.outputPort());
        this.addOperator(index);

        switch (collect.getCollectionType()) {
            case ARRAY: {
                // specialized version of ARRAY_AGG
                boolean flatten = inputRowType.size() == 1;
                assert type.getFieldType(0).is(DBSPTypeVec.class);
                DBSPTypeVec vecType = type.getFieldType(0).to(DBSPTypeVec.class);
                DBSPType elementType = vecType.getElementType();
                assert elementType.sameType(flatten ? inputRowType.getFieldType(0) : inputRowType);

                row = inputRowType.ref().var();
                DBSPExpression zero = DBSPVecLiteral.emptyWithElementType(elementType, type.mayBeNull);
                DBSPVariablePath accumulator = vecType.var();
                String functionName;
                DBSPExpression[] arguments;
                functionName = "array_agg" + vecType.nullableSuffix();
                arguments = new DBSPExpression[5];
                arguments[0] = accumulator.borrow(true);
                arguments[1] = flatten ? row.deref().field(0) : row.deref().applyCloneIfNeeded();
                arguments[2] = this.compiler.weightVar;
                arguments[3] = new DBSPBoolLiteral(false);
                arguments[4] = new DBSPBoolLiteral(true);
                DBSPExpression increment = new DBSPApplyExpression(node, functionName, vecType, arguments);
                DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "ConcatSemigroup", false, accumulator.getType());
                agg = new NonLinearAggregate(
                        node, zero, increment.closure(accumulator, row, this.compiler.weightVar),
                        zero, semigroup);
                break;
            }
            case MAP: {
                assert type.getFieldType(0).is(DBSPTypeMap.class);
                DBSPTypeMap mapType = type.getFieldType(0).to(DBSPTypeMap.class);
                DBSPType keyType = mapType.getKeyType();
                DBSPType valueType = mapType.getValueType();
                assert inputRowType.size() == 2;
                assert keyType.sameType(inputRowType.getFieldType(0));
                assert valueType.sameType(inputRowType.getFieldType(1));

                row = inputRowType.ref().var();
                DBSPExpression zero = new DBSPMapLiteral(mapType, Linq.list());
                DBSPVariablePath accumulator = mapType.var();
                String functionName;
                DBSPExpression[] arguments;
                functionName = "map_agg" + mapType.nullableSuffix();
                arguments = new DBSPExpression[3];
                arguments[0] = accumulator.borrow(true);
                arguments[1] = row.deref().applyCloneIfNeeded();
                arguments[2] = this.compiler.weightVar;
                DBSPExpression increment = new DBSPApplyExpression(node, functionName, mapType, arguments);
                DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "ConcatSemigroup", false, accumulator.getType());
                agg = new NonLinearAggregate(
                        node, zero, increment.closure(accumulator, row, this.compiler.weightVar),
                        zero, semigroup);
                break;
            }
            default:
                throw new UnimplementedException("Aggregation to " + collect.getCollectionType() +
                        " not yet implemented", node);
        }
        DBSPAggregate aggregate = new DBSPAggregate(node, row, Linq.list(agg));
        DBSPSimpleOperator aggregateOperator = new DBSPAggregateOperator(
                node, new DBSPTypeIndexedZSet(node, new DBSPTypeTuple(), type), null, aggregate, index.outputPort());
        this.addOperator(aggregateOperator);

        DBSPSimpleOperator deindex = new DBSPDeindexOperator(node, aggregateOperator.outputPort());
        this.assignOperator(collect, deindex);
    }

    @Nullable
    ModifyTableTranslation modifyTableTranslation;

    /**
     * Visit a LogicalValue: a SQL literal, as produced by a VALUES expression.
     * This can be invoked by a DDM statement, or by a SQL query that computes a constant result.
     */
    void visitLogicalValues(LogicalValues values) {
        CalciteObject node = CalciteObject.create(values);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(values, null, this.compiler);
        DBSPTypeTuple sourceType = this.convertType(values.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPTypeTuple resultType;
        if (this.modifyTableTranslation != null) {
            resultType = this.modifyTableTranslation.getResultType();
            if (sourceType.size() != resultType.size())
                throw new InternalCompilerError("Expected a tuple with " + resultType.size() +
                        " values but got " + values, node);
        } else {
            resultType = sourceType;
        }

        DBSPZSetLiteral result = DBSPZSetLiteral.emptyWithElementType(resultType);
        for (List<RexLiteral> t : values.getTuples()) {
            List<DBSPExpression> expressions = new ArrayList<>();
            if (t.size() != sourceType.size())
                throw new InternalCompilerError("Expected a tuple with " + sourceType.size() +
                        " values but got " + t, node);
            int i = 0;
            for (RexLiteral rl : t) {
                DBSPType resultFieldType = resultType.tupFields[i];
                DBSPExpression expr = expressionCompiler.compile(rl);
                if (expr.is(DBSPLiteral.class)) {
                    // The expression compiler does not actually have type information
                    // so the nulls produced will have the wrong type.
                    DBSPLiteral lit = expr.to(DBSPLiteral.class);
                    if (lit.isNull())
                        expr = DBSPLiteral.none(resultFieldType);
                }
                if (!expr.getType().sameType(resultFieldType)) {
                    DBSPExpression cast = expr.cast(resultFieldType);
                    expressions.add(cast);
                } else {
                    expressions.add(expr);
                }
                i++;
            }
            DBSPTupleExpression expression = new DBSPTupleExpression(node, expressions);
            result.add(expression);
        }

        if (this.modifyTableTranslation != null) {
            this.modifyTableTranslation.setResult(result);
        } else {
            // We currently don't have a reliable way to check whether there are duplicates
            // in the Z-set, so we assume it is a multiset
            DBSPSimpleOperator constant = new DBSPConstantOperator(node, result, false, true);
            this.assignOperator(values, constant);
        }
    }

    void visitIntersect(LogicalIntersect intersect) {
        CalciteObject node = CalciteObject.create(intersect);
        // Intersect is a special case of join.
        List<RelNode> inputs = intersect.getInputs();
        RelNode input = intersect.getInput(0);
        DBSPSimpleOperator previous = this.getInputAs(input, false);

        if (inputs.isEmpty())
            throw new UnsupportedException(node);
        if (inputs.size() == 1) {
            Utilities.putNew(this.nodeOperator, intersect, previous);
            return;
        }

        DBSPType inputRowType = this.convertType(input.getRowType(), false);
        DBSPTypeTuple resultType = this.convertType(intersect.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPVariablePath t = inputRowType.ref().var();
        DBSPClosureExpression entireKey =
                new DBSPRawTupleExpression(
                        t.deref().applyClone(),
                        new DBSPTupleExpression()).closure(t);
        DBSPVariablePath l = new DBSPTypeTuple().ref().var();
        DBSPVariablePath r = new DBSPTypeTuple().ref().var();
        DBSPVariablePath k = inputRowType.ref().var();

        DBSPClosureExpression closure = k.deref().applyClone().closure(k, l, r);
        for (int i = 1; i < inputs.size(); i++) {
            DBSPSimpleOperator previousIndex = new DBSPMapIndexOperator(
                    node, entireKey,
                    makeIndexedZSet(inputRowType, new DBSPTypeTuple()),
                    previous.outputPort());
            this.addOperator(previousIndex);
            DBSPSimpleOperator inputI = this.getInputAs(intersect.getInput(i), false);
            DBSPSimpleOperator index = new DBSPMapIndexOperator(
                    node, entireKey.deepCopy().to(DBSPClosureExpression.class),
                    makeIndexedZSet(inputRowType, new DBSPTypeTuple()),
                    inputI.outputPort());
            this.addOperator(index);
            previous = new DBSPStreamJoinOperator(node, this.makeZSet(resultType),
                    closure, false, previousIndex.outputPort(), index.outputPort());
            this.addOperator(previous);
        }
        Utilities.putNew(this.nodeOperator, intersect, previous);
    }

    /** If this is not null, the parent LogicalFilter should use this implementation
     * instead of generating code. */
    @Nullable
    DBSPSimpleOperator filterImplementation = null;

    /** Index the data according to the keys specified by a window operator */
    DBSPSimpleOperator indexWindow(LogicalWindow window, Window.Group group) {
        CalciteObject node = CalciteObject.create(window);
        // This code duplicates code from the SortNode.
        RelNode input = window.getInput();
        DBSPSimpleOperator opInput = this.getOperator(input);
        DBSPType inputRowType = this.convertType(input.getRowType(), false);
        DBSPVariablePath t = inputRowType.ref().var();
        DBSPExpression[] fields = new DBSPExpression[group.keys.cardinality()];
        int ix = 0;
        for (int field : group.keys.toList()) {
            fields[ix++] = t.deepCopy().deref().field(field).applyCloneIfNeeded();
        }
        DBSPTupleExpression tuple = new DBSPTupleExpression(fields);
        DBSPClosureExpression groupKeys =
                new DBSPRawTupleExpression(
                        tuple, DBSPTupleExpression.flatten(t.deref())).closure(t);
        DBSPSimpleOperator index = new DBSPMapIndexOperator(
                node, groupKeys,
                makeIndexedZSet(tuple.getType(), inputRowType),
                opInput.outputPort());
        this.addOperator(index);
        return index;
    }

    static DBSPComparatorExpression generateComparator(
            CalciteObject node, List<RelFieldCollation> collations, DBSPType comparedFields) {
        DBSPComparatorExpression comparator = new DBSPNoComparatorExpression(node, comparedFields);
        for (RelFieldCollation collation : collations) {
            int field = collation.getFieldIndex();
            RelFieldCollation.Direction direction = collation.getDirection();
            boolean ascending = switch (direction) {
                case ASCENDING -> true;
                case DESCENDING -> false;
                default -> throw new UnimplementedException("Sort direction " + direction + " not yet implemented",
                        comparator.getNode());
            };
            comparator = comparator.field(field, ascending);
        }
        return comparator;
    }

    void generateNestedTopK(LogicalWindow window, Window.Group group, int limit, SqlKind kind) {
        CalciteObject node = CalciteObject.create(window);
        DBSPIndexedTopKOperator.TopKNumbering numbering = switch (kind) {
            case RANK -> RANK;
            case DENSE_RANK -> DENSE_RANK;
            case ROW_NUMBER -> ROW_NUMBER;
            default -> throw new UnimplementedException(
                    "Ranking function " + kind + " not yet implemented in a WINDOW aggregate",
                    node);
        };

        DBSPSimpleOperator index = this.indexWindow(window, group);

        // Generate comparison function for sorting the vector
        RelNode input = window.getInput();
        DBSPType inputRowType = this.convertType(input.getRowType(), false);
        DBSPComparatorExpression comparator = CalciteToDBSPCompiler.generateComparator(
                node, group.orderKeys.getFieldCollations(), inputRowType);

        // The rank must be added at the end of the input tuple (that's how Calcite expects it).
        DBSPVariablePath left = new DBSPVariablePath(new DBSPTypeInteger(
                node, 64, true, false));
        DBSPVariablePath right = new DBSPVariablePath(inputRowType.ref());
        List<DBSPExpression> flattened = DBSPTypeTupleBase.flatten(right.deref());
        flattened.add(left);
        DBSPTupleExpression tuple = new DBSPTupleExpression(flattened, false);
        DBSPClosureExpression outputProducer = tuple.closure(left, right);

        // TopK operator.
        // Since TopK is always incremental we have to wrap it into a D-I pair
        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, index.outputPort());
        this.addOperator(diff);
        DBSPI32Literal limitValue = new DBSPI32Literal(limit);
        DBSPIndexedTopKOperator topK = new DBSPIndexedTopKOperator(
                node, numbering, comparator, limitValue, outputProducer, diff.outputPort());
        this.addOperator(topK);
        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, topK.outputPort());
        this.addOperator(integral);
        // We must drop the index we built.
        DBSPDeindexOperator deindex = new DBSPDeindexOperator(node, integral.outputPort());
        if (this.filterImplementation != null)
            throw new InternalCompilerError("Unexpected filter implementation ", node);
        this.filterImplementation = deindex;
        this.assignOperator(window, deindex);
    }

    /**
     * Helper function for isLimit.  Check to see if an expression is a comparison with a constant.
     * @param compared  Expression that is compared.  Check if it is a RexInputRef with the expectedIndex.
     * @param limit     Expression compared against.  Check if it is a RexLiteral with an integer value.
     * @param eq        True if comparison is for equality.
     * @return          -1 if the operands don't have the expected shape.  The limit value otherwise.
     */
    int limitValue(RexNode compared, int expectedIndex, RexNode limit, boolean eq) {
        if (compared instanceof RexInputRef ri) {
            if (ri.getIndex() != expectedIndex)
                return -1;
        }
        if (limit instanceof RexLiteral literal) {
            SqlTypeName type = literal.getType().getSqlTypeName();
            if (SqlTypeName.INT_TYPES.contains(type)) {
                Integer value = literal.getValueAs(Integer.class);
                Objects.requireNonNull(value);
                if (!eq)
                    value--;
                return value;
            }
        }
        return -1;
    }

    /**
     * Analyze whether an expression has the form row.index <= constant
     * or something equivalent.
     * @param node  Expression to analyze.
     * @param variableIndex  Row index of the field that should be compared
     * @return -1 if the shape is unsuitable.
     *         An integer value representing the limit otherwise.
     */
    int isLimit(RexCall node, int variableIndex) {
        if (node.getOperands().size() != 2)
            return -1;
        RexNode left = node.operands.get(0);
        RexNode right = node.operands.get(1);
        switch (node.getKind()) {
            case EQUALS: {
                // Treat 'a == 1' like 'a <= 1'.
                // This works because the smallest legal limit is 1.
                int leftLimit = limitValue(left, variableIndex, right, true);
                int rightLimit = limitValue(left, variableIndex, right, true);
                if (leftLimit == 1 || rightLimit == 1)
                    return 1;
            }
            case LESS_THAN:
                return limitValue(left, variableIndex, right, false);
            case LESS_THAN_OR_EQUAL:
                return limitValue(left, variableIndex, right, true);
            case GREATER_THAN:
                return limitValue(right, variableIndex, left, false);
            case GREATER_THAN_OR_EQUAL:
                return limitValue(right, variableIndex, left, true);
            default:
                return -1;
        }
    }

    /** Helper class for window processing.
     * Calcite can sometimes use the same group for window computations
     * that we cannot perform in one operator, so we
     * divide some group/window combinations into multiple
     * combinations. */
    static abstract class GroupAndAggregates {
        final CalciteToDBSPCompiler compiler;
        final CalciteObject node;
        final Window window;
        final Window.Group group;
        final List<AggregateCall> aggregateCalls;
        final int windowFieldIndex;
        final DBSPTypeTuple windowResultType;
        final DBSPTypeTuple inputRowType;
        final List<Integer> partitionKeys;
        final DBSPVariablePath inputRowRefVar;
        final ExpressionCompiler eComp;

        /**
         * Create a new window aggregate.
         * @param compiler          Compiler.
         * @param window            Window being compiled.
         * @param group             Group within window being compiled.
         * @param windowFieldIndex  Index of first field of aggregate within window.
         *                          The list aggregateCalls contains aggregates starting at this index.
         */
        GroupAndAggregates(CalciteToDBSPCompiler compiler, Window window, Window.Group group, int windowFieldIndex) {
            this.node = CalciteObject.create(window);
            this.compiler = compiler;
            this.window = window;
            this.group = group;
            this.aggregateCalls = new ArrayList<>();
            this.windowFieldIndex = windowFieldIndex;
            this.windowResultType = this.compiler.convertType(
                    window.getRowType(), false).to(DBSPTypeTuple.class);
            this.inputRowType = this.compiler.convertType(
                    window.getInput().getRowType(), false).to(DBSPTypeTuple.class);
            this.partitionKeys = this.group.keys.toList();
            this.inputRowRefVar = this.inputRowType.ref().var();
            this.eComp = new ExpressionCompiler(window, this.inputRowRefVar, window.constants, this.compiler.compiler);
        }

        abstract DBSPSimpleOperator implement(DBSPSimpleOperator input, DBSPSimpleOperator lastOperator);

        void addAggregate(AggregateCall call) {
            this.aggregateCalls.add(call);
        }

        public DBSPTupleExpression partitionKeys() {
            List<DBSPExpression> expressions = Linq.map(this.partitionKeys,
                    f -> this.inputRowRefVar.deepCopy().deref().field(f).applyCloneIfNeeded());
            return new DBSPTupleExpression(node, expressions);
        }

        abstract boolean isCompatible(AggregateCall call);

        static boolean isUnbounded(Window.Group group) {
            return group.lowerBound.isUnboundedPreceding() && group.upperBound.isUnboundedFollowing();
        }

        static GroupAndAggregates newGroup(CalciteToDBSPCompiler compiler, Window window, Window.Group group,
                                    int windowFieldIndex, AggregateCall call) {
            GroupAndAggregates result = switch (call.getAggregation().getKind()) {
                case LAG, LEAD -> new LeadLagAggregates(compiler, window, group, windowFieldIndex);
                default -> (isUnbounded(group) && group.orderKeys.getFieldCollations().isEmpty()) ?
                            new SimpleAggregates(compiler, window, group, windowFieldIndex) :
                            new RangeAggregates(compiler, window, group, windowFieldIndex);
            };
            result.addAggregate(call);
            return result;
        }

        @Override
        public String toString() {
            return this.aggregateCalls.toString();
        }
    }

    /** A class representing a LEAD or LAG function */
    static class LeadLagAggregates extends GroupAndAggregates {
        protected LeadLagAggregates(CalciteToDBSPCompiler compiler, Window window,
                                    Window.Group group, int windowFieldIndex) {
            super(compiler, window, group, windowFieldIndex);
            if (group.exclude != RexWindowExclusion.EXCLUDE_NO_OTHER)
                throw new UnimplementedException("EXCLUDE BY in OVER", 457, node);
        }

        @Override
        DBSPSimpleOperator implement(DBSPSimpleOperator firstInput, DBSPSimpleOperator lastOperator) {
            // All the aggregate calls have the same arguments by construction
            AggregateCall lastCall = Utilities.last(this.aggregateCalls);
            SqlKind kind = lastCall.getAggregation().getKind();
            int offset = kind == org.apache.calcite.sql.SqlKind.LEAD ? -1 : +1;

            DBSPType firstInputRowType = firstInput.getOutputZSetElementType();
            DBSPVariablePath firstInputVar = firstInputRowType.ref().var();
            ExpressionCompiler eComp = new ExpressionCompiler(
                    this.window, firstInputVar, this.window.constants, this.compiler.compiler);
            OutputPort inputIndexed;

            if (lastOperator == firstInput) {
                List<DBSPExpression> expressions = Linq.map(this.partitionKeys,
                        f -> firstInputVar.deepCopy().deref().field(f).applyCloneIfNeeded());
                DBSPTupleExpression partition = new DBSPTupleExpression(node, expressions);
                // Map each row to an expression of the form: |t| (partition, (*t).clone()))
                DBSPExpression row = DBSPTupleExpression.flatten(
                        firstInputVar.deepCopy().deref().applyClone());
                DBSPExpression mapExpr = new DBSPRawTupleExpression(partition, row);
                DBSPClosureExpression mapClo = mapExpr.closure(firstInputVar);
                DBSPSimpleOperator index = new DBSPMapIndexOperator(node, mapClo,
                        CalciteToDBSPCompiler.makeIndexedZSet(
                                partition.getType(), row.getType()), lastOperator.outputPort());
                this.compiler.circuit.addOperator(index);
                inputIndexed = index.outputPort();
            } else {
                // avoid a deindex->index chain which does nothing
                assert lastOperator.is(DBSPDeindexOperator.class);
                inputIndexed = lastOperator.inputs.get(0);
            }

            // This operator is always incremental, so create the non-incremental version
            // of it by adding a D and an I around it.
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, inputIndexed);
            this.compiler.circuit.addOperator(diff);

            DBSPType inputRowType = lastOperator.getOutputZSetElementType();
            DBSPVariablePath inputVar = inputRowType.ref().var();
            DBSPExpression row = DBSPTupleExpression.flatten(
                    inputVar.deepCopy().deref().applyClone());
            DBSPComparatorExpression comparator = CalciteToDBSPCompiler.generateComparator(
                    node, group.orderKeys.getFieldCollations(), row.getType());

            // Lag argument calls
            List<Integer> operands = lastCall.getArgList();
            if (operands.size() > 1) {
                int amountIndex = operands.get(1);
                RexInputRef ri = new RexInputRef(
                        amountIndex, window.getRowType().getFieldList().get(amountIndex).getType());
                DBSPExpression amount = eComp.compile(ri);
                if (!amount.is(DBSPI32Literal.class)) {
                    throw new UnimplementedException("Currently LAG/LEAD amount must be a compile-time constant", 457, node);
                }
                assert amount.is(DBSPI32Literal.class);
                offset *= Objects.requireNonNull(amount.to(DBSPI32Literal.class).value);
            }

            // Lag has this signature
            //     pub fn lag_custom_order<VL, OV, PF, CF, OF>(
            //        &self,
            //        offset: isize,
            //        project: PF,
            //        output: OF,
            //    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
            //    where
            //        VL: DBData,
            //        OV: DBData,
            //        CF: CmpFunc<V>,
            //        PF: Fn(Option<&V>) -> VL + 'static,
            //        OF: Fn(&V, &VL) -> OV + 'static,
            // Notice that the project function takes an Option<&V>.
            List<Integer> lagColumns = Linq.map(this.aggregateCalls, call -> call.getArgList().get(0));
            DBSPVariablePath var = new DBSPVariablePath(inputRowType.ref().withMayBeNull(true));
            List<DBSPExpression> lagColumnExpressions = new ArrayList<>();
            for (int i = 0; i < lagColumns.size(); i++) {
                int field = lagColumns.get(i);
                DBSPExpression expression = var.unwrap()
                        .deref()
                        .field(field)
                        // cast the results to whatever Calcite says they will be.
                        .applyCloneIfNeeded()
                        .cast(this.windowResultType.getFieldType(this.windowFieldIndex + i));
                lagColumnExpressions.add(expression);
            }
            DBSPTupleExpression lagTuple = new DBSPTupleExpression(
                    lagColumnExpressions, false);
            DBSPExpression[] defaultValues = new DBSPExpression[lagTuple.size()];
            // Default value is NULL, or may be specified explicitly
            int i = 0;
            for (AggregateCall call: this.aggregateCalls) {
                List<Integer> args = call.getArgList();
                assert lagTuple.fields != null;
                DBSPType resultType = lagTuple.fields[i].getType();
                if (args.size() > 2) {
                    int defaultIndex = args.get(2);
                    RexInputRef ri = new RexInputRef(defaultIndex,
                            // Same type as field i
                            window.getRowType().getFieldList().get(i).getType());
                    // a default argument is present
                    defaultValues[i] = eComp.compile(ri).cast(resultType);
                } else {
                    defaultValues[i] = resultType.none();
                }
                i++;
            }
            // All fields of none are NULL
            DBSPExpression none = new DBSPTupleExpression(defaultValues);
            DBSPExpression conditional = new DBSPIfExpression(node,
                    var.is_null(), none, lagTuple);

            DBSPExpression projection = conditional.closure(var);

            DBSPVariablePath origRow = new DBSPVariablePath(inputRowType.ref());
            DBSPVariablePath delayedRow = new DBSPVariablePath(lagTuple.getType().ref());
            DBSPExpression functionBody = DBSPTupleExpression.flatten(origRow.deref(), delayedRow.deref());
            DBSPExpression function = functionBody.closure(origRow, delayedRow);

            DBSPLagOperator lag = new DBSPLagOperator(
                    node, offset, projection, function, comparator,
                    CalciteToDBSPCompiler.makeIndexedZSet(
                            diff.getOutputIndexedZSetType().keyType, functionBody.getType()), diff.outputPort());
            this.compiler.circuit.addOperator(lag);

            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, lag.outputPort());
            this.compiler.circuit.addOperator(integral);

            return new DBSPDeindexOperator(node, integral.outputPort());
        }

        @Override
        boolean isCompatible(AggregateCall call) {
            assert !this.aggregateCalls.isEmpty();
            AggregateCall lastCall = Utilities.last(this.aggregateCalls);
            SqlKind kind = call.getAggregation().getKind();
            if (lastCall.getAggregation().getKind() != kind)
                return false;
            // A call is compatible if the first 2 arguments are the same
            List<Integer> args = call.getArgList();
            List<Integer> lastArgs = lastCall.getArgList();
            if (!Objects.equals(args.get(0), lastArgs.get(0)))
                return false;
            Integer arg1 = null;
            Integer lastArg1 = null;
            if (args.size() > 1)
                arg1 = args.get(1);
            if (lastArgs.size() > 1)
                lastArg1 = lastArgs.get(1);
            return Objects.equals(arg1, lastArg1);
            // Argument #3 may be different
        }
    }

    /** Simple aggregates used in an OVER, no LAG, or RANGE. */
    static class SimpleAggregates extends GroupAndAggregates {
        protected SimpleAggregates(CalciteToDBSPCompiler compiler, Window window, Window.Group group, int windowFieldIndex) {
            super(compiler, window, group, windowFieldIndex);
            assert this.group.orderKeys.getFieldCollations().isEmpty();
            assert isUnbounded(group);
        }

        @Override
        DBSPSimpleOperator implement(DBSPSimpleOperator unused, DBSPSimpleOperator lastOperator) {
            DBSPTypeTuple tuple = this.windowResultType.slice(
                    this.windowFieldIndex, this.windowFieldIndex + this.aggregateCalls.size());
            DBSPType groupKeyType = this.partitionKeys().getType();
            DBSPType inputType = lastOperator.getOutputZSetElementType();

            List<DBSPAggregate> aggregates = CalciteToDBSPCompiler.createAggregates(
                    this.compiler.compiler,
                    this.window, this.aggregateCalls, tuple, inputType,
                    0, this.group.keys, true);

            // Index the previous input using the group keys
            DBSPTypeIndexedZSet localGroupAndInput = makeIndexedZSet(groupKeyType, inputType);
            DBSPVariablePath rowVar = inputType.ref().var();
            DBSPExpression[] expressions = new DBSPExpression[]{rowVar.deref()};
            DBSPTupleExpression flattened = DBSPTupleExpression.flatten(expressions);
            DBSPClosureExpression makeKeys =
                    new DBSPRawTupleExpression(
                            new DBSPTupleExpression(
                                    Linq.map(this.partitionKeys, p -> rowVar.deref().field(p)), false),
                            new DBSPTupleExpression(this.node,
                                    lastOperator.getOutputZSetElementType().to(DBSPTypeTuple.class),
                                    Objects.requireNonNull(flattened.fields)))
                            .closure(rowVar);
            DBSPSimpleOperator indexedInput = new DBSPMapIndexOperator(
                    node, makeKeys, localGroupAndInput, lastOperator.outputPort());
            this.compiler.addOperator(indexedInput);

            DBSPSimpleOperator join = this.compiler.joinAllAggregates(
                    this.node, groupKeyType, indexedInput.outputPort(), aggregates);

            // Join again with the indexed input
            DBSPVariablePath key = groupKeyType.ref().var();
            DBSPVariablePath left = flattened.getType().ref().var();
            DBSPVariablePath right = join.getOutputIndexedZSetType().elementType.ref().var();
            DBSPClosureExpression append =
                    DBSPTupleExpression.flatten(left.deref(), right.deref()).closure(
                            key, left, right);
            // Do not insert the last operator
            return new DBSPStreamJoinOperator(this.node,
                    this.compiler.makeZSet(append.getResultType()),
                    append, false, indexedInput.outputPort(), join.outputPort());
        }

        @Override
        boolean isCompatible(AggregateCall call) {
            SqlKind kind = call.getAggregation().getKind();
            return kind != org.apache.calcite.sql.SqlKind.LAG && kind != org.apache.calcite.sql.SqlKind.LEAD;
        }
    }

    /** Implements a window aggregate with a RANGE */
    static class RangeAggregates extends GroupAndAggregates {
        final int orderColumnIndex;
        final RelFieldCollation collation;

        protected RangeAggregates(CalciteToDBSPCompiler compiler, Window window, Window.Group group, int windowFieldIndex) {
            super(compiler, window, group, windowFieldIndex);

            List<RelFieldCollation> orderKeys = this.group.orderKeys.getFieldCollations();
            if (orderKeys.isEmpty())
                throw new CompilationError("Missing ORDER BY in OVER", node);
            if (orderKeys.size() > 1)
                throw new UnimplementedException("ORDER BY in OVER requires exactly 1 column", 457, node);
            if (group.exclude != RexWindowExclusion.EXCLUDE_NO_OTHER)
                throw new UnimplementedException("EXCLUDE BY in OVER", 457, node);

            this.collation = orderKeys.get(0);
            this.orderColumnIndex = this.collation.getFieldIndex();
        }

        DBSPWindowBoundExpression compileWindowBound(
                RexWindowBound bound, DBSPType sortType, DBSPType unsignedType, ExpressionCompiler eComp) {
            CalciteObject node = CalciteObject.create(this.window);
            IsNumericType numType = unsignedType.as(IsNumericType.class);
            if (numType == null) {
                throw new UnimplementedException("Currently windows must use integer values, so "
                        + unsignedType + " is not legal", 457, node);
            }
            DBSPExpression numericBound;
            if (bound.isUnbounded())
                numericBound = numType.getMaxValue();
            else if (bound.isCurrentRow())
                numericBound = numType.getZero();
            else {
                DBSPExpression value = eComp.compile(Objects.requireNonNull(bound.getOffset()));
                Simplify simplify = new Simplify(this.compiler.compiler);
                IDBSPInnerNode simplified = simplify.apply(value);
                if (!simplified.is(DBSPLiteral.class)) {
                    throw new UnsupportedException("Currently window bounds must be constant values: " +
                            simplified, node);
                }
                DBSPLiteral literal = simplified.to(DBSPLiteral.class);
                if (!literal.to(IsNumericLiteral.class).gt0()) {
                    throw new UnsupportedException("Window bounds must be positive: " + literal.toSqlString(), node);
                }
                if (literal.getType().is(DBSPTypeInteger.class)) {
                    numericBound = value.cast(unsignedType);
                } else {
                    RustSqlRuntimeLibrary.FunctionDescription desc =
                            RustSqlRuntimeLibrary.getWindowBound(node, unsignedType, sortType, literal.getType());
                    numericBound = new DBSPApplyExpression(desc.function, unsignedType, literal.borrow());
                }
            }
            return new DBSPWindowBoundExpression(node, bound.isPreceding(), numericBound);
        }

        @Override
        DBSPSimpleOperator implement(DBSPSimpleOperator input, DBSPSimpleOperator lastOperator) {
            // The final result is accumulated using join operators, which just keep adding columns to
            // the "lastOperator".  The "lastOperator" is initially the input node itself.
            List<RelFieldCollation> orderKeys = this.group.orderKeys.getFieldCollations();
            if (orderKeys.isEmpty())
                throw new CompilationError("Missing ORDER BY in OVER", node);
            if (orderKeys.size() > 1)
                throw new UnimplementedException("ORDER BY in OVER requires exactly 1 column", 457, node);

            DBSPType sortType, originalSortType;
            DBSPType unsignedSortType;
            DBSPSimpleOperator mapIndex;
            boolean ascending = collation.getDirection() == RelFieldCollation.Direction.ASCENDING;
            boolean nullsLast = collation.nullDirection != RelFieldCollation.NullDirection.FIRST;
            DBSPType partitionType;
            DBSPType partitionAndRowType;
            DBSPTypeTuple lastTupleType = lastOperator.getOutputZSetElementType().to(DBSPTypeTuple.class);

            {
                DBSPTupleExpression partitionKeys = this.partitionKeys();
                partitionType = partitionKeys.getType();

                DBSPExpression originalOrderField = this.inputRowRefVar.deref().field(orderColumnIndex);
                sortType = originalOrderField.getType();
                originalSortType = sortType;
                // Original scale if the sort field is a DECIMAL
                if (sortType.is(DBSPTypeDecimal.class)) {
                    // Scale decimal to make it an integer by multiplying with 10^scale
                    DBSPTypeDecimal dec = sortType.to(DBSPTypeDecimal.class);

                    DBSPTypeInteger intType;
                    DBSPTypeCode code = DBSPTypeInteger.smallestInteger(dec.precision);
                    if (code != null) {
                        intType = DBSPTypeInteger.getType(node, code, dec.mayBeNull);
                        DBSPTypeDecimal mulType = new DBSPTypeDecimal(
                                node, dec.precision, 0, dec.mayBeNull);
                        // directly build the expression, no casts are needed
                        originalOrderField = new DBSPBinaryExpression(
                                node, mulType, DBSPOpcode.SHIFT_LEFT, originalOrderField,
                                new DBSPI32Literal(dec.scale));
                        originalOrderField = originalOrderField.cast(intType);
                        sortType = intType;
                    }
                }

                if (!sortType.is(DBSPTypeInteger.class) &&
                        !sortType.is(DBSPTypeTimestamp.class) &&
                        !sortType.is(DBSPTypeDate.class) &&
                        !sortType.is(DBSPTypeTime.class))
                    throw new UnimplementedException("OVER currently cannot sort on columns with type "
                            + Utilities.singleQuote(sortType.asSqlString()), 457, node);

                // This only works if the order field is unsigned.
                DBSPExpression orderField = new DBSPUnsignedWrapExpression(
                        node, originalOrderField, ascending, nullsLast);
                unsignedSortType = orderField.getType();

                // Map each row to an expression of the form: |t| (order, Tup2(partition, (*t).clone()))
                DBSPExpression partitionAndRow = new DBSPTupleExpression(
                        partitionKeys, inputRowRefVar.deepCopy().deref().applyClone());
                partitionAndRowType = partitionAndRow.getType();
                DBSPExpression indexExpr = new DBSPRawTupleExpression(orderField, partitionAndRow);
                DBSPClosureExpression indexClosure = indexExpr.closure(inputRowRefVar);
                mapIndex = new DBSPMapIndexOperator(node, indexClosure,
                        makeIndexedZSet(orderField.getType(), partitionAndRow.getType()), input.outputPort());
                this.compiler.circuit.addOperator(mapIndex);
            }

            DBSPSimpleOperator windowAgg;
            DBSPTypeTuple aggResultType;
            DBSPTypeIndexedZSet finalResultType;
            {
                // Compute the window aggregate

                // This operator is always incremental, so create the non-incremental version
                // of it by adding a D and an I around it.
                DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, mapIndex.outputPort());
                this.compiler.circuit.addOperator(diff);

                // Create window description
                DBSPWindowBoundExpression lb = this.compileWindowBound(group.lowerBound, sortType, unsignedSortType, eComp);
                DBSPWindowBoundExpression ub = this.compileWindowBound(group.upperBound, sortType, unsignedSortType, eComp);

                List<DBSPType> types = Linq.map(aggregateCalls, c -> this.compiler.convertType(c.type, false));
                DBSPTypeTuple tuple = new DBSPTypeTuple(types);
                List<DBSPAggregate> folds = CalciteToDBSPCompiler.createAggregates(this.compiler.compiler,
                        window, aggregateCalls, tuple, inputRowType, 0, ImmutableBitSet.of(), false);
                assert folds.size() == 1;
                DBSPAggregate fd = folds.get(0);

                // This function is always the same: |Tup2(x, y)| (x, y)
                DBSPVariablePath pr = new DBSPVariablePath(partitionAndRowType.ref());
                DBSPClosureExpression partitioningFunction =
                        new DBSPRawTupleExpression(
                                pr.deref().field(0).applyCloneIfNeeded(),
                                pr.deref().field(1).applyCloneIfNeeded())
                                .closure(pr);

                aggResultType = fd.getEmptySetResultType().to(DBSPTypeTuple.class);
                finalResultType = makeIndexedZSet(
                        new DBSPTypeTuple(partitionType, originalSortType), aggResultType);
                // Prepare a type that will make the operator following the window aggregate happy
                // (that operator is a map_index).  Currently, the compiler cannot represent
                // exactly the output type of the WindowAggregateOperator, so it lies about the actual type.
                DBSPTypeIndexedZSet windowOutputType =
                        makeIndexedZSet(partitionType,
                                new DBSPTypeTuple(
                                        unsignedSortType,
                                        aggResultType.withMayBeNull(true)));

                // Compute aggregates for the window
                windowAgg = new DBSPPartitionedRollingAggregateOperator(
                        node, partitioningFunction, null, fd,
                        lb, ub, windowOutputType, diff.outputPort());
                this.compiler.circuit.addOperator(windowAgg);
            }

            DBSPMapIndexOperator index;
            {
                // Index the produced result
                // map_index(|(key_ts_agg)| (
                //         Tup2::new(key_to_agg.0.clone(),
                //                   UnsignedWrapper::to_signed::<i32, i32, i64, u64>(key_ts_agg.1.0, true, true)),
                //         key_ts_agg.1.1.unwrap_or_default() ))
                DBSPVariablePath var = new DBSPVariablePath("key_ts_agg",
                        new DBSPTypeRawTuple(
                                partitionType.ref(),
                                new DBSPTypeTuple(
                                        unsignedSortType,  // not the sortType, but the wrapper type around it
                                        aggResultType.withMayBeNull(true)).ref()));
                // new DBSPTypeOption(aggResultType)).ref()));
                DBSPExpression ixKey = var.deepCopy().field(0).deref().applyCloneIfNeeded();
                DBSPExpression ts = var.deepCopy().field(1).deref().field(0);
                DBSPExpression agg = var.deepCopy().field(1).deref().field(1).applyCloneIfNeeded();
                DBSPExpression unwrap = new DBSPUnsignedUnwrapExpression(
                        node, ts, sortType, ascending, nullsLast);
                if (originalSortType.is(DBSPTypeDecimal.class)) {
                    DBSPTypeDecimal dec = originalSortType.to(DBSPTypeDecimal.class);
                    // convert back to decimal and rescale
                    var intermediateType = new DBSPTypeDecimal(node, dec.precision + dec.scale, 0, originalSortType.mayBeNull);
                    unwrap = unwrap.cast(intermediateType);
                    unwrap = new DBSPBinaryExpression(node, originalSortType,
                            DBSPOpcode.SHIFT_LEFT, unwrap, new DBSPI32Literal(-dec.scale));
                }
                DBSPExpression body = new DBSPRawTupleExpression(
                        new DBSPTupleExpression(ixKey, unwrap),
                        new DBSPApplyMethodExpression(node, "unwrap_or_default", aggResultType, agg));
                index = new DBSPMapIndexOperator(node, body.closure(var), finalResultType, windowAgg.outputPort());
                this.compiler.circuit.addOperator(index);
            }

            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, index.outputPort());
            this.compiler.circuit.addOperator(integral);

            // Join the previous result with the aggregate
            DBSPSimpleOperator indexInput;
            DBSPType lastPartAndOrderType;
            DBSPType lastCopiedFieldsType;
            {
                // Index the lastOperator
                DBSPVariablePath previousRowRefVar = lastTupleType.ref().var();
                List<DBSPExpression> expressions = Linq.map(partitionKeys,
                        f -> previousRowRefVar.deepCopy().deref().field(f).applyCloneIfNeeded());
                DBSPTupleExpression partition = new DBSPTupleExpression(node, expressions);
                DBSPExpression originalOrderField = previousRowRefVar.deref().field(orderColumnIndex);
                DBSPExpression partAndOrder = new DBSPTupleExpression(
                        partition.applyCloneIfNeeded(),
                        originalOrderField.applyCloneIfNeeded());
                lastPartAndOrderType = partAndOrder.getType();
                // Copy all the fields from the previousRowRefVar except the partition fields.
                List<DBSPExpression> fields = new ArrayList<>();
                for (int i = 0; i < lastTupleType.size(); i++) {
                    if (partitionKeys.contains(i))
                        continue;
                    if (orderColumnIndex == i)
                        continue;
                    fields.add(previousRowRefVar.deepCopy().deref().field(i).applyCloneIfNeeded());
                }
                DBSPExpression copiedFields = new DBSPTupleExpression(fields, false);
                lastCopiedFieldsType = copiedFields.getType();
                DBSPExpression indexedInput = new DBSPRawTupleExpression(partAndOrder, copiedFields);
                DBSPClosureExpression partAndOrderClo = indexedInput.closure(previousRowRefVar);

                indexInput = new DBSPMapIndexOperator(node, partAndOrderClo,
                        makeIndexedZSet(partAndOrder.getType(), copiedFields.getType()),
                        lastOperator.isMultiset, lastOperator.outputPort());
                this.compiler.circuit.addOperator(indexInput);
            }

            {
                // Join the results
                DBSPVariablePath key = lastPartAndOrderType.ref().var();
                DBSPVariablePath left = lastCopiedFieldsType.ref().var();
                DBSPVariablePath right = aggResultType.ref().var();
                DBSPExpression[] allFields = new DBSPExpression[
                        lastTupleType.size() + aggResultType.size()];
                int indexField = 0;
                for (int i = 0; i < lastTupleType.size(); i++) {
                    if (partitionKeys.contains(i)) {
                        int keyIndex = partitionKeys.indexOf(i);
                        // If the field is in the index, use it from the index
                        allFields[i] = key.deepCopy()
                                .deref()
                                .field(0) // partition part
                                .field(keyIndex)
                                .applyCloneIfNeeded();
                        indexField++;
                    } else if (orderColumnIndex == i) {
                        // If the field is the order key, use it from the index too
                        allFields[i] = key.deepCopy()
                                .deref()
                                .field(1)
                                .applyCloneIfNeeded();
                        indexField++;
                    } else {
                        allFields[i] = left.deepCopy().deref().field(i - indexField).applyCloneIfNeeded();
                    }
                }
                for (int i = 0; i < aggResultType.size(); i++) {
                    // Calcite is very smart and sometimes infers non-nullable result types
                    // for these aggregates.  So we have to cast the results to whatever
                    // Calcite says they will be.
                    allFields[i + lastTupleType.size()] = right.deepCopy().deref().field(i).applyCloneIfNeeded().cast(
                            this.windowResultType.getFieldType(this.windowFieldIndex + i));
                }
                DBSPTupleExpression addExtraFieldBody = new DBSPTupleExpression(allFields);
                DBSPClosureExpression addExtraField =
                        addExtraFieldBody.closure(key, left, right);
                return new DBSPStreamJoinOperator(node, this.compiler.makeZSet(addExtraFieldBody.getType()),
                        addExtraField, indexInput.isMultiset || windowAgg.isMultiset,
                        indexInput.outputPort(), integral.outputPort());
            }
        }

        @Override
        boolean isCompatible(AggregateCall call) {
            SqlKind kind = call.getAggregation().getKind();
            return kind != org.apache.calcite.sql.SqlKind.LAG && kind != org.apache.calcite.sql.SqlKind.LEAD;
        }
    }

    /** Decompose a window into a list of GroupAndAggregates that can be each implemented
     * by a separate DBSP operator. */
    List<GroupAndAggregates> splitWindow(LogicalWindow window, int windowFieldIndex) {
        List<GroupAndAggregates> result = new ArrayList<>();
        for (Window.Group group: window.groups) {
            if (group.isRows)
                throw new UnimplementedException("WINDOW aggregate with ROWS not yet implemented", 457,
                        CalciteObject.create(window));
            List<AggregateCall> calls = group.getAggregateCalls(window);
            GroupAndAggregates previous = null;
            // Must keep call in the same order as in the original list,
            // because the next operator also expects them in the same order.
            for (AggregateCall call: calls) {
                if (previous == null) {
                    previous = GroupAndAggregates.newGroup(this, window, group, windowFieldIndex, call);
                } else if (previous.isCompatible(call)) {
                    previous.addAggregate(call);
                } else {
                    result.add(previous);
                    previous = GroupAndAggregates.newGroup(this, window, group, windowFieldIndex, call);
                }
                windowFieldIndex++;
            }
            result.add(previous);
        }
        return result;
    }

    void visitWindow(LogicalWindow window) {
        DBSPSimpleOperator input = this.getInputAs(window.getInput(0), true);
        DBSPTypeTuple inputRowType = this.convertType(
                window.getInput().getRowType(), false).to(DBSPTypeTuple.class);
        int windowFieldIndex = inputRowType.size();
        DBSPType resultType = this.convertType(window.getRowType(), false);

        // Special handling for the following pattern:
        // LogicalFilter(condition=[<=(RANK_COLUMN, LIMIT)])
        // LogicalWindow(window#0=[window(partition ... order by ... aggs [RANK()])])
        // This is compiled into a TopK over each group.
        // There is no way this can be expressed otherwise in SQL or using RelNode.
        // This works for RANK, DENSE_RANK, and ROW_NUMBER.
        if (window.groups.size() == 1 && !this.ancestors.isEmpty()) {
            Window.Group group = window.groups.get(0);
            if (group.aggCalls.size() == 1) {
                Window.RexWinAggCall agg = group.aggCalls.get(0);
                RelNode parent = this.getParent();
                SqlOperator operator = agg.getOperator();
                // Aggregate functions seem always to be at the end.
                // The window always seems to collect all fields.
                int aggregationArgumentIndex = inputRowType.size();
                if (operator instanceof SqlRankFunction rank) {
                    if (parent instanceof LogicalFilter filter) {
                        RexNode condition = filter.getCondition();
                        if (condition instanceof RexCall) {
                            int limit = this.isLimit((RexCall) condition, aggregationArgumentIndex);
                            if (limit >= 0) {
                                this.generateNestedTopK(window, group, limit, rank.kind);
                                return;
                            }
                        }
                    }
                }
            }
        }

        // We have to process multiple Groups, and each group has multiple aggregates.
        DBSPSimpleOperator lastOperator = input;
        List<GroupAndAggregates> toProcess = this.splitWindow(window, windowFieldIndex);
        for (GroupAndAggregates ga: toProcess) {
            if (lastOperator != input)
                this.addOperator(lastOperator);
            lastOperator = ga.implement(input, lastOperator);
        }

        assert lastOperator.getOutputZSetElementType().sameType(resultType);
        this.assignOperator(window, lastOperator);
    }

    private RelNode getParent() {
        return Utilities.last(this.ancestors);
    }

    void warnNoSort(CalciteObject node) {
        boolean isFinal = this.ancestors.isEmpty();
        String viewName = "";
        if (this.currentView != null)
            viewName = "producing view " + this.currentView.relationName.singleQuote() + " ";
        this.compiler.reportWarning(node.getPositionRange(), "ORDER BY is ignored",
                "ORDER BY clause " + viewName + "is currently ignored" +
                        (isFinal ? "" :
                                "\nThis is tracked by issue https://github.com/feldera/feldera/issues/2833"));
    }

    void visitSort(LogicalSort sort) {
        CalciteObject node = CalciteObject.create(sort);
        RelNode input = sort.getInput();
        DBSPSimpleOperator opInput = this.getOperator(input);
        if (this.options.languageOptions.ignoreOrderBy && sort.fetch == null) {
            this.warnNoSort(node);
            Utilities.putNew(this.nodeOperator, sort, opInput);
            return;
        }
        if (sort.offset != null)
            throw new UnimplementedException("OFFSET in SORT not yet implemented", 172, node);

        DBSPExpression limit = null;
        if (sort.fetch != null) {
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(sort, null, this.compiler);
            // We expect the limit to be a constant
            limit = expressionCompiler.compile(sort.fetch);
        }

        DBSPType inputRowType = this.convertType(input.getRowType(), false);
        DBSPVariablePath t = inputRowType.ref().var();
        DBSPClosureExpression emptyGroupKeys =
                new DBSPRawTupleExpression(
                        new DBSPRawTupleExpression(),
                        DBSPTupleExpression.flatten(t.deref())).closure(t);
        DBSPSimpleOperator index = new DBSPMapIndexOperator(
                node, emptyGroupKeys,
                makeIndexedZSet(new DBSPTypeRawTuple(), inputRowType),
                opInput.outputPort());
        this.addOperator(index);

        // Generate comparison function for sorting the vector
        DBSPComparatorExpression comparator = generateComparator(
                node, sort.getCollation().getFieldCollations(), inputRowType);
        if (sort.fetch != null) {
            // TopK operator.
            // Since TopK is always incremental we have to wrap it into a D-I pair
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, index.outputPort());
            this.addOperator(diff);
            DBSPIndexedTopKOperator topK = new DBSPIndexedTopKOperator(
                    node, DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER,
                    comparator, limit, null, diff.outputPort());
            this.addOperator(topK);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, topK.outputPort());
            this.addOperator(integral);
            // If we ignore ORDER BY this is the result.
            boolean done = this.options.languageOptions.ignoreOrderBy;
            // We can also ignore the order by for some ancestors
            if (!this.ancestors.isEmpty()) {
                RelNode last = Utilities.last(this.ancestors);
                if (last instanceof LogicalAggregate ||
                        last instanceof LogicalProject ||
                        last instanceof LogicalJoin) {
                    done = true;
                }
            }
            if (sort.getCollation().getFieldCollations().isEmpty())
                // We don't really need to sort; this is just a limit operator
                done = true;
            else
                this.warnNoSort(node);
            if (done) {
                // We must drop the index we built.
                DBSPDeindexOperator deindex = new DBSPDeindexOperator(node, integral.outputPort());
                this.assignOperator(sort, deindex);
                return;
            }
            // Otherwise we have to sort again in a vector!
            // Fall through, continuing from the integral.
            index = integral;
        }
        // Global sort.  Implemented by aggregate in a single Vec<> which is then sorted.
        // Apply an aggregation function that just creates a vector.
        DBSPTypeVec vecType = new DBSPTypeVec(inputRowType, false);
        DBSPExpression zero = new DBSPPath(vecType.name, "new").toExpression().call();
        DBSPVariablePath accum = vecType.ref(true).var();
        DBSPVariablePath row = inputRowType.ref().var();
        // An element with weight 'w' is pushed 'w' times into the vector
        DBSPExpression wPush = new DBSPApplyExpression(node,
                "weighted_push", new DBSPTypeVoid(), accum, row, this.compiler.weightVar);
        DBSPExpression push = wPush.closure(accum, row, this.compiler.weightVar);
        DBSPExpression constructor =
                new DBSPPath(new DBSPSimplePathSegment("Fold", DBSPTypeAny.getDefault(),
                        DBSPTypeAny.getDefault(),
                        new DBSPTypeUser(node, USER, "UnimplementedSemigroup",                                false, DBSPTypeAny.getDefault()),
                        DBSPTypeAny.getDefault(),
                        DBSPTypeAny.getDefault()),
                        new DBSPSimplePathSegment("new")).toExpression();

        DBSPExpression folder = constructor.call(zero, push);
        DBSPStreamAggregateOperator agg = new DBSPStreamAggregateOperator(node,
                makeIndexedZSet(new DBSPTypeRawTuple(), new DBSPTypeVec(inputRowType, false)),
                folder, null, index.outputPort());
        this.addOperator(agg);

        if (limit != null)
            limit = limit.cast(new DBSPTypeUSize(node, false));
        DBSPSortExpression sorter = new DBSPSortExpression(node, inputRowType, comparator, limit);
        DBSPSimpleOperator result = new DBSPMapOperator(
                node, sorter, this.makeZSet(vecType), agg.outputPort());
        this.assignOperator(sort, result);
    }

    @Override
    public void visit(
            RelNode node, int ordinal,
            @Nullable RelNode parent) {
        Logger.INSTANCE.belowLevel(this, 3)
                .append("Visiting ")
                .append(node.toString())
                .newline();
        if (this.nodeOperator.containsKey(node))
            // We have already done this one.  This can happen because the
            // plan can be a DAG, not just a tree.
            return;

        // logical correlates are not done in postorder.
        if (this.visitIfMatches(node, LogicalCorrelate.class, this::visitCorrelate))
            return;

        this.ancestors.add(node);
        // First process children
        super.visit(node, ordinal, parent);
        RelNode last = Utilities.removeLast(this.ancestors);
        assert last == node: "Corrupted stack: got " + last + " expected " + node;

        // Synthesize current node
        boolean success =
                this.visitIfMatches(node, LogicalTableScan.class, l -> this.visitScan(l, false)) ||
                this.visitIfMatches(node, JdbcTableScan.class, l -> this.visitScan(l, true)) ||
                this.visitIfMatches(node, LogicalProject.class, this::visitProject) ||
                this.visitIfMatches(node, LogicalUnion.class, this::visitUnion) ||
                this.visitIfMatches(node, LogicalMinus.class, this::visitMinus) ||
                this.visitIfMatches(node, LogicalFilter.class, this::visitFilter) ||
                this.visitIfMatches(node, LogicalValues.class, this::visitLogicalValues) ||
                this.visitIfMatches(node, LogicalAggregate.class, this::visitAggregate) ||
                this.visitIfMatches(node, LogicalJoin.class, this::visitJoin) ||
                this.visitIfMatches(node, LogicalIntersect.class, this::visitIntersect) ||
                this.visitIfMatches(node, LogicalWindow.class, this::visitWindow) ||
                this.visitIfMatches(node, LogicalSort.class, this::visitSort) ||
                this.visitIfMatches(node, Uncollect.class, this::visitUncollect) ||
                this.visitIfMatches(node, LogicalTableFunctionScan.class, this::visitTableFunction) ||
                this.visitIfMatches(node, LogicalAsofJoin.class, this::visitAsofJoin) ||
                this.visitIfMatches(node, Collect.class, this::visitCollect);
        if (!success)
            throw new UnimplementedException("Calcite operator not yet implemented", CalciteObject.create(node));
    }

    InputColumnMetadata convertMetadata(RelColumnMetadata metadata) {
        DBSPType type = this.convertType(metadata.getType(), false);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(null, null, this.compiler);
        DBSPExpression lateness = null;
        if (metadata.lateness != null) {
            lateness = expressionCompiler.compile(metadata.lateness);
            if (!lateness.getType().is(IHasZero.class)) {
                this.compiler.reportError(lateness.getSourcePosition(), "Illegal expression",
                        "Illegal expression for lateness value");
                lateness = null;
            }
        }
        DBSPExpression watermark = null;
        if (metadata.watermark != null) {
            watermark = expressionCompiler.compile(metadata.watermark);
            if (!watermark.getType().is(IHasZero.class)) {
                this.compiler.reportError(watermark.getSourcePosition(), "Illegal expression",
                        "Illegal expression for watermark value");
                watermark = null;
            }
        }
        DBSPExpression defaultValue = null;
        if (metadata.defaultValue != null) {
            defaultValue = expressionCompiler.compile(metadata.defaultValue);
            defaultValue = defaultValue.cast(type);
        }
        return new InputColumnMetadata(metadata.getNode(), metadata.getName(), type,
                metadata.isPrimaryKey, lateness, watermark, defaultValue, metadata.defaultValuePosition);
    }

    DBSPNode compileCreateView(CreateViewStatement view) {
        CreateViewStatement previousView = this.currentView;
        this.currentView = view;
        RelNode rel = view.getRelNode();
        Logger.INSTANCE.belowLevel(this, 2)
                .append(CalciteCompiler.getPlan(rel, false))
                .newline();
        this.go(rel);
        DBSPSimpleOperator op = this.getOperator(rel);
        if (view.emitFinalColumn(this.compiler) >= 0 && this.options.languageOptions.optimizationLevel < 2) {
            PropertyList properties = Objects.requireNonNull(view.getProperties());
            SqlFragment value = Objects.requireNonNull(properties.getPropertyKey("emit_only"));
            throw new UnsupportedException("The 'emit_only' annotation can only be used with " +
                    " optimization level 2 or higher in the compiler",
                    CalciteObject.create(value.getParserPosition()));
        }

        // The operator above may not contain all columns that were computed.
        RelRoot root = view.getRoot();
        DBSPTypeZSet producedType = op.getOutputZSetType();
        // The output element may be a Tuple or a Vec<Tuple> when we sort;
        DBSPType elemType = producedType.getElementType();
        DBSPTypeTupleBase tuple;
        boolean isVector = false;
        if (elemType.is(DBSPTypeVec.class)) {
            isVector = true;
            tuple = elemType.to(DBSPTypeVec.class).getElementType().to(DBSPTypeTupleBase.class);
        } else {
            tuple = elemType.to(DBSPTypeTupleBase.class);
        }
        if (root.fields.size() != tuple.size()) {
            DBSPVariablePath t = tuple.ref().var();
            List<DBSPExpression> resultFields = new ArrayList<>();
            for (Map.Entry<Integer, String> field: root.fields) {
                resultFields.add(t.deref().field(field.getKey()).applyCloneIfNeeded());
            }
            DBSPExpression all = new DBSPTupleExpression(resultFields, false);
            DBSPClosureExpression closure = all.closure(t);
            DBSPType outputElementType = all.getType();
            if (isVector) {
                outputElementType = new DBSPTypeVec(outputElementType, false);
                DBSPVariablePath v = elemType.var();
                closure = new DBSPApplyExpression("map", outputElementType, v, closure)
                        .closure(v);
            }
            op = new DBSPMapOperator(view.getCalciteObject(), closure, this.makeZSet(outputElementType), op.outputPort());
            this.addOperator(op);
        }

        DBSPSimpleOperator o;
        DBSPTypeStruct struct = view.getRowTypeAsStruct(this.compiler().typeCompiler)
                .rename(view.relationName);
        List<ViewColumnMetadata> additionalMetadata = new ArrayList<>();
        // Synthesize the metadata for the view's columns.
        Map<ProgramIdentifier, ViewColumnMetadata> map = this.viewMetadata.get(view.relationName);
        for (DBSPTypeStruct.Field field: struct.fields.values()) {
            ViewColumnMetadata cm = null;
            if (map != null)
                cm = map.get(field.name);
            if (cm == null) {
                cm = new ViewColumnMetadata(view.getCalciteObject(), view.relationName,
                        field.name, field.getType(), null);
            } else {
                cm = cm.withType(field.getType());
            }
            additionalMetadata.add(cm);
        }
        // Validate in the other direction: every declared metadata field must be used
        if (map != null) {
            for (ViewColumnMetadata cmeta: map.values()) {
                if (!struct.hasField(cmeta.columnName))
                    this.compiler.reportError(cmeta.getPositionRange(),
                            "No such column",
                            "View " + view.relationName.singleQuote() +
                                    " does not contain a column named " +
                                    cmeta.columnName.singleQuote());
            }
        }

        int emitFinalIndex = view.emitFinalColumn(this.compiler);
        DeclareViewStatement declare = this.recursiveViews.get(view.relationName);
        ViewMetadata meta = new ViewMetadata(view.relationName,
                additionalMetadata, view.getViewKind(), emitFinalIndex,
                // The view is a system view if it's not visible
                declare != null, !currentView.isVisible());
        if (view.getViewKind() != SqlCreateView.ViewKind.LOCAL) {
            this.metadata.addView(view);
            // Create two operators chained, a ViewOperator and a SinkOperator.
            DBSPViewOperator vo = new DBSPViewOperator(
                    view.getCalciteObject(), view.relationName, view.getStatement(),
                    struct, meta, op.outputPort());
            this.addOperator(vo);
            o = new DBSPSinkOperator(
                    view.getCalciteObject(), view.relationName,
                    view.getStatement(), struct, meta, vo.outputPort());
        } else {
            // We may already have a node for this output
            DBSPSimpleOperator previous = this.circuit.getView(view.relationName);
            if (previous != null)
                return previous;
            o = new DBSPViewOperator(view.getCalciteObject(), view.relationName,
                    view.getStatement(), struct, meta, op.outputPort());
        }
        this.addOperator(o);
        this.currentView = previousView;
        return o;
    }

    DBSPNode compileModifyTable(TableModifyStatement modify) {
        // The type of the data must be extracted from the modified table
        boolean isInsert = modify.insert;
        SqlNodeList targetColumnList;
        if (isInsert) {
            SqlInsert insert = (SqlInsert) modify.parsedStatement.statement();
            targetColumnList = insert.getTargetColumnList();
        } else {
            SqlRemove remove = (SqlRemove) modify.parsedStatement.statement();
            targetColumnList = remove.getTargetColumnList();
        }
        CreateTableStatement def = this.tableContents.getTableDefinition(modify.tableName);
        this.modifyTableTranslation = new ModifyTableTranslation(
                modify, def, targetColumnList, this.compiler);
        DBSPZSetLiteral result;
        if (modify.rel instanceof LogicalTableScan scan) {
            // Support for INSERT INTO table (SELECT * FROM otherTable)
            ProgramIdentifier sourceTable = Utilities.toIdentifier(scan.getTable().getQualifiedName());
            result = this.tableContents.getTableContents(sourceTable);
        } else if (modify.rel instanceof LogicalValues) {
            this.go(modify.rel);
            result = this.modifyTableTranslation.getTranslation();
        } else if (modify.rel instanceof LogicalProject) {
            // The Calcite optimizer is not able to convert a projection
            // of a VALUES statement that contains ARRAY constructors,
            // because there is no array literal in Calcite.  These expressions
            // are left as projections in the code.  We only handle
            // the case where all project expressions are "constants".
            result = this.compileConstantProject((LogicalProject) modify.rel);
        } else {
            throw new UnimplementedException("CREATE TABLE statement of this form not supported",
                    modify.getCalciteObject());
        }
        if (!isInsert)
            result = result.negate();
        this.modifyTableTranslation = null;
        this.tableContents.addToTable(modify.tableName, result);
        return result;
    }

    @Nullable
    DBSPNode compileCreateType(CreateTypeStatement stat) {
        CalciteObject object = CalciteObject.create(stat.createType.name);
        SqlCreateType ct = stat.createType;
        int index = 0;
        if (!stat.relDataType.isStruct()) {
            return null;
        }
        List<RelDataTypeField> relFields = stat.relDataType.getFieldList();
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        for (SqlNode def : Objects.requireNonNull(ct.attributeDefs)) {
            // the relFields implementation has already flattened structs,
            // so for structs we look them up again using the Sql representation
            DBSPType fieldType = null;
            final SqlAttributeDefinition attributeDef =
                    (SqlAttributeDefinition) def;
            final SqlDataTypeSpec typeSpec = attributeDef.dataType;
            if (typeSpec.getTypeNameSpec() instanceof SqlUserDefinedTypeNameSpec) {
                // Assume it is a reference to another struct
                SqlIdentifier identifier = typeSpec.getTypeNameSpec().getTypeName();
                ProgramIdentifier referred = Utilities.toIdentifier(identifier);
                fieldType = this.compiler.getStructByName(referred);
                if (typeSpec.getNullable() != null && typeSpec.getNullable() && fieldType != null)
                    fieldType = fieldType.withMayBeNull(true);
            }
            if (fieldType == null) {
                // Not a user-defined type
                RelDataTypeField ft = relFields.get(index);
                fieldType = this.convertType(ft.getType(), true);
            }
            DBSPTypeStruct.Field field = new DBSPTypeStruct.Field(
                    object,
                    Utilities.toIdentifier(attributeDef.name),
                    index++,
                    fieldType);
            fields.add(field);
        }

        String saneName = this.compiler.getSaneStructName(stat.typeName);
        DBSPTypeStruct struct = new DBSPTypeStruct(object, stat.typeName, saneName, fields, false);
        this.compiler.registerStruct(struct);
        DBSPItem item = new DBSPStructItem(struct);
        this.circuit.addDeclaration(new DBSPDeclaration(item));
        return null;
    }

    @Nullable
    DBSPNode compileCreateTable(CreateTableStatement create) {
        ProgramIdentifier tableName = create.relationName;
        CreateTableStatement def = this.tableContents.getTableDefinition(tableName);
        DBSPType rowType = def.getRowTypeAsTuple(this.compiler.getTypeCompiler());
        DBSPTypeStruct originalRowType = def.getRowTypeAsStruct(this.compiler.getTypeCompiler());
        CalciteObject identifier = CalciteObject.EMPTY;
        if (create.parsedStatement.statement() instanceof SqlCreateTable sct) {
            identifier = CalciteObject.create(sct.name);
        }
        List<InputColumnMetadata> metadata = Linq.map(create.columns, this::convertMetadata);
        boolean materialized = create.isMaterialized();
        boolean appendOnly = create.isAppendOnly() ||
                options.languageOptions.streaming;

        TableMetadata tableMeta = new TableMetadata(
                tableName, metadata, create.foreignKeys, materialized, appendOnly);
        DBSPSourceMultisetOperator result = new DBSPSourceMultisetOperator(
                create.getCalciteObject(), identifier, this.makeZSet(rowType), originalRowType,
                tableMeta, tableName, def.getStatement());
        this.addOperator(result);
        this.metadata.addTable(create);
        return result;
    }

    @Nullable
    DBSPNode compileDeclareView(DeclareViewStatement create) {
        ProgramIdentifier tableName = create.getName();  // not the same as the declared view's name!
        Utilities.putNew(this.recursiveViews, create.relationName, create);
        DBSPType rowType = create.getRowTypeAsTuple(this.compiler.getTypeCompiler());
        DBSPTypeStruct originalRowType = create.getRowTypeAsStruct(this.compiler.getTypeCompiler());
        CalciteObject identifier = CalciteObject.EMPTY;
        List<InputColumnMetadata> metadata = Linq.map(create.columns, this::convertMetadata);
        TableMetadata tableMeta = new TableMetadata(
                tableName, metadata, new ArrayList<>(), false, false);
        DBSPViewDeclarationOperator result = new DBSPViewDeclarationOperator(
                create.getCalciteObject(), identifier, this.makeZSet(rowType), originalRowType,
                tableMeta, tableName);
        this.circuit.addOperator(result);
        this.metadata.addTable(create);
        return result;
    }

    @Nullable
    DBSPNode compileCreateFunction(CreateFunctionStatement stat) {
        DBSPFunction function = stat.function.getImplementation(
                this.compiler.getTypeCompiler(), this.compiler);
        if (function != null) {
            DBSPType returnType = stat.function.getFunctionReturnType(this.compiler.getTypeCompiler());
            if (returnType.is(DBSPTypeStruct.class)) {
                this.circuit.addDeclaration(new DBSPDeclaration(
                        new DBSPStructWithHelperItem(returnType.to(DBSPTypeStruct.class))));
            }
            this.circuit.addDeclaration(new DBSPDeclaration(new DBSPFunctionItem(function)));
            return function;
        } else {
            return stat.function.getDeclaration(this.compiler.getTypeCompiler());
        }
    }

    @Nullable
    DBSPNode compileLateness(LatenessStatement stat) {
        ExpressionCompiler compiler = new ExpressionCompiler(null, null, this.compiler);
        DBSPExpression lateness = compiler.compile(stat.value);
        ViewColumnMetadata vcm = new ViewColumnMetadata(
                stat.getCalciteObject(), Utilities.toIdentifier(stat.view),
                Utilities.toIdentifier(stat.column), null, lateness);
        if (!this.viewMetadata.containsKey(vcm.viewName))
            this.viewMetadata.put(vcm.viewName, new HashMap<>());
        Map<ProgramIdentifier, ViewColumnMetadata> map = this.viewMetadata.get(vcm.viewName);
        if (map.containsKey(vcm.columnName)) {
            this.compiler.reportError(stat.getPosition(), "Duplicate",
                    "Lateness for " + vcm.viewName + "." + vcm.columnName + " already declared");
            this.compiler.reportError(map.get(vcm.columnName).getNode().getPositionRange(), "Duplicate",
                    "Location of the previous declaration", true);
        } else {
            map.put(vcm.columnName, vcm);
        }
        return null;
    }

    @SuppressWarnings("UnusedReturnValue")
    @Nullable
    public DBSPNode compile(FrontEndStatement statement) {
        if (statement.is(CreateViewStatement.class)) {
            CreateViewStatement view = statement.to(CreateViewStatement.class);
            return this.compileCreateView(view);
        } else if (statement.is(CreateTableStatement.class) ||
                statement.is(DropTableStatement.class)) {
            boolean success = this.tableContents.execute(statement);
            if (!success)
                return null;
            CreateTableStatement create = statement.as(CreateTableStatement.class);
            if (create != null) {
                // We create an input for the circuit.
                return this.compileCreateTable(create);
            }
            return null;
        } else if (statement.is(TableModifyStatement.class)) {
            TableModifyStatement modify = statement.to(TableModifyStatement.class);
            return this.compileModifyTable(modify);
        } else if (statement.is(CreateTypeStatement.class)) {
            CreateTypeStatement stat = statement.to(CreateTypeStatement.class);
            return this.compileCreateType(stat);
        } else if (statement.is(CreateFunctionStatement.class)) {
            CreateFunctionStatement stat = statement.to(CreateFunctionStatement.class);
            return this.compileCreateFunction(stat);
        } else if (statement.is(LatenessStatement.class)) {
            LatenessStatement stat = statement.to(LatenessStatement.class);
            return this.compileLateness(stat);
        } else if (statement.is(DeclareViewStatement.class)) {
            DeclareViewStatement decl = statement.to(DeclareViewStatement.class);
            return this.compileDeclareView(decl);
        }
        throw new UnsupportedException(statement.getCalciteObject());
    }

    private DBSPZSetLiteral compileConstantProject(LogicalProject project) {
        // Specialization of the visitor's visit method for LogicalProject
        // Does not produce a DBSPOperator, but only a literal.
        CalciteObject node = CalciteObject.create(project);
        DBSPType outputElementType = this.convertType(project.getRowType(), false);
        DBSPTypeTuple tuple = outputElementType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(project.getInput().getRowType(), false);
        DBSPVariablePath row = inputType.ref().var();  // should not be used
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(project, row, this.compiler);
        DBSPZSetLiteral result = DBSPZSetLiteral.emptyWithElementType(outputElementType);

        List<DBSPExpression> resultColumns = new ArrayList<>();
        int index = 0;
        for (RexNode column : project.getProjects()) {
            DBSPExpression exp = expressionCompiler.compile(column);
            DBSPType expectedType = tuple.getFieldType(index);
            if (!exp.getType().sameType(expectedType)) {
                // Calcite's optimizations do not preserve types!
                exp = exp.applyCloneIfNeeded().cast(expectedType);
            }
            resultColumns.add(exp);
            index++;
        }

        DBSPExpression exp = new DBSPTupleExpression(node, resultColumns);
        result.add(exp);
        return result;
    }

    public TableContents getTableContents() {
        return this.tableContents;
    }

    public void clearTables() {
        this.tableContents.clear();
    }
}
