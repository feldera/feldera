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
import org.apache.calcite.rel.core.Filter;
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
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.ddl.SqlAttributeDefinition;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.aggregates.AggregateCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.aggregates.RankAggregate;
import org.dbsp.sqlCompiler.compiler.frontend.aggregates.WindowAggregates;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlUserDefinedAggregationFunction;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.LastRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.IntermediateRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelAnd;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CalciteTableDescription;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateAggregateStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateFunctionStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateIndexStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTypeStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DeclareViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DropTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.RelStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.HasSchema;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlRemove;
import org.dbsp.sqlCompiler.compiler.frontend.statements.TableModifyStatement;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FieldUseMap;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindUnusedFields;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEqualityComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.ICollectionType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.IHasZero;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.ICastable;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.IdShuffle;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Shuffle;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.*;

/**
 * The compiler is stateful: it compiles a sequence of SQL statements
 * defining tables and views.  The views must be defined in terms of
 * the previously defined tables and views.  Multiple views can be
 * compiled.  The result is a circuit which has an input for each table
 * and an output for each (non-local) view. */
public class CalciteToDBSPCompiler extends RelVisitor
        implements IWritesLogs, ICompilerComponent {
    // Result is deposited here
    @Nullable private DBSPCircuit circuit;
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

    /**
     * Create a compiler that translated from calcite to DBSP circuits.
     * @param options             Options for compilation.
     * @param compiler            Parent compiler; used to report errors.
     */
    public CalciteToDBSPCompiler(CompilerOptions options, DBSPCompiler compiler,
                                 ProgramMetadata metadata) {
        this.circuit = new DBSPCircuit(compiler.metadata);
        this.compiler = compiler;
        this.nodeOperator = new HashMap<>();
        this.tableContents = new TableContents(compiler);
        this.options = options;
        this.ancestors = new ArrayList<>();
        this.metadata = metadata;
    }

    private DBSPCircuit getCircuit() {
        return Objects.requireNonNull(this.circuit);
    }

    public void addOperator(DBSPOperator operator) {
        this.getCircuit().addOperator(operator);
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
    }

    public DBSPType convertType(SourcePositionRange context, RelDataType dt, boolean asStruct) {
        return this.compiler.getTypeCompiler().convertType(context, dt, asStruct);
    }

    private static DBSPTypeIndexedZSet makeIndexedZSet(DBSPType keyType, DBSPType valueType) {
        return TypeCompiler.makeIndexedZSet(keyType, valueType);
    }

    /** Gets the circuit produced so far and starts a new one. */
    @Nullable public DBSPCircuit getFinalCircuit() {
        if (this.compiler.hasErrors()) {
            throw new CompilationError("Stopping compilation due to errors");
        }
        DBSPCircuit result = this.circuit;
        this.circuit = null;
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
                    .appendSupplier(node::toString)
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
    public DBSPAggregateList createAggregates(
            DBSPCompiler compiler,
            RelNode node, List<AggregateCall> aggregates, List<RexLiteral> constants, DBSPTypeTuple resultType,
            DBSPType inputRowType, int groupCount, ImmutableBitSet groupKeys, boolean linearAllowed) {
        Utilities.enforce(!aggregates.isEmpty());

        CalciteObject obj = CalciteObject.create(node);
        List<IAggregate> simple = new ArrayList<>();
        DBSPVariablePath rowVar = inputRowType.ref().var();
        {
            int aggIndex = 0;
            for (AggregateCall call : aggregates) {
                DBSPType resultFieldType = resultType.getFieldType(aggIndex + groupCount);
                AggregateCompiler aggCompiler = new AggregateCompiler(node,
                        compiler, call, constants, resultFieldType, rowVar, groupKeys, linearAllowed);
                IAggregate implementation = aggCompiler.compile();
                simple.add(implementation);
                aggIndex++;
            }
        }

        return new DBSPAggregateList(obj, rowVar, simple);
    }

    UnimplementedException decorrelateError(CalciteObject node) {
        return new UnimplementedException(
                "It looks like the compiler could not decorrelate this query.", 2555, node);
    }

    void visitCorrelate(LogicalCorrelate correlate) {
        /*
         We decorrelate queries using Calcite's optimizer, which doesn't always work.
         In particular, it won't decorrelate queries with unnest.
         Here we check for unnest-type queries.  We assume that unnest queries
         have a restricted plan of this form:
         LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{...}])
            LeftSubquery (arbitrary)
            Uncollect
              LogicalProject(COL=[$cor0.ARRAY])  // uncollectInput
                LogicalValues(tuples=[[{ 0 }]])
         or
         LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{...}])
            LeftSubquery
            LogicalFilter // rightFilter
              Uncollect
                LogicalProject(COL=[$cor0.ARRAY])  // uncollectInput
                  LogicalValues(tuples=[[{ 0 }]])
         or
         LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{...}])
            LeftSubquery
            LogicalProject // rightProject
              Uncollect
                LogicalProject(COL=[$cor0.ARRAY])  // uncollectInput
                  LogicalValues(tuples=[[{ 0 }]])
         Instead of projecting and joining again we directly apply flatmap.
         The translation for this is:
         stream.flat_map({
           move |x: &Tuple2<Vec<i32>, Option<i32>>, | -> _ {
             let xA: Vec<i32> = x.0.clone();
             let xB: x.1.clone();
             x.0.clone().into_iter().map({
                move |e: i32, | -> Tuple3<Vec<i32>, Option<i32>, i32> {
                    Tuple3::new(xA.clone(), xB.clone(), e)
                }
             })
         }});
         */
        CalciteObject node = CalciteObject.create(correlate);
        DBSPTypeTuple type = this.convertType(
                node.getPositionRange(), correlate.getRowType(), false).to(DBSPTypeTuple.class);
        /*
        The join type does not influence the results of the decorrelate!
        A Decorrelate is a cross join, and an outer cross join is equivalent to an inner cross join.
        if (correlate.getJoinType().isOuterJoin())
            throw this.decorrelateError(node);
         */
        this.visit(correlate.getLeft(), 0, correlate);
        DBSPSimpleOperator left = this.getInputAs(correlate.getLeft(), true);
        DBSPTypeTuple leftElementType = left.getOutputZSetElementType().to(DBSPTypeTuple.class);

        RelNode correlateRight = correlate.getRight();
        Project rightProject = null;
        Filter rightFilter = null;
        if (correlateRight instanceof Project) {
            rightProject = (Project) correlateRight;
            correlateRight = rightProject.getInput();
        } else if (correlateRight instanceof Filter) {
            rightFilter = (Filter) correlateRight;
            correlateRight = rightFilter.getInput();
        }
        if (!(correlateRight instanceof Uncollect uncollect))
            throw this.decorrelateError(node);
        RelNode uncollectInput = uncollect.getInput();
        if (!(uncollectInput instanceof LogicalProject project))
            throw this.decorrelateError(node);
        if (project.getProjects().size() != 1)
            throw this.decorrelateError(node);
        RexNode projection = project.getProjects().get(0);
        DBSPVariablePath dataVar = new DBSPVariablePath(leftElementType.ref());
        ExpressionCompiler eComp = new ExpressionCompiler(correlate, dataVar, this.compiler);
        DBSPClosureExpression arrayExpression = eComp.compile(projection).closure(dataVar);
        DBSPTypeTuple uncollectElementType = this.convertType(
                node.getPositionRange(), uncollect.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPType collectionElementType = arrayExpression.getResultType().to(ICollectionType.class).getElementType();
        if (collectionElementType.mayBeNull) {
            // This seems to be a bug in Calcite, we should not need to do this adjustment
            if (uncollect.withOrdinality) {
                List<DBSPType> fieldTypes = new ArrayList<>();
                DBSPTypeTuple tuple = uncollectElementType.to(DBSPTypeTuple.class);
                for (int i = 0; i < tuple.size(); i++) {
                    DBSPType ft = tuple.getFieldType(i);
                    if (i < tuple.size() - 1)
                        // skip the ordinality field
                        ft = ft.withMayBeNull(true);
                    fieldTypes.add(ft);
                }
                uncollectElementType = new DBSPTypeTuple(fieldTypes);
            } else {
                uncollectElementType = uncollectElementType.withMayBeNull(true).to(DBSPTypeTuple.class);
            }
        }

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

        int shuffleSize = leftElementType.size();
        if (rightProjections != null)
            shuffleSize += rightProjections.size();
        if (indexType != null) {
            if (indexType.is(DBSPTypeTupleBase.class))
                shuffleSize += indexType.to(DBSPTypeTupleBase.class).size();
            else
                shuffleSize += 1;
        }
        if (collectionElementType.is(DBSPTypeTupleBase.class))
            shuffleSize += collectionElementType.to(DBSPTypeTupleBase.class).size();
        else
            shuffleSize += 1;
        DBSPFlatmap flatmap = new DBSPFlatmap(node,
                leftElementType, arrayExpression,
                Linq.range(0, leftElementType.size()),
                rightProjections, indexType, new IdShuffle(shuffleSize));

        DBSPTypeFunction functionType = new DBSPTypeFunction(type, leftElementType.ref());
        Utilities.enforce(flatmap.getType().sameType(functionType),
                () -> "Expected type to be\n" + functionType + "\nbut it is\n" + flatmap.getType());
        DBSPSimpleOperator result = new DBSPFlatMapOperator(
                new LastRel(correlate, SourcePositionRange.INVALID),
                flatmap, TypeCompiler.makeZSet(type), left.outputPort());
        if (rightFilter != null) {
            // This is a specialized version of visit(LogicalFilter)
            DBSPVariablePath t = type.ref().var();
            // Here we apply the filter AFTER the flatmap, whereas the original
            // filter was applied BEFORE the flatmap.  So we need to adjust the
            // index of RexInputRef expressions in the condition to apply to the
            // result AFTER the join.
            ShiftingExpressionCompiler expressionCompiler = new ShiftingExpressionCompiler(
                    rightFilter, t, this.compiler, -correlate.getLeft().getRowType().getFieldCount());
            DBSPExpression condition = expressionCompiler.compile(rightFilter.getCondition());
            condition = condition.wrapBoolIfNeeded();
            condition = new DBSPClosureExpression(
                    CalciteObject.create(rightFilter, rightFilter.getCondition()), condition, t.asParameter());
            this.addOperator(result);
            result = new DBSPFilterOperator(
                    new LastRel(correlate, SourcePositionRange.INVALID), condition, result.outputPort());
        }

        Utilities.enforce(type.sameType(result.getOutputZSetElementType()));
        this.assignOperator(correlate, result);
    }

    /** Given a DESCRIPTOR RexCall, return the reference to the single colum
     * that is referred by the DESCRIPTOR. */
    int getDescriptor(RexNode descriptor) {
        Utilities.enforce(descriptor instanceof RexCall);
        RexCall call = (RexCall) descriptor;
        Utilities.enforce(call.getKind() == SqlKind.DESCRIPTOR);
        Utilities.enforce(call.getOperands().size() == 1);
        RexNode operand = call.getOperands().get(0);
        Utilities.enforce(operand instanceof RexInputRef);
        RexInputRef ref = (RexInputRef) operand;
        return ref.getIndex();
    }

    void compileTumble(LogicalTableFunctionScan scan, RexCall call) {
        IntermediateRel node = CalciteObject.create(scan);
        DBSPTypeTuple type = this.convertType(
                node.getPositionRange(), scan.getRowType(), false).to(DBSPTypeTuple.class);
        Utilities.enforce(scan.getInputs().size() == 1);
        RelNode input = scan.getInput(0);
        DBSPTypeTuple inputRowType = this.convertType(
                node.getPositionRange(), input.getRowType(), false).to(DBSPTypeTuple.class);
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
        Utilities.enforce(call.operandCount() == 2 || call.operandCount() == 3);
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

        DBSPExpression[] args = new DBSPExpression[2 + ((start != null) ? 1 : 0)];
        if (row.deref().getType().to(DBSPTypeTupleBase.class).getFieldType(timestampIndex).mayBeNull) {
            this.compiler.reportWarning(new SourcePositionRange(call.getParserPosition()),
                    "TUMBLE applied to nullable value",
                    "TIMESTAMP used for TUMBLE function is nullable; this can cause runtime crashes.\n" +
                            "We recommend filtering out null values before applying the TUMBLE function");
        }
        args[0] = row.deref().field(timestampIndex).unwrapIfNullable();
        args[1] = interval;
        if (start != null)
            args[2] = start;
        DBSPType tumbleType = type.tupFields[nextIndex];
        String typeName = "_" + interval.getType().to(DBSPTypeBaseType.class).shortName();
        String functionName = "tumble_" + args[0].getType().baseTypeWithSuffix() + typeName;
        if (start != null)
            functionName += "_" + start.getType().to(DBSPTypeBaseType.class).shortName();

        results[nextIndex] = new DBSPApplyExpression(node, functionName, tumbleType, args);
        results[nextIndex + 1] = ExpressionCompiler.makeBinaryExpression(node,
                tumbleType, DBSPOpcode.ADD, results[nextIndex], interval);

        DBSPClosureExpression func = new DBSPTupleExpression(results).closure(row);
        DBSPMapOperator result = new DBSPMapOperator(node.getFinal(), func, TypeCompiler.makeZSet(type), opInput.outputPort());
        this.assignOperator(scan, result);
    }

    void compileHop(LogicalTableFunctionScan scan, RexCall call) {
        IntermediateRel node = CalciteObject.create(scan);
        DBSPTypeTuple type = this.convertType(node.getPositionRange(), scan.getRowType(), false).to(DBSPTypeTuple.class);
        Utilities.enforce(scan.getInputs().size() == 1);
        RelNode input = scan.getInput(0);
        DBSPTypeTuple inputRowType = this.convertType(
                node.getPositionRange(), input.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        DBSPVariablePath row = inputRowType.ref().var();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(scan, row, this.compiler);
        List<RexNode> operands = call.getOperands();
        Utilities.enforce(call.operandCount() == 3 || call.operandCount() == 4);
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
            start = new DBSPIntervalMillisLiteral(DBSPTypeMillisInterval.Units.SECONDS, 0, false);

        DBSPHopOperator hop = new DBSPHopOperator(
                node.getFinal(), timestampIndex, interval, start, size, TypeCompiler.makeZSet(type), opInput.outputPort());
        this.assignOperator(scan, hop);
    }

    void visitTableFunction(LogicalTableFunctionScan tf) {
        CalciteObject node = CalciteObject.create(tf);
        RexNode operation = tf.getCall();
        Utilities.enforce(operation instanceof RexCall);
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
        IntermediateRel node = CalciteObject.create(uncollect);
        DBSPType type = this.convertType(node.getPositionRange(), uncollect.getRowType(), false);
        RelNode input = uncollect.getInput();
        DBSPTypeTuple inputRowType = this.convertType(node.getPositionRange(), input.getRowType(), false).to(DBSPTypeTuple.class);
        // We expect this to be a single-element tuple whose type is a vector.
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        DBSPType indexType = null;
        if (uncollect.withOrdinality) {
            DBSPTypeTuple tuple = type.to(DBSPTypeTuple.class);
            indexType = tuple.getFieldType(tuple.size() - 1);
        }
        if (inputRowType.size() > 1) {
            throw new UnimplementedException("UNNEST with multiple vectors", node);
        }
        DBSPVariablePath data = new DBSPVariablePath(inputRowType.ref());
        DBSPType arrayType = data.deref().field(0).getType();
        DBSPType collectionElementType = arrayType.to(ICollectionType.class).getElementType();
        DBSPClosureExpression getField0 = data.deref().field(0).closure(data);

        int shuffleSize = 0;
        if (indexType != null) {
            if (indexType.is(DBSPTypeTupleBase.class))
                shuffleSize += indexType.to(DBSPTypeTupleBase.class).size();
            else
                shuffleSize += 1;
        }
        if (collectionElementType.is(DBSPTypeTupleBase.class)) {
            shuffleSize += collectionElementType.to(DBSPTypeTupleBase.class).size();
        } else {
            shuffleSize += 1;
        }

        DBSPFlatmap function = new DBSPFlatmap(node, inputRowType, getField0,
                Linq.list(), null, indexType, new IdShuffle(shuffleSize));
        DBSPTypeFunction functionType = new DBSPTypeFunction(type, inputRowType.ref());
        Utilities.enforce(function.getType().sameType(functionType));
        DBSPFlatMapOperator flatMap = new DBSPFlatMapOperator(node.getFinal(), function,
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
    DBSPTupleExpression generateKeyExpression(CalciteObject node,
            ImmutableBitSet keyFields, ImmutableBitSet groupSet,
            DBSPVariablePath t, DBSPTypeTuple slice) {
        Utilities.enforce(groupSet.cardinality() == slice.size());
        DBSPExpression[] keys = new DBSPExpression[keyFields.cardinality()];
        int keyFieldIndex = 0;
        int groupSetIndex = 0;
        for (int index : groupSet) {
            if (keyFields.get(index)) {
                keys[keyFieldIndex] = t
                        .deref()
                        .field(index)
                        .applyCloneIfNeeded()
                        .cast(node, slice.getFieldType(groupSetIndex), false);
                keyFieldIndex++;
            }
            groupSetIndex++;
        }
        return new DBSPTupleExpression(keys);
    }

    /** Given two results of aggregation functions, join them on the common keys and concatenate the fields */
    public static DBSPStreamJoinIndexOperator combineTwoAggregates(
            CalciteRelNode node, DBSPType groupKeyType,
            DBSPSimpleOperator left, DBSPSimpleOperator right,
            Consumer<DBSPOperator> addOperator) {
        DBSPTypeTupleBase leftType = left.getOutputIndexedZSetType().getElementTypeTuple();
        DBSPTypeTupleBase rightType = right.getOutputIndexedZSetType().getElementTypeTuple();

        DBSPVariablePath leftVar = leftType.ref().var();
        DBSPVariablePath rightVar = rightType.ref().var();

        leftType = leftType.concat(rightType);
        DBSPTypeIndexedZSet joinOutputType = makeIndexedZSet(groupKeyType, leftType);

        DBSPVariablePath key = groupKeyType.ref().var();
        DBSPExpression body = new DBSPRawTupleExpression(
                DBSPTupleExpression.flatten(key.deref()),
                DBSPTupleExpression.flatten(leftVar.deref(), rightVar.deref()));
        DBSPClosureExpression appendFields = body.closure(key, leftVar, rightVar);

        DBSPStreamJoinIndexOperator result = new DBSPStreamJoinIndexOperator(
                node, joinOutputType, appendFields, false, left.outputPort(), right.outputPort());
        addOperator.accept(result);
        return result;
    }

    /** Implement an AggregateList using a {@link DBSPStreamAggregateOperator} operator */
    public DBSPSimpleOperator implementAggregateList(
            CalciteRelNode node, DBSPType groupKeyType, OutputPort indexedInput, DBSPAggregateList agg) {
        DBSPTypeTuple typeFromAggregate = agg.getEmptySetResultType();
        DBSPTypeIndexedZSet aggregateType = makeIndexedZSet(groupKeyType, typeFromAggregate);

        DBSPSimpleOperator aggOp = new DBSPStreamAggregateOperator(
                node, aggregateType, null, agg, indexedInput);
        this.addOperator(aggOp);
        return aggOp;
    }

    /** Given a list of aggregates, combine them using joins in a balanced binary tree. */
    public static DBSPSimpleOperator combineAggregateList(
            CalciteRelNode node, DBSPType groupKeyType, List<DBSPSimpleOperator> aggregates,
            Consumer<DBSPOperator> addOperator) {
        if (aggregates.size() == 1) {
            return aggregates.get(0);
        }

        List<DBSPSimpleOperator> pairs = new ArrayList<>();
        for (int i = 0; i < aggregates.size(); i += 2) {
            if (i == aggregates.size() - 1) {
                pairs.add(aggregates.get(i));
            } else {
                DBSPSimpleOperator join = combineTwoAggregates(
                        node, groupKeyType, aggregates.get(i), aggregates.get(i + 1), addOperator);
                pairs.add(join);
            }
        }
        return combineAggregateList(node, groupKeyType, pairs, addOperator);
    }

    /** Implement one list of aggregates (possibly from a rollup) described by a LogicalAggregate. */
    OutputPort implementAggregateList(LogicalAggregate aggregate, ImmutableBitSet localKeys) {
        CalciteRelNode node = CalciteObject.create(aggregate);
        RelNode input = aggregate.getInput();
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        List<AggregateCall> aggregateCalls = aggregate.getAggCallList();

        DBSPType type = this.convertType(node.getPositionRange(), aggregate.getRowType(), false);
        DBSPTypeTuple tuple = type.to(DBSPTypeTuple.class);
        DBSPType inputRowType = this.convertType(node.getPositionRange(), input.getRowType(), false);
        DBSPVariablePath t = inputRowType.ref().var();
        DBSPTypeTuple keySlice = tuple.slice(0, aggregate.getGroupSet().cardinality());
        DBSPTupleExpression globalKeys = this.generateKeyExpression(node,
                aggregate.getGroupSet(), aggregate.getGroupSet(), t, keySlice);
        DBSPType[] aggTypes = Utilities.arraySlice(tuple.tupFields, aggregate.getGroupCount());
        DBSPTypeTuple aggType = new DBSPTypeTuple(aggTypes);

        DBSPTupleExpression localKeyExpression = this.generateKeyExpression(node,
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
        DBSPAggregateList aggregates = null;
        if (aggregateCalls.isEmpty()) {
            // No aggregations: just apply distinct
            DBSPVariablePath var = localGroupAndInput.getKVRefType().var();
            DBSPExpression addEmpty = new DBSPRawTupleExpression(
                    var.field(0).deref().applyCloneIfNeeded(),
                    new DBSPTupleExpression());
            DBSPSimpleOperator ix2 = new DBSPMapIndexOperator(node, addEmpty.closure(var),
                    makeIndexedZSet(localGroupType, DBSPTypeTuple.EMPTY), indexedInput.outputPort());
            this.addOperator(ix2);
            result = new DBSPStreamDistinctOperator(node, ix2.outputPort());
            this.addOperator(result);
        } else {
            aggregates = createAggregates(this.compiler(),
                    aggregate, aggregateCalls, Linq.list(), tuple, inputRowType,
                    aggregate.getGroupCount(), localKeys, true);
            result = this.implementAggregateList(node, localGroupType, indexedInput.outputPort(), aggregates);
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
                    Utilities.enforce(i < reindexFields.length);
                    Utilities.enforce(globalKeys.fields != null);
                    reindexFields[i] = DBSPLiteral.none(globalKeys.fields[i].getType());
                }
                i++;
            }
            DBSPExpression remap = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(reindexFields),
                    reindexVar.field(1).deref().applyCloneIfNeeded());
            result = new DBSPMapIndexOperator(node, remap.closure(reindexVar), result.outputPort());
            this.addOperator(result);
        }

        // Flatten the resulting set and adjust the types
        DBSPTypeTupleBase kvType = new DBSPTypeRawTuple(globalKeys.getType().ref(),
                result.getOutputIndexedZSetType().elementType.ref());
        DBSPVariablePath kv = kvType.var();
        DBSPExpression[] flattenFields = new DBSPExpression[aggregate.getGroupCount() + aggType.size()];
        for (int i = 0; i < aggregate.getGroupCount(); i++) {
            DBSPExpression cast = kv.field(0).deref().field(i).applyCloneIfNeeded()
                    .cast(node, tuple.getFieldType(i), false);
            flattenFields[i] = cast;
        }
        for (int i = 0; i < aggType.size(); i++) {
            DBSPExpression flattenField = kv.field(1).deref().field(i).applyCloneIfNeeded();
            // Here we correct from the type produced by the Folder (typeFromAggregate) to the
            // actual expected type aggType (which is the tuple of aggTypes).
            DBSPExpression cast = flattenField.cast(node, aggTypes[i], false);
            flattenFields[aggregate.getGroupCount() + i] = cast;
        }

        DBSPExpression mapper = new DBSPTupleExpression(node, tuple, flattenFields).closure(kv);
        DBSPSimpleOperator map = new DBSPMapOperator(node, mapper, TypeCompiler.makeZSet(tuple), result.outputPort());
        this.addOperator(map);
        if (aggregate.getGroupCount() != 0 || aggregateCalls.isEmpty()) {
            return map.outputPort();
        }

        DBSPExpression emptyResults = Objects.requireNonNull(aggregates).getEmptySetResult();
        DBSPBaseTupleExpression emptySetResult = DBSPTupleExpression.flatten(emptyResults);
        DBSPAggregateZeroOperator zero = new DBSPAggregateZeroOperator(node, emptySetResult, map.outputPort());
        this.addOperator(zero);
        return zero.outputPort();
    }

    /** Implement a LogicalAggregate.  The LogicalAggregate can contain a rollup,
     * described by a set of groups.  The aggregate is computed for each group,
     * and the results are combined. */
    void visitAggregate(LogicalAggregate aggregate) {
        IntermediateRel node = CalciteObject.create(aggregate);
        DBSPType type = this.convertType(node.getPositionRange(), aggregate.getRowType(), false);
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
            DBSPVariablePath var = opInput.getOutputZSetElementType().ref().var();
            DBSPExpression[] fields = new DBSPExpression[aggregate.getGroupCount()];
            int index = 0;
            for (int i: aggregate.getGroupSet()) {
                // This is not a noop when the field is a tuple.
                fields[index++] = ExpressionCompiler.expandTuple(node, var.deref().field(i));
            }
            DBSPTupleExpression tup = new DBSPTupleExpression(fields);
            DBSPMapOperator map = new DBSPMapOperator(node.intermediate(), tup.closure(var), opInput.outputPort());
            this.addOperator(map);

            DBSPSimpleOperator result = new DBSPStreamDistinctOperator(
                    node.getFinal(), map.outputPort());
            Utilities.enforce(result.getOutputZSetElementType().sameType(type));
            this.assignOperator(aggregate, result);
        } else {
            // One aggregate for each group
            List<OutputPort> aggregates = Linq.map(plan, b -> this.implementAggregateList(aggregate, b));
            // The result is the sum of all aggregates
            DBSPSimpleOperator sum = new DBSPSumOperator(node.getFinal(), aggregates);
            Utilities.enforce(sum.getOutputZSetElementType().sameType(type));
            this.assignOperator(aggregate, sum);
        }
    }

    void visitScan(TableScan scan, boolean create) {
        var node = CalciteObject.create(scan);

        RelOptTable tbl = scan.getTable();
        Utilities.enforce(tbl != null);
        ProgramIdentifier tableName = null;
        if (tbl instanceof RelOptTableImpl impl) {
            CalciteTableDescription descr = impl.unwrap(CalciteTableDescription.class);
            if (descr != null)
                tableName = descr.getName();
        }
        if (tableName == null) {
            tableName = Utilities.toIdentifier(tbl.getQualifiedName());
        }
        @Nullable
        IInputOperator source = this.getCircuit().getInput(tableName);
        // The inputs should have been created while parsing the CREATE TABLE statements.
        if (source != null) {
            // Multiple queries can share an input.
            // Or the input may have been created by a CREATE TABLE statement.
            DBSPSourceTableOperator table = source.asOperator().to(DBSPSourceTableOperator.class);
            Utilities.putNew(this.nodeOperator, scan, table);
            table.refer(scan);
        } else {
            // Try a view if no table with this name exists.
            DBSPViewOperator sourceView = this.getCircuit().getView(tableName);
            if (sourceView != null) {
                Utilities.putNew(this.nodeOperator, scan, sourceView);
            } else {
                if (!create)
                    throw new InternalCompilerError("Could not find operator for table " + tableName, node);

                // Create external tables table
                JdbcTableScan jscan = (JdbcTableScan) scan;
                RelDataType tableRowType = jscan.jdbcTable.getRowType(this.compiler.sqlToRelCompiler.typeFactory);
                DBSPTypeStruct originalRowType = this.convertType(node.getPositionRange(), tableRowType, true)
                        .to(DBSPTypeStruct.class)
                        .rename(tableName);
                DBSPType rowType = originalRowType.toTuple();
                HasSchema withSchema = new HasSchema(CalciteObject.EMPTY, tableName, tableRowType);
                this.metadata.addTable(withSchema);
                TableMetadata tableMeta = new TableMetadata(
                        tableName, Linq.map(withSchema.getColumns(), this::convertMetadata), Linq.list(),
                        false, false);
                DBSPSourceMultisetOperator sourceMulti = new DBSPSourceMultisetOperator(
                        node, CalciteObject.EMPTY,
                        TypeCompiler.makeZSet(rowType), originalRowType,
                        tableMeta, tableName, null);
                this.addOperator(sourceMulti);
                Utilities.putNew(this.nodeOperator, scan, sourceMulti);
            }
        }
    }

    void assignOperator(RelNode rel, DBSPSimpleOperator op) {
        Utilities.enforce(op.getNode().is(LastRel.class));
        Utilities.enforce(op.getNode().to(LastRel.class).relNode == rel);
        Utilities.putNew(this.nodeOperator, rel, op);
        this.addOperator(op);
    }

    DBSPSimpleOperator getOperator(RelNode node) {
        return Utilities.getExists(this.nodeOperator, node);
    }

    void visitProject(LogicalProject project) {
        IntermediateRel node = CalciteObject.create(project);
        RelNode input = project.getInput();
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        DBSPType outputElementType = this.convertType(node.getPositionRange(), project.getRowType(), false);
        DBSPTypeTuple tuple = outputElementType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(node.getPositionRange(), project.getInput().getRowType(), false);
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
                    exp = ExpressionCompiler.expandTupleCast(exp.getNode(), exp, expectedType);
                } else {
                    exp = exp.applyCloneIfNeeded();
                }
                resultColumns[index] = exp;
                index++;
            }
        }
        DBSPExpression exp = new DBSPTupleExpression(node, tuple, resultColumns);
        DBSPExpression mapFunc = new DBSPClosureExpression(node, exp, row.asParameter());
        DBSPMapOperator op = new DBSPMapOperator(
                node.getFinal(), mapFunc, TypeCompiler.makeZSet(outputElementType), opInput.outputPort());
        // No distinct needed - in SQL project may produce a multiset.
        this.assignOperator(project, op);
    }

    DBSPSimpleOperator castOutput(CalciteRelNode node, DBSPSimpleOperator operator, DBSPType outputElementType) {
        DBSPType inputElementType = operator.getOutputZSetElementType();
        if (inputElementType.sameType(outputElementType))
            return operator;
        DBSPExpression function = inputElementType.caster(outputElementType, false).reduce(this.compiler);
        DBSPSimpleOperator map = new DBSPMapOperator(
                node, function, TypeCompiler.makeZSet(outputElementType), operator.outputPort());
        this.addOperator(map);
        return map;
    }

    boolean seemsGenerated(String name) {
        return name.contains("$EXPR");
    }

    /** Check if the fields of the left data type are a non-identical permutation
     * of the fields of the right data type.  Return 'true' if that happens. */
    boolean checkPermuted(CalciteObject node, String statement, RelDataType left, RelDataType right) {
        if (left.equals(right))
            return false;
        if (!(left instanceof RelRecordType) || !(right instanceof  RelRecordType))
            return false;
        // Turns out that types may not match in nullability...
        // so we need to check whether the field names are the same but the types differ
        Set<String> leftNames = new HashSet<>();
        Utilities.enforce(left.getFieldCount() == right.getFieldCount());
        var leftFields = left.getFieldList();
        for (var field: leftFields) {
            String name = field.getName();
            if (this.seemsGenerated(name))
                return false;
            leftNames.add(field.getName());
        }
        boolean anyDifferent = false;
        int position = 0;
        for (var field: right.getFieldList()) {
            String name = field.getName();
            String leftName = leftFields.get(position).getName();
            position++;
            if (this.seemsGenerated(name))
                return false;
            leftNames.remove(name);
            if (!leftName.equals(name))
                anyDifferent = true;
        }
        if (!leftNames.isEmpty() || !anyDifferent)
            // Field names do not match
            return false;
        this.compiler.reportWarning(node.getPositionRange(), "Fields reordered",
                        "The input collections of a " + Utilities.singleQuote(statement) +
                                " operation have columns with the same names, but in a different order.  This may be a mistake.\n" +
                                "Note that " + statement + " never reorders fields.");
        this.compiler.reportWarning(node.getPositionRange(),"Fields reordered",
                "First type: " + SqlToRelCompiler.typeToColumns(ProgramIdentifier.EMPTY, left), true);
        this.compiler.reportWarning(node.getPositionRange(), "Fields reordered",
                "Mismatched type: " + SqlToRelCompiler.typeToColumns(ProgramIdentifier.EMPTY, right), true);
        return true;
    }

    private void checkPermutation(CalciteObject node, List<RelNode> inputs, String operation) {
        if (inputs.size() > 1) {
            RelDataType first = inputs.get(0).getRowType();
            for (int i = 1; i < inputs.size(); i++) {
                RelDataType nextType = inputs.get(i).getRowType();
                if (this.checkPermuted(node, operation, first, nextType))
                    // One warning is enough
                    break;
            }
        }
    }

    private void visitUnion(LogicalUnion union) {
        IntermediateRel node = CalciteObject.create(union);
        RelDataType rowType = union.getRowType();
        DBSPType outputType = this.convertType(node.getPositionRange(), rowType, false);
        List<RelNode> unionInputs = union.getInputs();
        List<DBSPSimpleOperator> inputs = Linq.map(unionInputs, this::getOperator);
        this.checkPermutation(node, unionInputs, "UNION");

        // input type nullability may not match
        List<OutputPort> ports = Linq.map(inputs, o -> this.castOutput(node, o, outputType).outputPort());
        if (union.all) {
            DBSPSumOperator sum = new DBSPSumOperator(node.getFinal(), ports);
            Utilities.enforce(sum.getOutputZSetElementType().sameType(outputType));
            this.assignOperator(union, sum);
        } else {
            DBSPSumOperator sum = new DBSPSumOperator(node, ports);
            this.addOperator(sum);
            DBSPStreamDistinctOperator d = new DBSPStreamDistinctOperator(node.getFinal(), sum.outputPort());
            Utilities.enforce(sum.getOutputZSetElementType().sameType(outputType));
            this.assignOperator(union, d);
        }
    }

    private void visitMinus(LogicalMinus minus) {
        IntermediateRel node = CalciteObject.create(minus);
        boolean first = true;
        RelDataType rowType = minus.getRowType();
        DBSPType outputType = this.convertType(node.getPositionRange(), rowType, false);
        List<OutputPort> inputs = new ArrayList<>();
        this.checkPermutation(node, minus.getInputs(), "EXCEPT");

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

        if (minus.all) {
            DBSPSumOperator sum = new DBSPSumOperator(node.getFinal(), inputs);
            this.assignOperator(minus, sum);
        } else {
            DBSPSumOperator sum = new DBSPSumOperator(node, inputs);
            this.addOperator(sum);
            DBSPStreamDistinctOperator d = new DBSPStreamDistinctOperator(node.getFinal(), sum.outputPort());
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

        IntermediateRel node = CalciteObject.create(filter);
        DBSPType type = this.convertType(node.getPositionRange(), filter.getRowType(), false);
        DBSPVariablePath t = type.ref().var();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(filter, t, this.compiler);
        DBSPExpression condition = expressionCompiler.compile(filter.getCondition());
        condition = condition.wrapBoolIfNeeded();
        condition = new DBSPClosureExpression(
                CalciteObject.create(filter, filter.getCondition()), condition, t.asParameter());
        DBSPSimpleOperator input = this.getOperator(filter.getInput());
        DBSPFilterOperator fop = new DBSPFilterOperator(node.getFinal(), condition, input.outputPort());
        Utilities.enforce(type.sameType(fop.getOutputZSetElementType()));
        this.assignOperator(filter, fop);
    }

    /** Apply a filter on a set of fields, keeping only rows that have non-null values in all these fields
     * @param join    Join node that requires filtering the inputs.
     * @param fields  List of fields to filter.
     * @param input   Join input node that is being filtered.
     * @param complement If true, revert the filter.
     * @return        An operator that performs the filtering.  If none of the fields are
     *                nullable, the original input is returned. */
    private DBSPSimpleOperator filterNonNullFields(CalciteObject node,
            Join join, List<Integer> fields, DBSPSimpleOperator input, boolean complement) {
        var relNode = CalciteObject.create(join);
        DBSPTypeTuple rowType = input.getOutputZSetElementType().to(DBSPTypeTuple.class);
        boolean shouldFilter = Linq.any(fields, i -> rowType.tupFields[i].mayBeNull);
        if (!shouldFilter) {
            if (!complement)
                return input;
            else {
                // result is empty
                var constant = new DBSPConstantOperator(
                        relNode, DBSPZSetExpression.emptyWithElementType(rowType), false);
                this.addOperator(constant);
                return constant;
            }
        }

        DBSPVariablePath var = rowType.ref().var(node);
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
        if (!complement)
            // This is right, if we don't complement we use a "not".
            condition = condition.not();
        DBSPClosureExpression filterFunc = condition.closure(var);
        DBSPFilterOperator filter = new DBSPFilterOperator(relNode, filterFunc, input.outputPort());
        this.addOperator(filter);
        return filter;
    }

    /** Keeps track of the fields of a tuple that are used as key fields.
     * Note that key fields may be duplicated.
     * E.g., a key may contain fields 1, 3, and 1 of a tuple of size 5, in this order.
     * [_, 0 and 2, _, 1, _]. */
    static class KeyFields {
        /** Indexes of fields that belong to the key, in the order they appear in the key: [1, 3, 1] */
        private final List<Integer> keyFields;
        private final Set<Integer> keyFieldSet;

        public KeyFields() {
            this.keyFieldSet = new HashSet<>();
            this.keyFields = new ArrayList<>();
        }

        public KeyFields(KeyFields other) {
            this.keyFieldSet = new HashSet<>(other.keyFieldSet);
            this.keyFields = new ArrayList<>(other.keyFields);
        }

        public boolean isKeyField(int i) {
            return this.keyFieldSet.contains(i);
        }

        /** Call this function to add key fields in the order they appear in the key */
        public void add(int index) {
            this.keyFieldSet.add(index);
            this.keyFields.add(index);
        }

        public void addAll(List<Integer> indexes) {
            for (int i: indexes)
                this.add(i);
        }

        public void addAllUsed(@Nullable FieldUseMap map) {
            if (map == null)
                return;
            if (map.isRef())
                map = map.deref();
            this.addAll(map.getUsedFields());
        }

        /** Extract the non-key fields from a value into a tuple, in the order they appear in the tuple.
         * For our example, the result will be [e.0, e.2, e.4]
         *
         * @param e Expression that is a reference to a tuple.
         * @return  A tuple containing just the non-key fields, in order.
         */
        DBSPTupleExpression nonKeyFields(DBSPExpression e) {
            // Almost like nonKeyFields with 3 arguments, except we don't insert casts ever
            // return this.nonKeyFields(var, e.getType().to(DBSPTypeTupleBase.class), 0);
            List<DBSPExpression> fields = new ArrayList<>();
            DBSPTypeTupleBase tuple = e.getType().to(DBSPTypeTupleBase.class);
            for (int i = 0; i < tuple.size(); i++) {
                if (this.isKeyField(i))
                    continue;
                fields.add(e.field(i).applyCloneIfNeeded());
            }
            return new DBSPTupleExpression(fields, false);
        }

        /** Extract the non-key fields from a value into a tuple, and insert casts if needed.
         * For our example, the result will be [(desiredType[0])e.0, (desiredType[1])e.2, (desiredType[2])e.4]
         *
         * @param e        Expression that is a reference to a tuple.
         * @param desiredType Type to cast resulting tuple to.
         * @return          A tuple containing just the non-key fields, in order.
         */
        DBSPTupleExpression nonKeyFields(DBSPExpression e, DBSPTypeTupleBase desiredType) {
            List<DBSPExpression> fields = new ArrayList<>();
            Utilities.enforce(e.getType().getToplevelFieldCount() >= desiredType.size());
            for (int i = 0; i < desiredType.size(); i++) {
                if (this.isKeyField(i))
                    continue;
                fields.add(e.field(i)
                        .applyCloneIfNeeded()
                        .nullabilityCast(desiredType.getFieldType(i), false));
            }
            return new DBSPTupleExpression(fields, false);
        }

        /** Extract the key fields from a value (e), in the order they have to appear in the key.
         * For our example the result will be [(keyType[0])e.1, (keyType[1])e.3, (keyType[2])e.1]
         *
         * @param e       Expression that is a reference to a tuple.
         * @param keyType Type for the key fields in the result; may introduce nullability casts.
         * @return A tuple containing just the key fields, in order.
         */
        DBSPTupleExpression keyFields(DBSPExpression e, DBSPTypeTupleBase keyType) {
            List<DBSPExpression> fields = new ArrayList<>();
            Utilities.enforce(keyType.size() == this.size());
            int index = 0;
            for (int kf: this.keyFields) {
                fields.add(e.field(kf)
                        .applyCloneIfNeeded()
                        .nullabilityCast(keyType.getFieldType(index), false));
                index++;
            }
            return new DBSPTupleExpression(fields, false);
        }

        /** Given a type for the row, return the type for the key */
        DBSPTypeTuple keyType(DBSPTypeTupleBase row) {
            List<DBSPType> fields = new ArrayList<>();
            for (int kf: this.keyFields) {
                fields.add(row.getFieldType(kf));
            }
            return new DBSPTypeTuple(fields);
        }

        /**
         * Produce a list with original fields from two values: the key value, and the data value.
         *
         * @param key       Expression containing the key fields, in order.
         * @param data      Expression containing the data fields, in order.
         * @param result    A list of the unshuffled fields.  The result is
         *                  [data.0, key.0, data.1, key.1, data.2]
         *                  (In this case we expect key.0 == key.2 always)
         */
        void unshuffleKeyAndDataFields(DBSPExpression key, DBSPExpression data, List<DBSPExpression> result) {
            Utilities.enforce(key.getType().getToplevelFieldCount() == this.size());
            int size = this.keyFieldSet.size() + data.getType().getToplevelFieldCount();
            int skipped = 0;
            boolean nullableData = data.getType().deref().mayBeNull;
            for (int i = 0; i < size; i++) {
                @Nullable DBSPExpression checkNull = nullableData ? data.deref().is_null() : null;
                if (this.isKeyField(i)) {
                    int index = 0;
                    for (int ki: this.keyFields) {
                        // There could be multiple positions, stop at the first one
                        if (ki == i)
                            break;
                        index++;
                    }
                    DBSPExpression keyField = key.deref().field(index).applyCloneIfNeeded();
                    if (nullableData) {
                        if (!keyField.getType().mayBeNull)
                            keyField = keyField.some();
                        keyField = new DBSPIfExpression(key.getNode(),
                                checkNull, keyField.getType().none(), keyField);
                    }
                    result.add(keyField);
                    skipped++;
                } else {
                    DBSPExpression dataField;
                    if (nullableData) {
                        dataField = data.deref().field(i - skipped).applyCloneIfNeeded();
                        if (!dataField.getType().mayBeNull)
                            dataField = dataField.some();
                        dataField = new DBSPIfExpression(key.getNode(),
                                checkNull, dataField.getType().none(), dataField);
                    }  else {
                        dataField = data.deref().field(i - skipped).applyCloneIfNeeded();
                    }
                    result.add(dataField);
                }
            }
        }

        private int size() {
            return this.keyFields.size();
        }

        @Override
        public String toString() {
            return "KeyFields: " + this.keyFields;
        }
    }

    /** Create the AND of a list of boolean expressions */
    DBSPExpression makeAnd(List<DBSPExpression> predicates) {
        DBSPExpression result = predicates.get(0);
        Utilities.enforce(result.getType().code == BOOL);
        for (int i = 1; i < predicates.size(); i++) {
            DBSPExpression next = predicates.get(i);
            Utilities.enforce(next.getType().code == BOOL);
            boolean nullable = result.getType().mayBeNull || next.getType().mayBeNull;
            result = ExpressionCompiler.makeBinaryExpression(
                    result.getNode(), DBSPTypeBool.create(nullable), DBSPOpcode.AND, result, next);
        }
        return result.wrapBoolIfNeeded();
    }

    /** Like {@link ExpressionCompiler}, but shift input references by the specified amount.
     * Used to pull predicates to the RHS of a join.  The indexes are expressed in terms of the
     * join output, which contains the left input concatenated with the right input. */
    static class ShiftingExpressionCompiler extends ExpressionCompiler {
        final int shiftAmount;

        public ShiftingExpressionCompiler(
                @Nullable RelNode context, @Nullable DBSPVariablePath inputRow, DBSPCompiler compiler, int shift) {
            super(context, inputRow, compiler);
            this.shiftAmount = shift;
        }

        /** Convert an expression that refers to a field in the input row.
         * @param inputRef   index in the input row.
         * @return           the corresponding DBSP expression. */
        @Override
        public DBSPExpression visitInputRef(RexInputRef inputRef) {
            CalciteObject node = CalciteObject.create(this.context, inputRef);
            if (this.inputRow == null)
                throw new InternalCompilerError("Row referenced without a row context", node);
            // Unfortunately it looks like we can't trust the type coming from Calcite.
            DBSPTypeTuple type = this.inputRow.getType().deref().to(DBSPTypeTuple.class);
            int index = inputRef.getIndex() - this.shiftAmount;
            if (index < type.size()) {
                DBSPExpression field = this.inputRow.deref().field(index);
                return field.applyCloneIfNeeded();
            }
            if (index - type.size() < this.constants.size())
                return this.visitLiteral(this.constants.get(index - type.size()));
            throw new InternalCompilerError("Index in row out of bounds ", node);
        }
    }

    /** If necessary add a new Map operator to do a pointwise case for the output of 'operator'
     * such that it matches the expectedType.  If a new operator is created, 'operator' is
     * added to the circuit, but the newly created operator is not.
     * @return Either the inserted operator or the original one if no insertion was needed.
     * @param isLast  If true mark this map as the final operator. */
    DBSPSimpleOperator insertCastMap(CalciteObject conditionNode, DBSPSimpleOperator operator,
            DBSPTypeTuple resultType, boolean isLast) {
        CalciteRelNode node = operator.getRelNode();
        if (isLast)
            node = node.to(IntermediateRel.class).getFinal();

        DBSPTypeTuple lrType = operator.getOutputZSetElementType().to(DBSPTypeTuple.class);
        if (resultType.sameType(lrType)) {
            if (isLast) {
                // Since we need to mark the node as final, insert a noop
                this.addOperator(operator);
                return new DBSPNoopOperator(node, operator.outputPort(), null);
            }
            return operator;
        }

        // For outer joins additional columns may become nullable.
        // Even for other joins, the fields we copied from keys may have lost their nullability.
        DBSPVariablePath t = new DBSPVariablePath(conditionNode, lrType.ref());
        DBSPExpression[] casts = new DBSPExpression[lrType.size()];
        for (int index = 0; index < lrType.size(); index++) {
            casts[index] = t.deref().field(index).applyCloneIfNeeded()
                    .cast(node, resultType.getFieldType(index), false);
        }
        DBSPTupleExpression allFields = new DBSPTupleExpression(casts);
        this.addOperator(operator);
        return new DBSPMapOperator(node, allFields.closure(t),
                TypeCompiler.makeZSet(resultType), operator.outputPort());
    }

    private void visitJoin(LogicalJoin join) {
        final CalciteObject conditionNode = CalciteObject.create(join, join.getCondition());
        final IntermediateRel node = CalciteObject.create(join, conditionNode.getPositionRange());
        // The result is the sum of all these operators.
        final List<OutputPort> sumInputs = new ArrayList<>();

        JoinRelType joinType = join.getJoinType();
        if (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI)
            throw new UnimplementedException("JOIN of type " + joinType + " not yet implemented", node);

        final DBSPTypeTuple resultType = this.convertType(node.getPositionRange(), join.getRowType(), false)
                .to(DBSPTypeTuple.class);
        if (join.getInputs().size() != 2)
            throw new InternalCompilerError("Unexpected join with " + join.getInputs().size() + " inputs", node);
        final DBSPSimpleOperator left = this.getInputAs(join.getInput(0), true);
        final DBSPSimpleOperator right = this.getInputAs(join.getInput(1), true);
        final DBSPTypeTuple leftElementType = left.getOutputZSetElementType().to(DBSPTypeTuple.class);
        final DBSPTypeTuple rightElementType = right.getOutputZSetElementType().to(DBSPTypeTuple.class);
        DBSPSimpleOperator leftPulled = left;
        DBSPSimpleOperator rightPulled = right;
        final int leftColumns = leftElementType.size();
        final int rightColumns = rightElementType.size();
        final int totalColumns = leftColumns + rightColumns;
        FieldUseMap leftPulledFields = null;
        FieldUseMap rightPulledFields = null;
        FieldUseMap conditionFields = null;

        final JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(
                leftElementType.to(DBSPTypeTuple.class).size(), this.compiler.getTypeCompiler());

        final JoinConditionAnalyzer.ConditionDecomposition decomposition = analyzer.analyze(join, join.getCondition());
        if (!decomposition.leftPredicates.isEmpty()) {
            // Pull up predicates that can be computed only on the left side to the left input
            DBSPVariablePath t = leftElementType.ref().var();
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(join, t, this.compiler);
            List<DBSPExpression> pullLeft = Linq.map(decomposition.leftPredicates, expressionCompiler::compile);
            DBSPExpression result = makeAnd(pullLeft);
            DBSPClosureExpression clo = result.closure(t);
            leftPulled = new DBSPFilterOperator(node, clo, left.outputPort());
            leftPulledFields = FindUnusedFields.computeUsedFields(clo, this.compiler);
            this.addOperator(leftPulled);
        }
        if (!decomposition.rightPredicates.isEmpty()) {
            // Pull up predicates that can be computed only on the right side to the right input
            DBSPVariablePath t = rightElementType.ref().var();
            ExpressionCompiler expressionCompiler = new ShiftingExpressionCompiler(join, t, this.compiler, leftColumns);
            List<DBSPExpression> pullRight = Linq.map(decomposition.rightPredicates, expressionCompiler::compile);
            DBSPExpression result = makeAnd(pullRight);
            DBSPClosureExpression clo = result.closure(t);
            rightPulled = new DBSPFilterOperator(node, clo, right.outputPort());
            rightPulledFields = FindUnusedFields.computeUsedFields(clo, this.compiler);
            this.addOperator(rightPulled);
        }

        if (decomposition.isCrossJoin())
            // An outer cross-join is always equivalent with an inner cross join
            joinType = JoinRelType.INNER;

        // If any key field that is compared with = is nullable we need to filter the inputs of the join;
        // this will make some key columns non-nullable
        final DBSPSimpleOperator filteredLeft = this.filterNonNullFields(conditionNode, join,
                Linq.map(Linq.where(decomposition.comparisons, JoinConditionAnalyzer.EqualityTest::nonNull),
                        JoinConditionAnalyzer.EqualityTest::leftColumn), leftPulled, false);
        final DBSPSimpleOperator filteredRight = this.filterNonNullFields(conditionNode, join,
                Linq.map(Linq.where(decomposition.comparisons, JoinConditionAnalyzer.EqualityTest::nonNull),
                        JoinConditionAnalyzer.EqualityTest::rightColumn), rightPulled, false);

        final DBSPTypeTuple leftResultType = resultType.slice(0, leftColumns);
        final DBSPTypeTuple rightResultType = resultType.slice(leftColumns, leftColumns + rightColumns);
        // Map a left variable field that is used as key field into the corresponding key field index
        final KeyFields lkf = new KeyFields();
        final KeyFields rkf = new KeyFields();
        {
            for (var x : decomposition.comparisons) {
                lkf.add(x.leftColumn());
                rkf.add(x.rightColumn());
            }
        }

        // Index the left and right inputs.
        // Note that if a field from one of the inputs is used in both the key and the data fields,
        // it is present only once in the corresponding index output.
        final DBSPMapIndexOperator leftNonNullIndex, rightNonNullIndex;
        final DBSPTypeTupleBase keyType, leftTupleType, rightTupleType;
        {
            final DBSPVariablePath l = leftElementType.ref().var(conditionNode);
            final DBSPVariablePath r = rightElementType.ref().var(conditionNode);
            final List<DBSPExpression> leftKeyFields = Linq.map(
                    decomposition.comparisons,
                    c -> l.deref().field(c.node(), c.leftColumn())
                            .applyCloneIfNeeded()
                            .cast(c.node(), c.commonType(), false));
            final List<DBSPExpression> rightKeyFields = Linq.map(
                    decomposition.comparisons,
                    c -> r.deref().field(c.node(), c.rightColumn())
                            .applyCloneIfNeeded()
                            .cast(c.node(), c.commonType(), false));
            final DBSPExpression leftKey = new DBSPTupleExpression(node, leftKeyFields);
            keyType = leftKey.getType().to(DBSPTypeTupleBase.class);

            final DBSPTupleExpression leftTuple = lkf.nonKeyFields(l.deref());
            final DBSPTupleExpression rightTuple = rkf.nonKeyFields(r.deref());
            leftTupleType = leftTuple.getType().to(DBSPTypeTupleBase.class);
            rightTupleType = rightTuple.getType().to(DBSPTypeTupleBase.class);
            final DBSPClosureExpression toLeftKey = new DBSPRawTupleExpression(leftKey, leftTuple)
                    .closure(l);
            leftNonNullIndex = new DBSPMapIndexOperator(
                    node, toLeftKey,
                    makeIndexedZSet(leftKey.getType(), leftTuple.getType()), false, filteredLeft.outputPort());
            this.addOperator(leftNonNullIndex);

            final DBSPExpression rightKey = new DBSPTupleExpression(node, rightKeyFields);
            final DBSPClosureExpression toRightKey = new DBSPRawTupleExpression(rightKey, rightTuple)
                    .closure(r);
            rightNonNullIndex = new DBSPMapIndexOperator(
                    node, toRightKey,
                    makeIndexedZSet(rightKey.getType(), rightTuple.getType()), false, filteredRight.outputPort());
            this.addOperator(rightNonNullIndex);
        }

        DBSPSimpleOperator joinResult;
        // Save the result of the inner join here
        DBSPSimpleOperator inner;
        final DBSPTypeTuple lrType;
        // True if there is a filter required after the non (i.e. non equi-join)
        boolean hasFilter = false;
        {
            final DBSPVariablePath k = keyType.ref().var(conditionNode);
            final DBSPVariablePath l0 = leftTupleType.ref().var(conditionNode);
            final DBSPVariablePath r0 = rightTupleType.ref().var(conditionNode);

            final List<DBSPExpression> joinFields = new ArrayList<>();
            lkf.unshuffleKeyAndDataFields(k, l0, joinFields);
            rkf.unshuffleKeyAndDataFields(k, r0, joinFields);
            final DBSPTupleExpression lr = new DBSPTupleExpression(joinFields, false);
            lrType = lr.getType().to(DBSPTypeTuple.class);
            final @Nullable RexNode leftOver = decomposition.getLeftOver();
            DBSPClosureExpression postJoinCondition = null;
            if (leftOver != null) {
                final DBSPVariablePath t = lr.getType().ref().var();
                final ExpressionCompiler expressionCompiler = new ExpressionCompiler(join, t, this.compiler);
                final DBSPExpression postJoinConditionBody = expressionCompiler.compile(leftOver).wrapBoolIfNeeded();
                postJoinCondition = new DBSPClosureExpression(
                        CalciteObject.create(join, join.getCondition()), postJoinConditionBody, t.asParameter());

                // Apply additional filters
                final DBSPBoolLiteral bLit = postJoinConditionBody.as(DBSPBoolLiteral.class);
                if (bLit == null || bLit.value == null || !bLit.value) {
                    // Technically if blit.value == null or !blit.value then
                    // the filter is false, and the result is empty.  But hopefully
                    // the calcite optimizer won't allow that.
                    hasFilter = true;
                }
                // if bLit is true, we don't need to filter.
            }

            final DBSPClosureExpression makeTuple = lr.closure(k, l0, r0);
            joinResult = new DBSPStreamJoinOperator(node, TypeCompiler.makeZSet(lr.getType()),
                    makeTuple, left.isMultiset || right.isMultiset,
                    leftNonNullIndex.outputPort(), rightNonNullIndex.outputPort());
            inner = joinResult;

            if (joinType == JoinRelType.LEFT && leftPulled == left && !hasFilter) {
                // Special handling for LEFT joins without a post-join filter and no left-pulled predicates:
                // use the built-in LeftJoin operator.
                // The two inputs are: left and filteredRight (so nulls in keys never match).
                final DBSPVariablePath l = leftElementType.ref().var(conditionNode);
                final DBSPTypeTuple bareKeyType = lkf.keyType(leftElementType);
                final DBSPRawTupleExpression leftInputColumns = new DBSPRawTupleExpression(
                        lkf.keyFields(l.deref(), bareKeyType),
                        lkf.nonKeyFields(l.deref()));
                final DBSPSimpleOperator leftIndex = new DBSPMapIndexOperator(
                        node, leftInputColumns.closure(l), left.outputPort());
                this.addOperator(leftIndex);

                // Index the right input to make the value type Option, required by the
                // DBSPLeftJoinOperator.
                final DBSPVariablePath r = rightElementType.ref().var(conditionNode);
                final DBSPRawTupleExpression rightInputColumns = new DBSPRawTupleExpression(
                        rkf.keyFields(r.deref(), bareKeyType),
                        rkf.nonKeyFields(r.deref()).some());
                final DBSPSimpleOperator rightIndex = new DBSPMapIndexOperator(
                        node, rightInputColumns.closure(r), filteredRight.outputPort());
                this.addOperator(rightIndex);

                // The LeftJoin operator is incremental-only, so we make an D/J/I sandwich
                final DBSPDifferentiateOperator leftDiff = new DBSPDifferentiateOperator(node, leftIndex.outputPort());
                this.addOperator(leftDiff);
                final DBSPDifferentiateOperator rightDiff = new DBSPDifferentiateOperator(node, rightIndex.outputPort());
                this.addOperator(rightDiff);

                final DBSPVariablePath k1 = bareKeyType.ref().var(conditionNode);
                final DBSPVariablePath l1 = leftIndex.getOutputIndexedZSetType().elementType.ref().var(conditionNode);
                final DBSPVariablePath r1 = rightIndex.getOutputIndexedZSetType().elementType.ref().var(conditionNode);
                final List<DBSPExpression> leftJoinFields = new ArrayList<>();
                lkf.unshuffleKeyAndDataFields(k1, l1, leftJoinFields);
                rkf.unshuffleKeyAndDataFields(k1, r1, leftJoinFields);
                final DBSPClosureExpression makeLeftTuple =
                        new DBSPTupleExpression(leftJoinFields, false).closure(k1, l1, r1);

                final DBSPLeftJoinOperator lj = new DBSPLeftJoinOperator(
                        node, TypeCompiler.makeZSet(makeLeftTuple.getResultType()),
                        makeLeftTuple, left.isMultiset || right.isMultiset,
                        leftDiff.outputPort(), rightDiff.outputPort());
                this.addOperator(lj);
                final DBSPIntegrateOperator integrate = new DBSPIntegrateOperator(node, lj.outputPort());
                // Additional casts may be needed to fix key field types on the left side
                final DBSPSimpleOperator result = this.insertCastMap(conditionNode, integrate, resultType, true);
                this.assignOperator(join, result);
                // Finished with simple left join operators.
                return;
            }

            if (hasFilter) {
                this.addOperator(joinResult);
                inner = new DBSPFilterOperator(node, postJoinCondition, joinResult.outputPort());
                joinResult = inner;
                conditionFields = FindUnusedFields.computeUsedFields(postJoinCondition, this.compiler);
            }
        }

        // For outer joins additional columns may become nullable.
        // Even for other joins, the fields we copied from keys may have lost their nullability.
        joinResult = this.insertCastMap(conditionNode, joinResult, resultType, false);
        // Handle outer joins
        this.addOperator(joinResult);
        sumInputs.add(joinResult.outputPort());
        final DBSPVariablePath joinVar = lrType.ref().var(conditionNode);

        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
            if (!hasFilter && decomposition.leftPredicates.isEmpty()) {
                // If there is no filter before or after the join, we can do a more efficient antijoin
                final DBSPTypeTuple toSubtractKeyType = rightNonNullIndex.getOutputIndexedZSetType().getKeyTypeTuple();
                final DBSPType leftKeyType = lkf.keyType(leftElementType);
                final DBSPTypeTuple antiJoinKeyType = TypeCompiler.reduceType(node, toSubtractKeyType, leftKeyType, "", true)
                        .to(DBSPTypeTuple.class);
                {
                    // Subtract (using an antijoin) the right from the left.
                    final DBSPSimpleOperator sub = new DBSPStreamAntiJoinOperator(
                            node, leftNonNullIndex.outputPort(), rightNonNullIndex.outputPort());
                    this.addOperator(sub);

                    // Adjust the subtraction result:
                    // fill nulls in the right relation fields
                    final DBSPVariablePath var = sub.getOutputIndexedZSetType().getKVRefType().var(conditionNode);
                    final DBSPTupleExpression rNulls = new DBSPTupleExpression(
                            Linq.map(rightElementType.tupFields,
                                    et -> DBSPLiteral.none(et.withMayBeNull(true)), DBSPExpression.class));
                    final List<DBSPExpression> fields = new ArrayList<>();
                    lkf.unshuffleKeyAndDataFields(var.field(0), var.field(1), fields);
                    final DBSPClosureExpression leftRow =
                            DBSPTupleExpression.flatten(
                                            new DBSPTupleExpression(fields, false),
                                            rNulls)
                                    .pointwiseCast(resultType)
                                    .closure(var);
                    final DBSPSimpleOperator expand = new DBSPMapOperator(
                            node, leftRow, TypeCompiler.makeZSet(resultType), sub.outputPort());
                    this.addOperator(expand);
                    sumInputs.add(expand.outputPort());
                }

                if (!toSubtractKeyType.sameType(antiJoinKeyType)) {
                    // Since the leftNonNullIndex does not contain some elements in left,
                    // so we need add them separately to the result sumInputs.
                    // remainderLeft is the complement of leftNonNullIndex
                    final DBSPSimpleOperator remainderLeft = this.filterNonNullFields(conditionNode, join,
                            Linq.map(Linq.where(decomposition.comparisons, JoinConditionAnalyzer.EqualityTest::nonNull),
                                    JoinConditionAnalyzer.EqualityTest::leftColumn), left, true);

                    final DBSPVariablePath l = leftElementType.ref().var(conditionNode);
                    final DBSPClosureExpression toLeftKey = new DBSPRawTupleExpression(
                            lkf.keyFields(l.deref(), lkf.keyType(leftElementType)),
                            lkf.nonKeyFields(l.deref(), leftResultType))
                            .closure(l);
                    final DBSPSimpleOperator remainderIndexed = new DBSPMapIndexOperator(
                            node, toLeftKey, remainderLeft.outputPort());
                    this.addOperator(remainderIndexed);

                    final DBSPVariablePath var = remainderIndexed.getOutputIndexedZSetType().getKVRefType().var(conditionNode);
                    final DBSPTupleExpression rNulls = new DBSPTupleExpression(
                            Linq.map(rightElementType.tupFields,
                                    et -> DBSPLiteral.none(et.withMayBeNull(true)), DBSPExpression.class));
                    final List<DBSPExpression> fields = new ArrayList<>();
                    lkf.unshuffleKeyAndDataFields(var.field(0), var.field(1), fields);
                    final DBSPClosureExpression leftRow =
                            DBSPTupleExpression.flatten(
                                            new DBSPTupleExpression(fields, false),
                                            rNulls)
                                    .pointwiseCast(resultType)
                                    .closure(var);
                    final DBSPSimpleOperator expand = new DBSPMapOperator(
                            node, leftRow, TypeCompiler.makeZSet(resultType), remainderIndexed.outputPort());
                    this.addOperator(expand);
                    sumInputs.add(expand.outputPort());
                }
            } else {
                // There is a filter before or after the join.
                // Index the input of the join as follows:
                // - key = (join key fields, condition fields part of left)
                // - value = the remaining left columns
                final KeyFields extendedLeft = new KeyFields(lkf);
                if (conditionFields != null) {
                    // If we pulled a predicate up, there may be no condition fields left
                    FieldUseMap leftConditionFields = conditionFields.deref().slice(0, leftColumns);
                    extendedLeft.addAllUsed(leftConditionFields);
                }
                extendedLeft.addAllUsed(leftPulledFields);
                extendedLeft.addAllUsed(rightPulledFields);

                final DBSPTypeTuple leftSubKeyType = extendedLeft.keyType(leftResultType);

                final DBSPVariablePath joinLeftVar = joinVar.getType().var(conditionNode);
                final DBSPRawTupleExpression projectJoinLeft = new DBSPRawTupleExpression(
                        extendedLeft.keyFields(joinLeftVar.deref(), leftSubKeyType),
                        new DBSPTupleExpression());

                final DBSPClosureExpression toLeftColumns = projectJoinLeft.closure(joinLeftVar);
                final DBSPSimpleOperator joinLeftColumns = new DBSPMapIndexOperator(
                        node, toLeftColumns, inner.outputPort());
                this.addOperator(joinLeftColumns);

                // Index the left collection in the same way
                final DBSPVariablePath l1 = leftElementType.ref().var(conditionNode);
                final DBSPExpression projectLeft = new DBSPRawTupleExpression(
                        extendedLeft.keyFields(l1.deref(), leftSubKeyType),
                        extendedLeft.nonKeyFields(l1.deref(), leftResultType));

                final DBSPSimpleOperator leftIndexed = new DBSPMapIndexOperator(
                        node, projectLeft.closure(l1), left.outputPort());
                this.addOperator(leftIndexed);

                // subtract join from left relation
                final DBSPSimpleOperator sub = new DBSPStreamAntiJoinOperator(
                        node, leftIndexed.outputPort(), joinLeftColumns.outputPort());
                this.addOperator(sub);

                // fill nulls in the right relation fields and drop index
                final DBSPVariablePath var = sub.getOutputIndexedZSetType().getKVRefType().var();
                final List<DBSPExpression> rightEmpty = new ArrayList<>();
                extendedLeft.unshuffleKeyAndDataFields(var.field(0), var.field(1), rightEmpty);
                for (DBSPType t: rightResultType.tupFields)
                    rightEmpty.add(t.withMayBeNull(true).none());

                final DBSPSimpleOperator expand = new DBSPMapOperator(
                        node, new DBSPTupleExpression(rightEmpty, false).closure(var),
                        TypeCompiler.makeZSet(resultType), sub.outputPort());
                this.addOperator(expand);
                sumInputs.add(expand.outputPort());
            }
        }

        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
            if (!hasFilter && decomposition.rightPredicates.isEmpty()) {
                final DBSPTypeTuple toSubtractKeyType = leftNonNullIndex.getOutputIndexedZSetType().getKeyTypeTuple();
                {
                    // Subtract (using an antijoin) the left from the right.
                    final DBSPSimpleOperator sub = new DBSPStreamAntiJoinOperator(
                            node, rightNonNullIndex.outputPort(), leftNonNullIndex.outputPort());
                    this.addOperator(sub);

                    // Adjust the subtraction result:
                    // fill nulls in the right relation fields and drop index
                    final DBSPVariablePath var = sub.getOutputIndexedZSetType().getKVRefType().var();
                    final DBSPTupleExpression lNulls = new DBSPTupleExpression(
                            Linq.map(leftElementType.tupFields,
                                    et -> DBSPLiteral.none(et.withMayBeNull(true)), DBSPExpression.class));
                    final List<DBSPExpression> fields = new ArrayList<>();
                    rkf.unshuffleKeyAndDataFields(var.field(0), var.field(1), fields);
                    final DBSPClosureExpression rightRow =
                            DBSPTupleExpression.flatten(lNulls, new DBSPTupleExpression(fields, false))
                                    .pointwiseCast(resultType)
                                    .closure(var);
                    final DBSPSimpleOperator expand = new DBSPMapOperator(
                            node, rightRow, TypeCompiler.makeZSet(resultType), sub.outputPort());
                    this.addOperator(expand);
                    sumInputs.add(expand.outputPort());
                }

                final DBSPType rightKeyType = rkf.keyType(rightElementType);
                final DBSPTypeTuple antiJoinKeyType = TypeCompiler.reduceType(node, toSubtractKeyType, rightKeyType, "", true)
                        .to(DBSPTypeTuple.class);

                if (!toSubtractKeyType.sameType(antiJoinKeyType)) {
                    // Since the rightNonNullIndex does not contain some elements in right,
                    // so we need add them separately to the result sumInputs.
                    // remainderRight is the complement of rightNonNullIndex
                    final DBSPSimpleOperator remainderRight = this.filterNonNullFields(conditionNode, join,
                            Linq.map(Linq.where(decomposition.comparisons, JoinConditionAnalyzer.EqualityTest::nonNull),
                                    JoinConditionAnalyzer.EqualityTest::rightColumn), right, true);

                    final DBSPVariablePath r = rightElementType.ref().var();
                    final DBSPClosureExpression toRightKey = new DBSPRawTupleExpression(
                            rkf.keyFields(r.deref(), rkf.keyType(rightElementType)),
                            rkf.nonKeyFields(r.deref(), rightResultType))
                            .closure(r);
                    final DBSPSimpleOperator remainderIndexed = new DBSPMapIndexOperator(
                            node, toRightKey, remainderRight.outputPort());
                    this.addOperator(remainderIndexed);

                    final DBSPVariablePath var = remainderIndexed.getOutputIndexedZSetType().getKVRefType().var();
                    final DBSPTupleExpression lNulls = new DBSPTupleExpression(
                            Linq.map(leftElementType.tupFields,
                                    et -> DBSPLiteral.none(et.withMayBeNull(true)), DBSPExpression.class));
                    final List<DBSPExpression> fields = new ArrayList<>();
                    rkf.unshuffleKeyAndDataFields(var.field(0), var.field(1), fields);
                    final DBSPClosureExpression rightRow =
                            DBSPTupleExpression.flatten(
                                    lNulls, new DBSPTupleExpression(fields, false))
                                    .pointwiseCast(resultType)
                                    .closure(var);
                    final DBSPSimpleOperator expand = new DBSPMapOperator(
                            node, rightRow, TypeCompiler.makeZSet(resultType), remainderIndexed.outputPort());
                    this.addOperator(expand);
                    sumInputs.add(expand.outputPort());
                }
            } else {
                // Index the input of the join as follows:
                // - key = (join key fields, condition fields part of right)
                // - value = the remaining right columns
                final KeyFields extendedRight = new KeyFields(rkf);
                if (conditionFields != null) {
                    FieldUseMap rightConditionFields = conditionFields.deref().slice(leftColumns, totalColumns);
                    extendedRight.addAllUsed(rightConditionFields);
                }
                extendedRight.addAllUsed(leftPulledFields);
                extendedRight.addAllUsed(rightPulledFields);
                final DBSPTypeTuple rightSubKeyType = extendedRight.keyType(rightResultType);

                // The following loop is an adapted version of extendedRight.keyFields
                final DBSPVariablePath joinRightVar = joinVar.getType().var();
                final List<DBSPExpression> rightKeyFields = new ArrayList<>();
                for (int i = 0; i < extendedRight.size(); i++) {
                    int index = extendedRight.keyFields.get(i);
                    rightKeyFields.add(joinRightVar.deref().field(leftColumns + index)
                            .applyCloneIfNeeded()
                            .cast(node, rightResultType.getFieldType(index), false));
                }

                final DBSPRawTupleExpression projectJoinRight = new DBSPRawTupleExpression(
                        new DBSPTupleExpression(rightKeyFields, false),
                        new DBSPTupleExpression());

                final DBSPClosureExpression toRightColumns = projectJoinRight.closure(joinRightVar);
                final DBSPSimpleOperator joinRightColumns = new DBSPMapIndexOperator(
                        node, toRightColumns, inner.outputPort());
                this.addOperator(joinRightColumns);

                // Index the right collection in the same way
                final DBSPVariablePath r1 = rightElementType.ref().var();
                final DBSPExpression projectRight = new DBSPRawTupleExpression(
                        extendedRight.keyFields(r1.deref(), rightSubKeyType),
                        extendedRight.nonKeyFields(r1.deref(), rightResultType));

                final DBSPSimpleOperator rightIndexed = new DBSPMapIndexOperator(
                        node, projectRight.closure(r1), right.outputPort());
                this.addOperator(rightIndexed);

                // subtract join from right relation
                final DBSPSimpleOperator sub = new DBSPStreamAntiJoinOperator(
                        node, rightIndexed.outputPort(), joinRightColumns.outputPort());
                this.addOperator(sub);

                // fill nulls in the left relation fields and drop index
                final DBSPVariablePath var = sub.getOutputIndexedZSetType().getKVRefType().var();
                final List<DBSPExpression> leftEmpty = new ArrayList<>();
                for (DBSPType t: leftResultType.tupFields)
                    leftEmpty.add(t.withMayBeNull(true).none());
                extendedRight.unshuffleKeyAndDataFields(var.field(0), var.field(1), leftEmpty);

                final DBSPSimpleOperator expand = new DBSPMapOperator(
                        node, new DBSPTupleExpression(leftEmpty, false).closure(var),
                        TypeCompiler.makeZSet(resultType), sub.outputPort());
                this.addOperator(expand);
                sumInputs.add(expand.outputPort());
            }
        }
        final DBSPSumOperator result = new DBSPSumOperator(node.getFinal(), sumInputs);
        Utilities.enforce(resultType.sameType(result.getOutputZSetElementType()));
        this.assignOperator(join, result);
    }

    private void visitAsofJoin(LogicalAsofJoin join) {
        // This shares a lot of code with the LogicalJoin
        CalciteObject conditionNode = CalciteObject.create(join, join.getCondition());
        IntermediateRel node = CalciteObject.create(join, conditionNode.getPositionRange());
        JoinRelType joinType = join.getJoinType();
        boolean isLeft = joinType == JoinRelType.LEFT_ASOF;
        if (!isLeft)
            throw new UnimplementedException("Currently only LEFT ASOF joins are supported.", 2212, node);

        DBSPTypeTuple resultType = this.convertType(node.getPositionRange(), join.getRowType(), false).to(DBSPTypeTuple.class);
        if (join.getInputs().size() != 2)
            throw new InternalCompilerError("Unexpected join with " + join.getInputs().size() + " inputs", node);
        DBSPSimpleOperator left = this.getInputAs(join.getInput(0), true);
        DBSPSimpleOperator right = this.getInputAs(join.getInput(1), true);
        DBSPTypeTuple leftElementType = left.getOutputZSetElementType()
                .to(DBSPTypeTuple.class);
        int leftSize = leftElementType.size();

        JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(
                leftElementType.to(DBSPTypeTuple.class).size(), this.compiler.getTypeCompiler());
        JoinConditionAnalyzer.ConditionDecomposition decomposition = analyzer.analyze(join, join.getCondition());
        Utilities.enforce(decomposition.rightPredicates.isEmpty());
        Utilities.enforce(decomposition.leftPredicates.isEmpty());
        @Nullable
        RexNode leftOver = decomposition.getLeftOver();
        Utilities.enforce(leftOver == null);

        int leftTsIndex;  // Index of "timestamp" column in left input
        int rightTsIndex;
        SqlKind comparison;  // Comparison operation, with left table column always on the left
        boolean needsLeftCast = false;
        boolean needsRightCast = false;

        {
            // Analyze match condition.  We know it's always a simple comparison between
            // two columns (enforced by the Validator).
            RexNode matchCondition = join.getMatchCondition();
            Utilities.enforce(matchCondition instanceof RexCall);
            RexCall call = (RexCall) matchCondition;
            comparison = call.getOperator().getKind();
            Utilities.enforce(SqlKind.ORDER_COMPARISON.contains(comparison));
            List<RexNode> operands = call.getOperands();
            Utilities.enforce(operands.size() == 2);
            RexNode leftCompared = operands.get(0);
            RexNode rightCompared = operands.get(1);
            RexNode lc = leftCompared;
            RexNode rc = rightCompared;

            if (leftCompared instanceof RexCall lrc) {
                Utilities.enforce(lrc.getKind() == SqlKind.CAST);
                needsLeftCast = true;
                lc = lrc.getOperands().get(0);
            }

            if (rightCompared instanceof RexCall rrc) {
                Utilities.enforce(rrc.getKind() == SqlKind.CAST);
                needsRightCast = true;
                rc = rrc.getOperands().get(0);
            }
            Utilities.enforce(lc instanceof RexInputRef);
            Utilities.enforce(rc instanceof RexInputRef);
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
                left = new DBSPMapOperator(node, addCast.closure(l), left.outputPort());
                this.addOperator(left);
                leftTsIndex = leftElementType.size();
                leftElementType = left.getOutputZSetElementType().to(DBSPTypeTuple.class);
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
                right = new DBSPMapOperator(node, addCast.closure(r), right.outputPort());
                this.addOperator(right);
                rightTsIndex = rightElementType.size();
            }
        }

        // Throw away all rows in the right collection that have a NULL in a join key column
        // or in the timestamp column.  They can never surface in the join output.
        List<JoinConditionAnalyzer.EqualityTest> comparisons = decomposition.comparisons;
        List<Integer> rightFilteredColumns = Linq.map(comparisons, JoinConditionAnalyzer.EqualityTest::rightColumn);
        rightFilteredColumns.add(rightTsIndex);
        DBSPSimpleOperator filteredRight = this.filterNonNullFields(conditionNode, join, rightFilteredColumns, right, false);
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
                    c -> l.deref().field(c.leftColumn()).applyCloneIfNeeded()
                            .cast(c.node(), c.commonType(), false));
            List<DBSPExpression> rightKeyFields = Linq.map(
                    comparisons,
                    c -> r.deref().field(c.rightColumn()).applyCloneIfNeeded()
                            .cast(c.node(), c.commonType(), false));
            DBSPExpression leftKey = new DBSPTupleExpression(node, leftKeyFields);
            keyType = leftKey.getType().to(DBSPTypeTuple.class);
            DBSPExpression rightKey = new DBSPTupleExpression(node, rightKeyFields);

            // Index left input
            DBSPTupleExpression tuple = DBSPTupleExpression.flatten(l.deref());
            DBSPClosureExpression toLeftKey =
                    new DBSPRawTupleExpression(leftKey, tuple)
                    .closure(l);
            leftIndex = new DBSPMapIndexOperator(
                    node, toLeftKey,
                    makeIndexedZSet(leftKey.getType(), tuple.getType()), false, left.outputPort());
            this.addOperator(leftIndex);

            // Index right input
            tuple = DBSPTupleExpression.flatten(r.deref());
            DBSPClosureExpression toRightKey = new DBSPRawTupleExpression(rightKey, tuple)
                    .closure(r);
            rightIndex = new DBSPMapIndexOperator(
                    node, toRightKey,
                    makeIndexedZSet(rightKey.getType(), tuple.getType()),
                    false, filteredRight.outputPort());
            this.addOperator(rightIndex);

            // ASOF joins are only incremental, so need to differentiate the inputs
            rightIndex = new DBSPDifferentiateOperator(node, rightIndex.outputPort());
            this.addOperator(rightIndex);
            leftIndex = new DBSPDifferentiateOperator(node, leftIndex.outputPort());
            this.addOperator(leftIndex);
        }

        DBSPVariablePath leftVar = leftElementType.ref().var();
        DBSPExpression leftTS = leftVar.deref().field(leftTsIndex);
        CalciteObject matchNode = CalciteObject.create(join, join.getMatchCondition());
        DBSPType leftTSType = leftTS.getType();
        if (leftTSType.is(DBSPTypeTupleBase.class) || leftTSType.is(DBSPTypeStruct.class))
            throw new UnimplementedException("Join on struct types", 3398, matchNode);

        // Currently not used
        DBSPComparatorExpression comparator = new DBSPNoComparatorExpression(matchNode, leftTSType);
        boolean ascending = comparison == SqlKind.GREATER_THAN || comparison == SqlKind.GREATER_THAN_OR_EQUAL;
        if (comparison != SqlKind.GREATER_THAN_OR_EQUAL) {
            // Not yet supported by DBSP
            throw new UnimplementedException(
                    "Currently the only MATCH_CONDITION comparison supported by ASOF joins is '>='", 2212, node);
        }
        comparator = new DBSPDirectComparatorExpression(matchNode, comparator, ascending);

        DBSPVariablePath k = keyType.ref().var(conditionNode);
        DBSPVariablePath l0 = leftElementType.ref().var();
        DBSPType rightArgumentType = rightElementType;
        //noinspection ConstantValue
        if (isLeft)
            rightArgumentType = new DBSPTypeTuple(
                    Linq.map(rightElementType.tupFields, t -> t.withMayBeNull(true), DBSPType.class));

        DBSPVariablePath r0 = rightArgumentType.ref().var();
        List<DBSPExpression> lrFields = new ArrayList<>();
        // Don't forget to drop the added field
        for (int i = 0; i < leftElementType.size() - (needsLeftCast ? 1 : 0); i++)
            lrFields.add(l0.deref().field(i).applyCloneIfNeeded());
        for (int i = 0; i < rightElementType.size() - (needsRightCast ? 1 : 0); i++)
            lrFields.add(r0.deref().field(i).applyCloneIfNeeded());
        DBSPTupleExpression lr = new DBSPTupleExpression(lrFields, false);
        DBSPClosureExpression makeTuple = lr.closure(k, l0, r0);
        DBSPSimpleOperator result = new DBSPAsofJoinOperator(node, TypeCompiler.makeZSet(resultType),
                makeTuple, leftTsIndex, rightTsIndex, comparator,
                left.isMultiset || right.isMultiset, isLeft,
                leftIndex.outputPort(), rightIndex.outputPort());
        this.addOperator(result);

        result = new DBSPIntegrateOperator(node.getFinal(), result.outputPort());
        Utilities.enforce(resultType.sameType(result.getOutputZSetElementType()));
        this.assignOperator(join, Objects.requireNonNull(result));
    }

    private void visitCollect(Collect collect) {
        IntermediateRel node = CalciteObject.create(collect);
        DBSPTypeTuple type = this.convertType(node.getPositionRange(), collect.getRowType(), false).to(DBSPTypeTuple.class);
        Utilities.enforce(collect.getInputs().size() == 1);
        Utilities.enforce(type.size() == 1);
        RelNode input = collect.getInput(0);
        DBSPTypeTuple inputRowType = this.convertType(node.getPositionRange(), input.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPSimpleOperator opInput = this.getInputAs(input, true);
        DBSPVariablePath row = inputRowType.ref().var();
        IAggregate agg;

        // Index by the empty tuple
        DBSPExpression indexingFunction = new DBSPRawTupleExpression(
                new DBSPTupleExpression(),
                        new DBSPTupleExpression(DBSPTypeTuple.flatten(row.deref()), false));
        DBSPSimpleOperator index = new DBSPMapIndexOperator(node, indexingFunction.closure(row),
                new DBSPTypeIndexedZSet(node, DBSPTypeTuple.EMPTY, inputRowType), opInput.outputPort());
        this.addOperator(index);

        switch (collect.getCollectionType()) {
            case ARRAY: {
                // specialized version of ARRAY_AGG
                boolean flatten = inputRowType.size() == 1;
                Utilities.enforce(type.getFieldType(0).is(DBSPTypeArray.class));
                DBSPTypeArray arrayType = type.getFieldType(0).to(DBSPTypeArray.class);
                DBSPType elementType = arrayType.getElementType();
                Utilities.enforce(elementType.sameType(flatten ? inputRowType.getFieldType(0) : inputRowType));
                DBSPType accumulatorType = arrayType.innerType();

                row = inputRowType.ref().var();
                DBSPExpression empty = DBSPArrayExpression.emptyWithElementType(elementType, type.mayBeNull);
                DBSPExpression zero = new DBSPApplyExpression("vec!", accumulatorType);
                DBSPVariablePath accumulator = accumulatorType.var();
                String functionName;

                functionName = "array_agg" + arrayType.nullableSuffix();
                DBSPExpression increment = new DBSPApplyExpression(
                        node, functionName, DBSPTypeVoid.INSTANCE,
                        accumulator.borrow(true),
                        ExpressionCompiler.expandTuple(
                                node, flatten ? row.deref().field(0) : row.deref().applyCloneIfNeeded()),
                        this.compiler.weightVar,
                        new DBSPBoolLiteral(false),
                        new DBSPBoolLiteral(true));
                DBSPTypeUser semigroup = new DBSPTypeUser(
                        node, SEMIGROUP, "ConcatSemigroup", false, accumulatorType);
                DBSPVariablePath p = accumulatorType.var();
                String convertName = "to_array";
                if (arrayType.mayBeNull)
                    convertName += "N";
                DBSPClosureExpression post = new DBSPApplyExpression(convertName, arrayType, p).closure(p);
                agg = new NonLinearAggregate(
                        node, zero, increment.closure(accumulator, row, this.compiler.weightVar), post,
                        empty, semigroup);
                break;
            }
            case MAP: {
                Utilities.enforce(type.getFieldType(0).is(DBSPTypeMap.class));
                DBSPTypeMap mapType = type.getFieldType(0).to(DBSPTypeMap.class);
                DBSPType keyType = mapType.getKeyType();
                DBSPType valueType = mapType.getValueType();
                Utilities.enforce(inputRowType.size() == 2);
                Utilities.enforce(keyType.sameType(inputRowType.getFieldType(0)));
                Utilities.enforce(valueType.sameType(inputRowType.getFieldType(1)));
                DBSPTypeUser accumulatorType = mapType.innerType();

                row = inputRowType.ref().var();
                DBSPExpression zero = accumulatorType.constructor("new");
                // Accumulator does not have type Map, which is Arc<BTreeMap>, but just BTreeMap
                DBSPVariablePath accumulator = accumulatorType.var();
                String functionName;
                functionName = "map_agg" + mapType.nullableSuffix();
                DBSPExpression increment = new DBSPApplyExpression(node, functionName, DBSPTypeVoid.INSTANCE,
                        accumulator.borrow(true),
                        ExpressionCompiler.expandTuple(node, row.deref()),
                        this.compiler.weightVar);
                DBSPTypeUser semigroup = new DBSPTypeUser(
                        node, SEMIGROUP, "ConcatSemigroup", false, accumulatorType);
                DBSPVariablePath p = accumulatorType.var();
                String convertName = "to_map";
                if (mapType.mayBeNull)
                    convertName += "N";
                DBSPClosureExpression post = new DBSPApplyExpression(convertName, mapType, p).closure(p);
                agg = new NonLinearAggregate(
                        node, zero, increment.closure(accumulator, row, this.compiler.weightVar), post,
                        new DBSPApplyMethodExpression(convertName, mapType, zero), semigroup);
                break;
            }
            default:
                throw new UnimplementedException("Aggregation to " + collect.getCollectionType() +
                        " not yet implemented", node);
        }
        DBSPAggregateList aggregate = new DBSPAggregateList(node, row, Linq.list(agg));
        DBSPSimpleOperator aggregateOperator = new DBSPStreamAggregateOperator(
                node, new DBSPTypeIndexedZSet(node, DBSPTypeTuple.EMPTY, type), null, aggregate, index.outputPort());
        this.addOperator(aggregateOperator);

        DBSPSimpleOperator deindex = new DBSPDeindexOperator(node.getFinal(), node, aggregateOperator.outputPort());
        Utilities.enforce(type.sameType(deindex.getOutputZSetElementType()));
        this.assignOperator(collect, deindex);
    }

    @Nullable
    ModifyTableTranslation modifyTableTranslation;

    /**
     * Visit a LogicalValue: a SQL literal, as produced by a VALUES expression.
     * This can be invoked by a DDM statement, or by a SQL query that computes a constant result.
     */
    void visitLogicalValues(LogicalValues values) {
        IntermediateRel node = CalciteObject.create(values);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(values, null, this.compiler);
        DBSPTypeTuple sourceType = this.convertType(node.getPositionRange(), values.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPTypeTuple resultType;
        if (this.modifyTableTranslation != null) {
            resultType = this.modifyTableTranslation.getResultType();
            if (sourceType.size() != resultType.size())
                throw new InternalCompilerError("Expected a tuple with " + resultType.size() +
                        " values but got " + values, node);
        } else {
            resultType = sourceType;
        }

        DBSPZSetExpression result = DBSPZSetExpression.emptyWithElementType(resultType);
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
                    DBSPExpression cast = expr.cast(expr.getNode(), resultFieldType, false);
                    expressions.add(cast);
                } else {
                    expressions.add(expr);
                }
                i++;
            }
            DBSPTupleExpression expression = new DBSPTupleExpression(node, expressions);
            result.append(expression);
        }

        if (this.modifyTableTranslation != null) {
            this.modifyTableTranslation.setResult(result);
        } else {
            // We currently don't have a reliable way to check whether there are duplicates
            // in the Z-set, so we assume it is a multiset
            DBSPSimpleOperator constant = new DBSPConstantOperator(node.getFinal(), result, true);
            this.assignOperator(values, constant);
        }
    }

    void visitIntersect(LogicalIntersect intersect) {
        var node = CalciteObject.create(intersect);
        // Intersect is a special case of join.
        List<RelNode> inputs = intersect.getInputs();
        RelNode input = intersect.getInput(0);
        DBSPSimpleOperator previous = this.getInputAs(input, false);
        this.checkPermutation(node, inputs, "INTERSECT");

        if (inputs.isEmpty())
            throw new UnsupportedException(node);
        if (inputs.size() == 1) {
            Utilities.putNew(this.nodeOperator, intersect, previous);
            return;
        }

        DBSPType inputRowType = this.convertType(node.getPositionRange(), input.getRowType(), false);
        DBSPTypeTuple resultType = this.convertType(node.getPositionRange(), intersect.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPVariablePath t = inputRowType.ref().var();
        DBSPClosureExpression entireKey =
                new DBSPRawTupleExpression(
                        DBSPTupleExpression.flatten(t.deref()),
                        new DBSPTupleExpression()).closure(t);
        DBSPVariablePath l = DBSPTypeTuple.EMPTY.ref().var();
        DBSPVariablePath r = DBSPTypeTuple.EMPTY.ref().var();
        DBSPVariablePath k = inputRowType.ref().var();

        DBSPClosureExpression closure = DBSPTupleExpression.flatten(k.deref()).closure(k, l, r);
        for (int i = 1; i < inputs.size(); i++) {
            DBSPSimpleOperator previousIndex = new DBSPMapIndexOperator(
                    node, entireKey,
                    makeIndexedZSet(inputRowType, DBSPTypeTuple.EMPTY),
                    previous.outputPort());
            this.addOperator(previousIndex);
            DBSPSimpleOperator inputI = this.getInputAs(intersect.getInput(i), false);
            DBSPSimpleOperator index = new DBSPMapIndexOperator(
                    node, entireKey.to(DBSPClosureExpression.class),
                    makeIndexedZSet(inputRowType, DBSPTypeTuple.EMPTY),
                    inputI.outputPort());
            this.addOperator(index);
            previous = new DBSPStreamJoinOperator(node, TypeCompiler.makeZSet(resultType),
                    closure, false, previousIndex.outputPort(), index.outputPort());
            this.addOperator(previous);
        }
        Utilities.enforce(resultType.sameType(previous.getOutputZSetElementType()));
        Utilities.putNew(this.nodeOperator, intersect, previous);
    }

    /** If this is not null, the parent LogicalFilter should use this implementation
     * instead of generating code. */
    @Nullable
    DBSPSimpleOperator filterImplementation = null;

    /** Index the data according to the keys specified by a window operator */
    public DBSPSimpleOperator indexWindow(Window window, Window.Group group) {
        var node = CalciteObject.create(window);
        // This code duplicates code from the SortNode.
        RelNode input = window.getInput();
        DBSPSimpleOperator opInput = this.getOperator(input);
        DBSPType inputRowType = this.convertType(node.getPositionRange(), input.getRowType(), false);
        DBSPVariablePath t = inputRowType.ref().var();
        DBSPExpression[] fields = new DBSPExpression[group.keys.cardinality()];
        int ix = 0;
        for (int field : group.keys.toList()) {
            fields[ix++] = t.deref().field(field).applyCloneIfNeeded();
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

    public static DBSPComparatorExpression generateComparator(
            CalciteObject node, List<RelFieldCollation> collations, DBSPType comparedFields, boolean reverse) {

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
            if (reverse)
                ascending = !ascending;
            comparator = comparator.field(field, ascending,
                    collation.nullDirection == RelFieldCollation.NullDirection.FIRST);
        }
        return comparator;
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
        } else {
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
                return -1;
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

    /** Decompose a window into a list of GroupAndAggregates that can be each implemented
     * by a separate DBSP operator. */
    List<WindowAggregates> splitWindow(LogicalWindow window, int windowFieldIndex) {
        List<WindowAggregates> result = new ArrayList<>();
        for (Window.Group group: window.groups) {
            List<AggregateCall> calls = group.getAggregateCalls(window);
            WindowAggregates previous = null;
            // Must keep call in the same order as in the original list,
            // because the next operator also expects them in the same order.
            for (AggregateCall call: calls) {
                if (previous == null) {
                    previous = WindowAggregates.newGroup(this, window, group, windowFieldIndex, call);
                } else if (previous.isCompatible(call)) {
                    previous.addAggregate(call);
                } else {
                    result.add(previous);
                    previous = WindowAggregates.newGroup(this, window, group, windowFieldIndex, call);
                }
                windowFieldIndex++;
            }
            result.add(previous);
        }
        return result;
    }

    /** Decomposition of a window condition for a TopK window.
     * This represents a condition of the form rank <= limit AND ...
     *
     * @param limit     A limit that is compared with the rank.
     * @param remaining The remaining condition.
     */
    record WindowCondition(int limit, @Nullable RexNode remaining) {}

    @Nullable
    WindowCondition findTopKCondition(RexNode expression, int aggregationArgumentIndex) {
        if (expression instanceof RexCall call) {
            if (call.op.kind == SqlKind.AND) {
                List<RexNode> conjuncts = new ArrayList<>(call.getOperands());
                for (int i = 0; i < conjuncts.size(); i++) {
                    RexNode node = conjuncts.get(i);
                    if (node instanceof RexCall) {
                        int limit = this.isLimit((RexCall) node, aggregationArgumentIndex);
                        if (limit >= 0) {
                            //noinspection SuspiciousListRemoveInLoop
                            conjuncts.remove(i);
                            if (conjuncts.size() == 1)
                                return new WindowCondition(limit, conjuncts.get(0));
                            else {
                                RexNode remaining = call.clone(call.type, conjuncts);
                                return new WindowCondition(limit, remaining);
                            }
                        }
                    }
                }
            } else {
                int limit = this.isLimit(call, aggregationArgumentIndex);
                if (limit >= 0)
                    return new WindowCondition(limit, null);
            }
        }
        return null;
    }

    void visitWindow(LogicalWindow window) {
        IntermediateRel node = CalciteRelNode.create(window);
        DBSPSimpleOperator input = this.getInputAs(window.getInput(0), true);
        DBSPTypeTuple inputRowType = this.convertType(node.getPositionRange(),
                window.getInput().getRowType(), false).to(DBSPTypeTuple.class);
        int windowFieldIndex = inputRowType.size();
        DBSPType resultType = this.convertType(node.getPositionRange(), window.getRowType(), false);

        List<WindowAggregates> toProcess = this.splitWindow(window, windowFieldIndex);

        // TopK aggregates have to be done later
        List<RankAggregate> topKAggregates = new ArrayList<>();

        // We have to process multiple Groups, and each group has multiple aggregates.
        List<Integer> shuffle = new ArrayList<>();
        List<Integer> later = new ArrayList<>();
        DBSPSimpleOperator lastOperator = input;
        int index = 0;
        int elementIndex = 0;

        for (int i = 0; i < inputRowType.size(); i++)
            shuffle.add(i);
        for (WindowAggregates ga: toProcess) {
            if (ga.is(RankAggregate.class)) {
                if (!topKAggregates.isEmpty()) {
                    throw new UnimplementedException(ga.to(RankAggregate.class).call.getAggregation() +
                            " Only one TopK pattern supported in a WINDOW query",
                            ga.getNode());
                }
                topKAggregates.add(ga.to(RankAggregate.class));
                later.add(inputRowType.size() + elementIndex);
                elementIndex++;
                continue;
            }
            if (lastOperator != input)
                this.addOperator(lastOperator);
            lastOperator = ga.implement(input, lastOperator, index == toProcess.size() - 1);
            shuffle.add(inputRowType.size() + elementIndex);
            elementIndex++;
            index++;
        }

        shuffle.addAll(later);
        // Implement the TopK aggregates
        if (!topKAggregates.isEmpty()) {
            // Special handling for the following pattern:
            // LogicalFilter(condition=[<=(RANK_COLUMN, LIMIT)])
            // LogicalWindow(window#0=[window(partition ... order by ... aggs [RANK()])])
            // This is compiled into a TopK over each group.
            // There is no way this can be expressed otherwise in SQL or using RelNode.
            // This works for RANK, DENSE_RANK, and ROW_NUMBER.
            // There can be multiple TopK patterns; they are all applied after the previous aggregates.
            RankAggregate first = topKAggregates.get(0);
            if (this.ancestors.isEmpty())
                throw new UnimplementedException(first.call.getAggregation() + " only supported in a TopK pattern",
                        first.getNode());
            RelNode parent = this.getParent();
            if (!(parent instanceof LogicalFilter filter))
                throw new UnimplementedException(first.call.getAggregation() + " only supported in a TopK pattern",
                        first.getNode());
            RexNode condition = filter.getCondition();

            for (RankAggregate aggregate: topKAggregates) {
                if (condition == null)
                    throw new UnimplementedException(first.call.getAggregation() + " only supported in a TopK pattern",
                            first.getNode());
                WindowCondition winCondition = this.findTopKCondition(condition, aggregate.windowFieldIndex);
                if (winCondition == null)
                    throw new UnimplementedException(first.call.getAggregation() + " only supported in a TopK pattern",
                            first.getNode());
                condition = winCondition.remaining;
                if (lastOperator != input)
                    this.addOperator(lastOperator);
                lastOperator = aggregate.implement(winCondition.limit, input, lastOperator,  index == toProcess.size() - 1);
                index++;
            }

            // Synthesize left-over condition from parent filter
            if (condition != null) {
                this.addOperator(lastOperator);
                DBSPVariablePath t = lastOperator.getOutputZSetElementType().ref().var();
                ExpressionCompiler expressionCompiler = new ExpressionCompiler(window, t, this.compiler);
                DBSPExpression cond = expressionCompiler.compile(condition).wrapBoolIfNeeded();
                DBSPClosureExpression func = new DBSPClosureExpression(
                        CalciteObject.create(window, condition), cond, t.asParameter());
                lastOperator = new DBSPFilterOperator(node.getFinal(), func, lastOperator.outputPort());
            }
        }

        // Permute the results to put the TopK results in their right place
        Shuffle s = new ExplicitShuffle(shuffle.size(), shuffle);
        if (!s.isIdentityPermutation()) {
            DBSPTypeTuple last = lastOperator.getOutputZSetElementType().to(DBSPTypeTuple.class);
            Utilities.enforce(s.inputLength() == last.size());
            DBSPVariablePath vReorder = last.ref().var();
            Shuffle inverse = s.invert();
            DBSPBaseTupleExpression value =
                    DBSPTupleExpression.flatten(vReorder.deref())
                            .shuffle(inverse);
            DBSPClosureExpression closure = value.closure(vReorder);
            this.addOperator(lastOperator);
            lastOperator = new DBSPMapOperator(node.getFinal(), closure, lastOperator.outputPort());
        }

        // Some aggregate functions always return nullable results (e.g., Max), but sometimes we know that the type
        // cannot be nullable (e.g. window aggregate of non-nullable values); adjust now:
        DBSPTypeTuple tuple = resultType.to(DBSPTypeTuple.class);
        if (!lastOperator.getOutputZSetElementType().sameType(resultType)) {
            Utilities.enforce(
                    lastOperator.getOutputZSetElementType().to(DBSPTypeTuple.class).size() == tuple.size(),
                    () -> "Window aggregate type size does not match expected size");
            DBSPVariablePath var = lastOperator.getOutputZSetElementType().ref().var();
            List<DBSPExpression> fields = new ArrayList<>();
            for (int i = 0; i < tuple.size(); i++) {
                fields.add(var.deref().field(i).applyCloneIfNeeded().nullabilityCast(tuple.tupFields[i], false));
            }
            DBSPClosureExpression convert = new DBSPTupleExpression(fields, false).closure(var);
            this.addOperator(lastOperator);
            lastOperator = new DBSPMapOperator(node.getFinal(), convert, lastOperator.outputPort());
        }
        if (!topKAggregates.isEmpty()) {
            if (this.filterImplementation != null)
                throw new InternalCompilerError("Unexpected filter implementation ",
                        CalciteObject.create(window));
            this.filterImplementation = lastOperator;
        }
        this.assignOperator(window, lastOperator);
    }

    private RelNode getParent() {
        return Utilities.last(this.ancestors);
    }

    void warnNoSort(CalciteObject node) {
        boolean isFinal = this.ancestors.isEmpty();
        String viewName = "";
        this.compiler.reportWarning(node.getPositionRange(), "ORDER BY is ignored",
                "ORDER BY clause " + viewName + "is currently ignored\n" +
                        "(the result will contain the correct data, but the data is not ordered)" +
                        (isFinal ? "" :
                                "\nThis is tracked by issue https://github.com/feldera/feldera/issues/2833"));
    }

    void visitSort(LogicalSort sort) {
        IntermediateRel node = CalciteObject.create(sort);
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

        DBSPType inputRowType = this.convertType(node.getPositionRange(), input.getRowType(), false);
        DBSPVariablePath t = inputRowType.ref().var();
        DBSPClosureExpression emptyGroupKeys =
                new DBSPRawTupleExpression(
                        new DBSPTupleExpression(),
                        DBSPTupleExpression.flatten(t.deref())).closure(t);
        DBSPSimpleOperator index = new DBSPMapIndexOperator(
                node, emptyGroupKeys,
                makeIndexedZSet(DBSPTypeTuple.EMPTY, inputRowType),
                opInput.outputPort());
        this.addOperator(index);

        // Generate comparison function for sorting the vector
        DBSPComparatorExpression comparator = generateComparator(
                node, sort.getCollation().getFieldCollations(), inputRowType, false);
        if (sort.fetch != null) {
            // TopK operator.
            // Since TopK is always incremental we have to wrap it into a D-I pair
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, index.outputPort());
            this.addOperator(diff);
            DBSPEqualityComparatorExpression eq = new DBSPEqualityComparatorExpression(node, comparator);

            // Output producer is (index, row) -> row
            DBSPVariablePath left = DBSPTypeInteger.getType(node, INT64, false).var();
            DBSPVariablePath right = inputRowType.ref().var();
            List<DBSPExpression> flattened = DBSPTypeTupleBase.flatten(right.deref());
            DBSPTupleExpression tuple = new DBSPTupleExpression(flattened, false);
            DBSPClosureExpression outputProducer = tuple.closure(left, right);

            limit = limit.cast(limit.getNode(), DBSPTypeUSize.create(limit.getType().mayBeNull), false);
            DBSPIndexedTopKOperator topK = new DBSPIndexedTopKOperator(
                    node, DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER,
                    comparator, limit, eq, outputProducer, diff.outputPort());
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
                DBSPDeindexOperator deindex = new DBSPDeindexOperator(node.getFinal(), node, integral.outputPort());
                this.assignOperator(sort, deindex);
                return;
            }
            // Otherwise we have to sort again in a vector!
            // Fall through, continuing from the integral.
            index = integral;
        }
        // Global sort.  Implemented by aggregate in a single Vec<> which is then sorted.
        // Apply an aggregation function that just creates a vector.
        DBSPTypeArray arrayType = new DBSPTypeArray(inputRowType, false);
        DBSPTypeVec vecType = arrayType.innerType();
        DBSPExpression zero = vecType.emptyVector();
        DBSPVariablePath accum = vecType.ref(true).var();
        DBSPVariablePath row = inputRowType.ref().var();
        // An element with weight 'w' is pushed 'w' times into the vector
        DBSPExpression wPush = new DBSPApplyExpression(node,
                "weighted_push", DBSPTypeVoid.INSTANCE, accum,
                new DBSPTupleExpression(DBSPTypeTuple.flatten(row.deref()), false).borrow(),
                this.compiler.weightVar);
        DBSPClosureExpression push = wPush.closure(accum, row, this.compiler.weightVar);
        DBSPTypeUser semigroup = new DBSPTypeUser(node, USER, "UnimplementedSemigroup",
                false, DBSPTypeAny.getDefault());
        DBSPVariablePath var = vecType.var();
        DBSPClosureExpression post = new DBSPApplyMethodExpression("into", arrayType, var).closure(var);
        DBSPFold folder = new DBSPFold(node, semigroup, zero, push, post);
        DBSPStreamAggregateOperator agg = new DBSPStreamAggregateOperator(node,
                makeIndexedZSet(DBSPTypeTuple.EMPTY, arrayType),
                folder, null, index.outputPort());
        this.addOperator(agg);

        if (limit != null)
            limit = limit.cast(limit.getNode(), DBSPTypeUSize.INSTANCE, false);
        DBSPSortExpression sorter = new DBSPSortExpression(node, inputRowType, comparator, limit);
        DBSPSimpleOperator result = new DBSPMapOperator(
                node.getFinal(), sorter, TypeCompiler.makeZSet(arrayType), agg.outputPort());
        this.assignOperator(sort, result);
    }

    @Override
    public void visit(
            RelNode node, int ordinal,
            @Nullable RelNode parent) {
        Logger.INSTANCE.belowLevel(this, 3)
                .append("Visiting ")
                .appendSupplier(node::toString)
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
        Utilities.enforce(last == node, () -> "Corrupted stack: got " + last + " expected " + node);

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
        DBSPType type = this.convertType(metadata.node.getPositionRange(), metadata.getType(), false);
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
            if (!type.mayBeNull && defaultValue.getType().mayBeNull)
                this.compiler.reportError(defaultValue.getSourcePosition(), "Illegal value",
                        "Nullable default value assigned to non-null column " +
                                metadata.getName().singleQuote());
            defaultValue = defaultValue.cast(defaultValue.getNode(), type, false);
        }
        if (metadata.interned) {
            if (!type.is(DBSPTypeString.class))
                this.compiler.reportWarning(metadata.node.getPositionRange(), "Illegal type interned",
                        "INTERNED is only applicable to CHAR/VARCHAR types " +
                                metadata.getName().singleQuote() +
                        ", hint will be ignored");
        }
        return new InputColumnMetadata(metadata.getNode(), metadata.getName(), type,
                metadata.isPrimaryKey, lateness, watermark, defaultValue, metadata.defaultValuePosition, metadata.interned);
    }

    private DBSPNode compileCreateView(CreateViewStatement view) {
        RelNode rel = view.getRel();
        this.go(rel);
        DBSPSimpleOperator op = this.getOperator(rel);

        // The operator above may not contain all columns that were computed.
        RelRoot root = view.getRoot();
        DBSPTypeZSet producedType = op.getOutputZSetType();
        // The output element may be a Tuple or a Vec<Tuple> when we sort;
        DBSPType elemType = producedType.getElementType();
        DBSPTypeTupleBase tuple;
        boolean isVector = false;
        if (elemType.is(DBSPTypeArray.class)) {
            isVector = true;
            tuple = elemType.to(DBSPTypeArray.class).getElementType().to(DBSPTypeTupleBase.class);
        } else {
            tuple = elemType.to(DBSPTypeTupleBase.class);
        }
        IntermediateRel node = CalciteObject.create(root.rel, new SourcePositionRange(view.getParserPosition()));
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
                outputElementType = new DBSPTypeArray(outputElementType, false);
                DBSPVariablePath v = elemType.ref().var();
                closure = new DBSPApplyExpression("map", outputElementType, v.deref().applyClone(), closure)
                        .closure(v);
            }
            op = new DBSPMapOperator(node, closure, TypeCompiler.makeZSet(outputElementType), op.outputPort());
            this.addOperator(op);
        }

        DBSPSimpleOperator o;
        DBSPTypeStruct struct = view.getRowTypeAsStruct(this.compiler().typeCompiler)
                .rename(view.relationName);
        List<ViewColumnMetadata> columnMetadata = new ArrayList<>();
        // Synthesize the metadata for the view's columns.
        for (RelColumnMetadata meta: view.columns) {
            InputColumnMetadata colMeta = this.convertMetadata(meta);
            ViewColumnMetadata cm = new ViewColumnMetadata(meta.getNode(), view.relationName,
                        meta.getName(), colMeta.type, colMeta.lateness);
            columnMetadata.add(cm);
        }

        int emitFinalIndex = view.emitFinalColumn();
        DeclareViewStatement declare = this.recursiveViews.get(view.relationName);
        DBSPTypeStruct declaredStruct = struct;
        if (declare != null) {
            declaredStruct = declare.getRowTypeAsStruct(this.compiler().typeCompiler);
            if (!declaredStruct.sameType(struct)) {
                // Use the declared type rather than the inferred type
                DBSPTypeTuple viewType = struct.toTuple();
                DBSPTypeTuple sinkType = declaredStruct.toTuple();
                DBSPVariablePath var = viewType.ref().var();
                DBSPExpression cast = new DBSPTupleExpression(DBSPTypeTuple.flatten(var.deref()), false)
                        .pointwiseCast(sinkType);
                op = new DBSPMapOperator(node, cast.closure(var), op.outputPort());
                this.addOperator(op);
                view.replaceColumns(declare.columns);
            }
        }
        ViewMetadata meta = new ViewMetadata(view.relationName,
                columnMetadata, view.getViewKind(), emitFinalIndex,
                // The view is a system view if it's not visible
                declare != null, !view.isVisible(), view.getProperties());
        if (view.getViewKind() != SqlCreateView.ViewKind.LOCAL) {
            this.metadata.addView(view);
            // Create two operators chained, a ViewOperator and a SinkOperator.
            DBSPViewOperator vo = new DBSPViewOperator(node, view.relationName, view.getStatement(),
                    declaredStruct, meta, op.outputPort());
            this.addOperator(vo);
            o = new DBSPSinkOperator(
                    node.getFinal(), view.relationName,
                    view.getStatement(), declaredStruct, meta, vo.outputPort());
        } else {
            // We may already have a node for this output
            DBSPSimpleOperator previous = this.getCircuit().getView(view.relationName);
            if (previous != null)
                return previous;
            o = new DBSPViewOperator(node.getFinal(), view.relationName,
                    view.getStatement(), declaredStruct, meta, op.outputPort());
        }
        this.addOperator(o);
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
                modify, def, targetColumnList, this.compiler.getTypeCompiler());
        DBSPZSetExpression result;
        if (modify.rel instanceof LogicalTableScan scan) {
            // Support for INSERT INTO table (SELECT * FROM otherTable)
            RelOptTable relTbl = scan.getTable();
            Utilities.enforce(relTbl != null);
            ProgramIdentifier sourceTable = Utilities.toIdentifier(relTbl.getQualifiedName());
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
        } else if (modify.rel instanceof LogicalUnion union) {
            result = null;
            for (RelNode node: union.getInputs()) {
                if (node instanceof LogicalProject project) {
                    DBSPZSetExpression lit = this.compileConstantProject(project);
                    if (result == null)
                        result = lit;
                    else
                        result.append(lit);
                }
            }
            Utilities.enforce(result != null);
        } else {
            throw new UnimplementedException("statement of this form not supported",
                    modify.getCalciteObject());
        }
        if (!isInsert)
            result = result.negate();
        this.modifyTableTranslation = null;
        this.tableContents.addToTable(modify.tableName, result, this.compiler);
        return result;
    }

    private DBSPTypeStruct.Field makeField(List<RelDataTypeField> relFields,
                                           CalciteObject object, SqlIdentifier name,
                                           int index, SqlDataTypeSpec typeSpec) {
        DBSPType fieldType = null;
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
            fieldType = this.convertType(object.getPositionRange(), ft.getType(), true);
        }
        return new DBSPTypeStruct.Field(
                object,
                Utilities.toIdentifier(name),
                index,
                fieldType);
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
        if (ct.attributeDefs != null) {
            for (SqlNode def : ct.attributeDefs) {
                // the relFields implementation has already flattened structs,
                // so for structs we look them up again using the Sql representation
                final SqlAttributeDefinition attributeDef =
                        (SqlAttributeDefinition) def;
                final SqlDataTypeSpec typeSpec = attributeDef.dataType;
                DBSPTypeStruct.Field field = this.makeField(relFields, object, attributeDef.name, index, typeSpec);
                index++;
                fields.add(field);
            }
        } else {
            throw new UnsupportedException("User-defined types cannot be defined to be ROW types; consider" +
                    " eliminating 'ROW' from this definition",
                    CalciteObject.create(ct.name));
            /*
            This doesn't quite work, and it's probably not necessary.

            Utilities.enforce(ct.dataType != null;
            var spec = ct.dataType.getTypeNameSpec();
            if (spec instanceof SqlRowTypeNameSpec row) {
                List<SqlIdentifier> fieldNames = row.getFieldNames();
                var types = row.getFieldTypes();
                for (int i = 0; i < row.getArity(); i++) {
                    SqlIdentifier name = fieldNames.get(i);
                    SqlDataTypeSpec typeSpec = types.get(i);
                    DBSPTypeStruct.Field field = this.makeField(relFields, object, name, index, typeSpec);
                    index++;
                    fields.add(field);
                }
            }
            */
        }

        String saneName = this.compiler.generateStructName(stat.typeName, fields);
        DBSPTypeStruct struct = new DBSPTypeStruct(object, stat.typeName, saneName, fields, false);
        this.compiler.registerStruct(struct);
        DBSPItem item = new DBSPStructItem(struct, null);
        this.getCircuit().addDeclaration(new DBSPDeclaration(item));
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
                new RelAnd(), identifier, TypeCompiler.makeZSet(rowType), originalRowType,
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
                create.getCalciteObject(), identifier, TypeCompiler.makeZSet(rowType), originalRowType,
                tableMeta, tableName);
        this.addOperator(result);
        this.metadata.addTable(create);
        return result;
    }

    @Nullable
    DBSPNode compileCreateFunction(CreateFunctionStatement stat) {
        DBSPFunction function = stat.function.getImplementation(
                this.compiler.getTypeCompiler(), this.compiler);
        if (function == null)
            function = stat.function.getDeclaration(this.compiler.getTypeCompiler());
        this.getCircuit().addDeclaration(new DBSPDeclaration(new DBSPFunctionItem(function)));
        return function;
    }

    @Nullable
    public DBSPNode compileCreateIndex(CreateIndexStatement ci) {
        ProgramIdentifier viewName = ci.refersTo;
        DBSPViewOperator view = this.getCircuit().getView(viewName);
        // This can happen when the index does not refer to a view
        if (view == null)
            return null;

        DBSPTypeStruct struct = view.originalRowType.to(DBSPTypeStruct.class);
        List<Integer> keyColumnIndexes = new ArrayList<>();
        List<ViewColumnMetadata> keyColumns = new ArrayList<>();
        DBSPVariablePath var = view.getOutputZSetElementType().ref().var();

        int i = 0;
        for (ProgramIdentifier col: ci.columns) {
            DBSPTypeStruct.Field field = struct.getField(col);
            Utilities.enforce(field != null);
            DBSPTypeCode code = field.type.code;
            if (code == ARRAY || code == MAP || code == VARIANT || code == STRUCT) {
                this.compiler.reportError(new SourcePositionRange(ci.columnParserPosition(i)),
                        "Illegal type in INDEX",
                        "Cannot index on column " + col.singleQuote() + " because it has type " +
                        field.type.asSqlString());
            }
            keyColumnIndexes.add(field.index);
            keyColumns.add(view.metadata.columns.get(field.index));
            i++;
        }

        DBSPTypeStruct keyStruct = TypeCompiler.asStruct(this.compiler,
                ci.getCalciteObject(), ci.indexName, keyColumns, false);

        DBSPExpression index = new DBSPRawTupleExpression(
                new DBSPTupleExpression(Linq.map(
                        keyColumnIndexes, c -> var.deref().field(c).applyCloneIfNeeded()), false),
                DBSPTupleExpression.flatten(var.deref()));
        DBSPMapIndexOperator indexOp = new DBSPMapIndexOperator(CalciteEmptyRel.INSTANCE,
                index.closure(var), view.outputPort());
        this.addOperator(indexOp);

        DBSPSimpleOperator result = new DBSPSinkOperator(
                CalciteEmptyRel.INSTANCE, ci.indexName,
                ci.refersTo.toString(), new DBSPTypeRawTuple(keyStruct, struct),
                view.metadata, indexOp.outputPort());
        this.addOperator(result);
        return result;
    }

    @Nullable
    public DBSPNode compileCreateAggregate(CreateAggregateStatement aggregate) {
        SqlUserDefinedAggregationFunction uda = aggregate.getFunction();
        CalciteObject node = aggregate.getCalciteObject();
        String name = uda.description.name.getSimple();
        if (uda.isLinear()) {
            // Add two functions that the user needs to define to the circuit declarations.
            DBSPType accumulatorType = LinearAggregate.userAccumulatorType(node, name);
            List<DBSPParameter> parameters = Linq.map(uda.description.parameterList,
                    p -> new DBSPParameter(p.getName(),
                            this.convertType(node.getPositionRange(), p.getType(), false).withMayBeNull(false)));
            DBSPFunction mapFunction = new DBSPFunction(
                    node, LinearAggregate.userDefinedMapFunctionName(name), parameters, accumulatorType, null, Linq.list());
            this.getCircuit().addDeclaration(new DBSPDeclaration(new DBSPFunctionItem(mapFunction)));

            DBSPType resultType = this.convertType(node.getPositionRange(), uda.description.returnType, false);
            DBSPFunction postFunction = new DBSPFunction(
                    node, LinearAggregate.userDefinedPostFunctionName(name),
                    Linq.list(new DBSPParameter("accumulator", accumulatorType)), resultType.withMayBeNull(false), null, Linq.list());
            this.getCircuit().addDeclaration(new DBSPDeclaration(new DBSPFunctionItem(postFunction)));
        } else {
            throw new UnimplementedException("Non-linear user-defined aggregation functions");
        }
        return null;
    }

    @SuppressWarnings("UnusedReturnValue")
    @Nullable
    public DBSPNode compile(RelStatement statement) {
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
        } else if (statement.is(DeclareViewStatement.class)) {
            DeclareViewStatement decl = statement.to(DeclareViewStatement.class);
            return this.compileDeclareView(decl);
        } else if (statement.is(CreateIndexStatement.class)) {
            CreateIndexStatement ci = statement.to(CreateIndexStatement.class);
            return this.compileCreateIndex(ci);
        } else if (statement.is(CreateAggregateStatement.class)) {
            CreateAggregateStatement ca = statement.to(CreateAggregateStatement.class);
            return this.compileCreateAggregate(ca);
        }
        throw new UnsupportedException(statement.getCalciteObject());
    }

    private DBSPZSetExpression compileConstantProject(LogicalProject project) {
        // Specialization of the visitor's visit method for LogicalProject
        // Does not produce a DBSPOperator, but only a literal.
        CalciteObject node = CalciteObject.create(project);
        DBSPType outputElementType = this.convertType(node.getPositionRange(), project.getRowType(), false);
        DBSPTypeTuple tuple = outputElementType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(node.getPositionRange(), project.getInput().getRowType(), false);
        DBSPVariablePath row = inputType.ref().var();  // should not be used
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(project, row, this.compiler);
        DBSPZSetExpression result = DBSPZSetExpression.emptyWithElementType(outputElementType);

        List<DBSPExpression> resultColumns = new ArrayList<>();
        int index = 0;
        for (RexNode column : project.getProjects()) {
            DBSPExpression exp = expressionCompiler.compile(column);
            DBSPType expectedType = tuple.getFieldType(index);
            if (!exp.getType().sameType(expectedType)) {
                // Calcite's optimizations do not preserve types!
                exp = exp.applyCloneIfNeeded().cast(exp.getNode(), expectedType, false);
            }
            resultColumns.add(exp);
            index++;
        }

        DBSPExpression exp = new DBSPTupleExpression(node, resultColumns);
        result.append(exp);
        return result;
    }

    public TableContents getTableContents() {
        return this.tableContents;
    }

    public void clearTables() {
        this.tableContents.clear();
    }
}
