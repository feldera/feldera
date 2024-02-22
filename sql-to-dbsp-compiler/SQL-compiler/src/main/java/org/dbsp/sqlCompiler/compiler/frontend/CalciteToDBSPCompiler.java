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

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlRankFunction;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowAggregateOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.InputTableMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DropTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.HasSchema;
import org.dbsp.sqlCompiler.compiler.frontend.statements.TableModifyStatement;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.ICastable;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator.TopKNumbering.*;
import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

/**
 * The compiler is stateful: it compiles a sequence of SQL statements
 * defining tables and views.  The views must be defined in terms of
 * the previously defined tables and views.  Multiple views can be
 * compiled.  The result is a circuit which has an input for each table
 * and an output for each view.
 * The function generateOutputForNextView can be used to prevent
 * some views from generating outputs.
 */
public class CalciteToDBSPCompiler extends RelVisitor
        implements IWritesLogs, ICompilerComponent {
    // Result is deposited here
    private DBSPPartialCircuit circuit;
    // Map each compiled RelNode operator to its DBSP implementation.
    final Map<RelNode, DBSPOperator> nodeOperator;
    final TableContents tableContents;
    final CompilerOptions options;
    final DBSPCompiler compiler;
    /** Keep track of position in RelNode tree */
    final List<RelNode> ancestors;
    final ProgramMetadata metadata;

    /**
     * Create a compiler that translated from calcite to DBSP circuits.
     * @param trackTableContents  If true this compiler will track INSERT and DELETE statements.
     * @param options             Options for compilation.
     * @param compiler            Parent compiler; used to report errors.
     */
    public CalciteToDBSPCompiler(boolean trackTableContents,
                                 CompilerOptions options, DBSPCompiler compiler,
                                 ProgramMetadata metadata) {
        this.circuit = new DBSPPartialCircuit(compiler, compiler.metadata);
        this.compiler = compiler;
        this.nodeOperator = new HashMap<>();
        this.tableContents = new TableContents(compiler, trackTableContents);
        this.options = options;
        this.ancestors = new ArrayList<>();
        this.metadata = metadata;
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }

    private DBSPType convertType(RelDataType dt, boolean asStruct) {
        return this.compiler.getTypeCompiler().convertType(dt, asStruct);
    }

    private DBSPTypeZSet makeZSet(DBSPType type) {
        return TypeCompiler.makeZSet(type);
    }

    private DBSPTypeIndexedZSet makeIndexedZSet(DBSPType keyType, DBSPType valueType) {
        return TypeCompiler.makeIndexedZSet(keyType, valueType);
    }

    /** Gets the circuit produced so far and starts a new one. */
    public DBSPPartialCircuit getFinalCircuit() {
        DBSPPartialCircuit result = this.circuit;
        this.circuit = new DBSPPartialCircuit(this.compiler, this.compiler.metadata);
        return result;
    }

    /**
     * This retrieves the operator that is an input.  If the operator may
     * produce multiset results and this is not desired (asMultiset = false),
     * a distinct operator is introduced in the circuit.
     */
    private DBSPOperator getInputAs(RelNode input, boolean asMultiset) {
        DBSPOperator op = this.getOperator(input);
        if (op.isMultiset && !asMultiset) {
            op = new DBSPStreamDistinctOperator(CalciteObject.create(input), op);
            this.circuit.addOperator(op);
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

    private boolean generateOutputForNextView = true;

    /**
     * @param generate
     * If 'false' the next "create view" statements will not generate
     * an output for the circuit.  This is sticky, it has to be
     * explicitly reset.
     */
    public void generateOutputForNextView(boolean generate) {
         this.generateOutputForNextView = generate;
    }

    /**
     * Helper function for creating aggregates.
     * @param node         RelNode that generates this aggregate.
     * @param aggregates   Aggregates to implement.
     * @param groupCount   Number of groupBy variables.
     * @param inputRowType Type of input row.
     * @param resultType   Type of result produced.
     */
    public DBSPAggregate createAggregate(RelNode node,
            List<AggregateCall> aggregates, DBSPTypeTuple resultType,
            DBSPType inputRowType, int groupCount) {
        DBSPVariablePath rowVar = inputRowType.ref().var("v");
        DBSPAggregate.Implementation[] implementations = new DBSPAggregate.Implementation[aggregates.size()];
        int aggIndex = 0;

        for (AggregateCall call: aggregates) {
            DBSPType resultFieldType = resultType.getFieldType(aggIndex + groupCount);
            AggregateCompiler compiler = new AggregateCompiler(node,
                    this.getCompiler(), call, resultFieldType, rowVar);
            DBSPAggregate.Implementation implementation = compiler.compile();
            implementations[aggIndex] =  implementation;
            aggIndex++;
        }
        return new DBSPAggregate(CalciteObject.create(node), rowVar, implementations, false);
    }

    /**
     * Given a struct type find the index of the specified field.
     * Throws if no such field exists.
     */
    static int getFieldIndex(String field, RelDataType type) {
        int index = 0;
        for (RelDataTypeField rowField: type.getFieldList()) {
            if (rowField.getName().equals(field))
                return index;
            index++;
        }
        throw new InternalCompilerError("Type " + type + " has no field named " + field, CalciteObject.create(type));
    }

    public void visitCorrelate(LogicalCorrelate correlate) {
        // We decorrelate queries using Calcite's optimizer.
        // So we assume that the only correlated queries we receive
        // are unnest-type queries.  We assume that unnest queries
        // have a restricted plan of this form:
        // LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{...}])
        //    LeftSubquery
        //    Uncollect
        //      LogicalProject(COL=[$cor0.ARRAY])
        //        LogicalValues(tuples=[[{ 0 }]])
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
            throw new UnimplementedException(node);
        this.visit(correlate.getLeft(), 0, correlate);
        DBSPOperator left = this.getInputAs(correlate.getLeft(), true);
        DBSPTypeTuple leftElementType = left.getOutputZSetElementType().to(DBSPTypeTuple.class);

        RelNode correlateRight = correlate.getRight();
        if (!(correlateRight instanceof Uncollect))
            throw new UnimplementedException(node);
        Uncollect uncollect = (Uncollect) correlateRight;
        CalciteObject uncollectNode = CalciteObject.create(uncollect);
        RelNode uncollectInput = uncollect.getInput();
        if (!(uncollectInput instanceof LogicalProject))
            throw new UnimplementedException(node);
        LogicalProject project = (LogicalProject) uncollectInput;
        if (project.getProjects().size() != 1)
            throw new UnimplementedException(node);
        RexNode projection = project.getProjects().get(0);
        if (!(projection instanceof RexFieldAccess))
            throw new UnimplementedException(node);
        RexFieldAccess field = (RexFieldAccess) projection;
        RelDataType leftRowType = correlate.getLeft().getRowType();
        // The index of the field that is the array
        int arrayFieldIndex = getFieldIndex(field.getField().getName(), leftRowType);
        List<Integer> allFields = IntStream.range(0, leftElementType.size())
                .boxed()
                .collect(Collectors.toList());
        allFields.add(DBSPFlatmap.ITERATED_ELEMENT);
        DBSPType indexType = null;
        if (uncollect.withOrdinality) {
            // Index field is always last
            indexType = type.getFieldType(type.size() - 1);
            allFields.add(DBSPFlatmap.COLLECTION_INDEX);
        }
        DBSPFlatmap flatmap = new DBSPFlatmap(node, leftElementType, arrayFieldIndex,
                allFields, indexType);
        DBSPFlatMapOperator flatMap = new DBSPFlatMapOperator(uncollectNode,
                flatmap, TypeCompiler.makeZSet(type), left);
        this.assignOperator(correlate, flatMap);
    }

    public void visitUncollect(Uncollect uncollect) {
        // This represents an unnest.
        // flat_map(move |x| { x.0.into_iter().map(move |e| Tuple1::new(e)) })
        CalciteObject node = CalciteObject.create(uncollect);
        DBSPType type = this.convertType(uncollect.getRowType(), false);
        RelNode input = uncollect.getInput();
        DBSPTypeTuple inputRowType = this.convertType(input.getRowType(), false).to(DBSPTypeTuple.class);
        // We expect this to be a single-element tuple whose type is a vector.
        DBSPOperator opInput = this.getInputAs(input, true);
        DBSPType indexType = null;
        List<Integer> indexes = new ArrayList<>();
        indexes.add(DBSPFlatmap.ITERATED_ELEMENT);
        if (uncollect.withOrdinality) {
            DBSPTypeTuple pair = type.to(DBSPTypeTuple.class);
            indexType = pair.getFieldType(1);
            indexes.add(DBSPFlatmap.COLLECTION_INDEX);
        }
        DBSPFlatmap function = new DBSPFlatmap(node, inputRowType, 0,
                indexes, indexType);
        DBSPFlatMapOperator flatMap = new DBSPFlatMapOperator(node, function,
                TypeCompiler.makeZSet(type), opInput);
        this.assignOperator(uncollect, flatMap);
    }

    public void visitAggregate(LogicalAggregate aggregate) {
        CalciteObject node = CalciteObject.create(aggregate);
        DBSPType type = this.convertType(aggregate.getRowType(), false);
        DBSPTypeTuple tuple = type.to(DBSPTypeTuple.class);
        RelNode input = aggregate.getInput();
        DBSPOperator opInput = this.getInputAs(input, true);
        DBSPType inputRowType = this.convertType(input.getRowType(), false);
        List<AggregateCall> aggregates = aggregate.getAggCallList();
        DBSPVariablePath t = inputRowType.ref().var("t");

        if (!aggregates.isEmpty()) {
            if (aggregate.getGroupType() != org.apache.calcite.rel.core.Aggregate.Group.SIMPLE)
                throw new UnimplementedException(node);
            DBSPExpression[] groups = new DBSPExpression[aggregate.getGroupCount()];
            int next = 0;
            for (int index: aggregate.getGroupSet()) {
                groups[next] = t.deref().field(index).applyCloneIfNeeded();
                next++;
            }
            DBSPExpression keyExpression = new DBSPTupleExpression(groups);
            DBSPType[] aggTypes = Utilities.arraySlice(tuple.tupFields, aggregate.getGroupCount());
            DBSPTypeTuple aggType = new DBSPTypeTuple(aggTypes);

            DBSPExpression groupKeys =
                    new DBSPTupleExpression(
                            keyExpression,
                            DBSPTupleExpression.flatten(t.deref())).closure(
                    t.asParameter());
            DBSPIndexOperator index = new DBSPIndexOperator(
                    node, groupKeys,
                    this.makeIndexedZSet(keyExpression.getType(), inputRowType), false, opInput);
            this.circuit.addOperator(index);
            DBSPType groupType = keyExpression.getType();
            DBSPAggregate fold = this.createAggregate(aggregate, aggregates, tuple, inputRowType, aggregate.getGroupCount());
            // The aggregate operator will not return a stream of type aggType, but a stream
            // with a type given by fd.defaultZero.
            DBSPTypeTuple typeFromAggregate = fold.defaultZeroType();
            DBSPStreamAggregateOperator agg = new DBSPStreamAggregateOperator(node,
                    this.makeIndexedZSet(groupType, typeFromAggregate), null, fold, index, fold.isLinear());

            // Flatten the resulting set
            DBSPTypeTupleBase kvType = new DBSPTypeRawTuple(groupType.ref(), typeFromAggregate.ref());
            DBSPVariablePath kv = kvType.var("kv");
            DBSPExpression[] flattenFields = new DBSPExpression[aggregate.getGroupCount() + aggType.size()];
            for (int i = 0; i < aggregate.getGroupCount(); i++)
                flattenFields[i] = kv.deepCopy().field(0).deref().field(i).applyCloneIfNeeded();
            for (int i = 0; i < aggType.size(); i++) {
                DBSPExpression flattenField = kv.deepCopy().field(1).deref().field(i).applyCloneIfNeeded();
                // Here we correct from the type produced by the Folder (typeFromAggregate) to the
                // actual expected type aggType (which is the tuple of aggTypes).
                flattenFields[aggregate.getGroupCount() + i] = flattenField.cast(aggTypes[i]);
            }
            DBSPExpression mapper = new DBSPTupleExpression(flattenFields).closure(kv.asParameter());
            this.circuit.addOperator(agg);
            DBSPMapOperator map = new DBSPMapOperator(node, mapper, this.makeZSet(tuple), agg);
            if (aggregate.getGroupCount() == 0) {
                // This almost works, but we have a problem with empty input collections
                // for aggregates without grouping.
                // aggregate_stream returns empty collections for empty input collections -- the fold
                // method is never invoked.
                // So we need to do some postprocessing step for this case.
                // The current result is a zset like {}/{c->1}: either the empty set (for an empty input)
                // or the correct count with a weight of 1.
                // We need to produce {z->1}/{c->1}, where z is the actual zero of the fold above.
                // For this we synthesize the following graph:
                // {}/{c->1}------------------------
                //    | map (|x| x -> z}           |
                // {}/{z->1}                       |
                //    | -                          |
                // {} {z->-1}   {z->1} (constant)  |
                //          \  /                  /
                //           +                   /
                //         {z->1}/{}  -----------
                //                 \ /
                //                  +
                //              {z->1}/{c->1}
                this.circuit.addOperator(map);
                DBSPVariablePath _t = tuple.ref().var("_t");
                DBSPExpression toZero = fold.defaultZero().closure(_t.asParameter());
                DBSPOperator map1 = new DBSPMapOperator(node, toZero, this.makeZSet(type), map);
                this.circuit.addOperator(map1);
                DBSPOperator neg = new DBSPNegateOperator(node, map1);
                this.circuit.addOperator(neg);
                DBSPOperator constant = new DBSPConstantOperator(
                        node, new DBSPZSetLiteral(fold.defaultZero()), false);
                this.circuit.addOperator(constant);
                DBSPOperator sum = new DBSPSumOperator(node, Linq.list(constant, neg, map));
                this.assignOperator(aggregate, sum);
            } else {
                this.assignOperator(aggregate, map);
            }
        } else {
            DBSPOperator dist = new DBSPStreamDistinctOperator(node, opInput);
            this.assignOperator(aggregate, dist);
        }
    }

    public void visitScan(TableScan scan, boolean create) {
        CalciteObject node = CalciteObject.create(scan);
        List<String> name = scan.getTable().getQualifiedName();
        String tableName = name.get(name.size() - 1);
        @Nullable
        DBSPOperator source = this.circuit.getInput(tableName);
        // The inputs should have been created while parsing the CREATE TABLE statements.
        if (source != null) {
            // Multiple queries can share an input.
            // Or the input may have been created by a CREATE TABLE statement.
            Utilities.putNew(this.nodeOperator, scan, source);
        } else {
            // Try a view if no table with this name exists.
            source = this.circuit.getOutput(tableName);
            if (source != null) {
                // We add the sink's source because sink operators do not have outputs.
                // A table scan for a sink operator can appear because of
                // a VIEW that is an input to a query.
                Utilities.putNew(this.nodeOperator, scan, source.to(DBSPSinkOperator.class).input());
            } else {
                // Try a noop
                source = this.circuit.getNoop(tableName);
                if (source == null) {
                    if (!create)
                        throw new InternalCompilerError("Could not find operator for table " + tableName, node);

                    // Create external tables table
                    JdbcTableScan jscan = (JdbcTableScan) scan;
                    RelDataType tableRowType = jscan.jdbcTable.getRowType(this.compiler.frontend.typeFactory);
                    DBSPTypeStruct originalRowType = this.convertType(tableRowType, true).to(DBSPTypeStruct.class);
                    DBSPType rowType = originalRowType.toTuple();
                    HasSchema withSchema = new HasSchema(CalciteObject.EMPTY, tableName, false, tableRowType);
                    this.metadata.addTable(withSchema);
                    InputTableMetadata tableMeta = new InputTableMetadata(
                            Linq.map(withSchema.getColumns(), this::convertMetadata));
                    source = new DBSPSourceMultisetOperator(
                            node, CalciteObject.EMPTY,
                            this.makeZSet(rowType), originalRowType,
                            null, tableMeta, tableName);
                    this.circuit.addOperator(source);
                }
                Utilities.putNew(this.nodeOperator, scan, source);
            }
        }
    }

    void assignOperator(RelNode rel, DBSPOperator op) {
        Utilities.putNew(this.nodeOperator, rel, op);
        this.circuit.addOperator(op);
    }

    DBSPOperator getOperator(RelNode node) {
        return Utilities.getExists(this.nodeOperator, node);
    }

    public void visitProject(LogicalProject project) {
        CalciteObject node = CalciteObject.create(project);
        // LogicalProject is not really SQL project, it is rather map.
        RelNode input = project.getInput();
        DBSPOperator opInput = this.getInputAs(input, true);
        DBSPType outputElementType = this.convertType(project.getRowType(), false);
        DBSPTypeTuple tuple = outputElementType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(project.getInput().getRowType(), false);
        DBSPVariablePath row = inputType.ref().var("t");
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(row, this.compiler);

        List<DBSPExpression> resultColumns = new ArrayList<>();
        int index = 0;
        for (RexNode column : project.getProjects()) {
            if (column instanceof RexOver) {
                throw new UnsupportedException("Optimizer should have removed OVER expressions",
                        CalciteObject.create(column));
            } else {
                DBSPExpression exp = expressionCompiler.compile(column);
                DBSPType expectedType = tuple.getFieldType(index);
                if (!exp.getType().sameType(expectedType)) {
                    // Calcite's optimizations do not preserve types!
                    exp = exp.applyCloneIfNeeded().cast(expectedType);
                }
                resultColumns.add(exp);
                index++;
            }
        }
        DBSPExpression exp = new DBSPTupleExpression(node, resultColumns);
        DBSPExpression mapFunc = new DBSPClosureExpression(node, exp, row.asParameter());
        DBSPMapOperator op = new DBSPMapOperator(
                node, mapFunc, this.makeZSet(outputElementType), opInput);
        // No distinct needed - in SQL project may produce a multiset.
        this.assignOperator(project, op);
    }

    DBSPOperator castOutput(CalciteObject node, DBSPOperator operator, DBSPType outputElementType) {
        DBSPType inputElementType = operator.getOutputZSetElementType();
        if (inputElementType.sameType(outputElementType))
            return operator;
        DBSPExpression function = inputElementType.caster(outputElementType);
        DBSPOperator map = new DBSPMapOperator(
                node, function, this.makeZSet(outputElementType), operator);
        this.circuit.addOperator(map);
        return map;
    }

    private void visitUnion(LogicalUnion union) {
        CalciteObject node = CalciteObject.create(union);
        RelDataType rowType = union.getRowType();
        DBSPType outputType = this.convertType(rowType, false);
        List<DBSPOperator> inputs = Linq.map(union.getInputs(), this::getOperator);
        // input type nullability may not match
        inputs = Linq.map(inputs, o -> this.castOutput(node, o, outputType));
        DBSPSumOperator sum = new DBSPSumOperator(node, inputs);
        if (union.all) {
            this.assignOperator(union, sum);
        } else {
            this.circuit.addOperator(sum);
            DBSPStreamDistinctOperator d = new DBSPStreamDistinctOperator(node, sum);
            this.assignOperator(union, d);
        }
    }

    private void visitMinus(LogicalMinus minus) {
        CalciteObject node = CalciteObject.create(minus);
        boolean first = true;
        RelDataType rowType = minus.getRowType();
        DBSPType outputType = this.convertType(rowType, false);
        List<DBSPOperator> inputs = new ArrayList<>();
        for (RelNode input : minus.getInputs()) {
            DBSPOperator opInput = this.getInputAs(input, false);
            if (!first) {
                DBSPOperator neg = new DBSPNegateOperator(node, opInput);
                neg = this.castOutput(node, neg, outputType);
                this.circuit.addOperator(neg);
                inputs.add(neg);
            } else {
                opInput = this.castOutput(node, opInput, outputType);
                inputs.add(opInput);
            }
            first = false;
        }

        DBSPSumOperator sum = new DBSPSumOperator(node, inputs);
        if (minus.all) {
            this.assignOperator(minus, sum);
        } else {
            this.circuit.addOperator(sum);
            DBSPStreamDistinctOperator d = new DBSPStreamDistinctOperator(node, sum);
            this.assignOperator(minus, d);
        }
    }

    public void visitFilter(LogicalFilter filter) {
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
        DBSPVariablePath t = type.ref().var("t");
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.compiler);
        DBSPExpression condition = expressionCompiler.compile(filter.getCondition());
        condition = ExpressionCompiler.wrapBoolIfNeeded(condition);
        condition = new DBSPClosureExpression(CalciteObject.create(filter.getCondition()), condition, t.asParameter());
        DBSPOperator input = this.getOperator(filter.getInput());
        DBSPFilterOperator fop = new DBSPFilterOperator(node, condition, input);
        this.assignOperator(filter, fop);
    }

    private DBSPOperator filterNonNullKeys(LogicalJoin join,
            List<Integer> keyFields, DBSPOperator input) {
        CalciteObject node = CalciteObject.create(join);
        DBSPTypeTuple rowType = input.getType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        boolean shouldFilter = Linq.any(keyFields, i -> rowType.tupFields[i].mayBeNull);
        if (!shouldFilter) return input;

        DBSPVariablePath var = rowType.ref().var("r");
        // Build a condition that checks whether any of the key fields is null.
        @Nullable
        DBSPExpression condition = null;
        for (int i = 0; i < rowType.size(); i++) {
            if (keyFields.contains(i)) {
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
        condition = new DBSPUnaryExpression(node, condition.getType(), DBSPOpcode.NOT, condition);
        DBSPClosureExpression filterFunc = condition.closure(var.asParameter());
        DBSPFilterOperator filter = new DBSPFilterOperator(node, filterFunc, input);
        this.circuit.addOperator(filter);
        return filter;
    }

    private void visitJoin(LogicalJoin join) {
        CalciteObject node = CalciteObject.create(join);
        JoinRelType joinType = join.getJoinType();
        if (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI)
            throw new UnimplementedException(node);

        DBSPTypeTuple resultType = this.convertType(join.getRowType(), false).to(DBSPTypeTuple.class);
        if (join.getInputs().size() != 2)
            throw new InternalCompilerError("Unexpected join with " + join.getInputs().size() + " inputs", node);
        DBSPOperator left = this.getInputAs(join.getInput(0), true);
        DBSPOperator right = this.getInputAs(join.getInput(1), true);
        DBSPTypeTuple leftElementType = left.getType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);

        JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(node,
                leftElementType.to(DBSPTypeTuple.class).size(), this.compiler.getTypeCompiler());
        JoinConditionAnalyzer.ConditionDecomposition decomposition = analyzer.analyze(join.getCondition());
        // If any key field is nullable we need to filter the inputs; this will make key columns non-nullable
        DBSPOperator filteredLeft = this.filterNonNullKeys(join, Linq.map(decomposition.comparisons, c -> c.leftColumn), left);
        DBSPOperator filteredRight = this.filterNonNullKeys(join, Linq.map(decomposition.comparisons, c -> c.rightColumn), right);

        leftElementType = filteredLeft.getType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        DBSPTypeTuple rightElementType = filteredRight.getType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);

        int leftColumns = leftElementType.size();
        int rightColumns = rightElementType.size();
        int totalColumns = leftColumns + rightColumns;
        DBSPTypeTuple leftResultType = resultType.slice(0, leftColumns);
        DBSPTypeTuple rightResultType = resultType.slice(leftColumns, leftColumns + rightColumns);

        DBSPVariablePath l = leftElementType.ref().var("l");
        DBSPVariablePath r = rightElementType.ref().var("r");
        DBSPTupleExpression lr = DBSPTupleExpression.flatten(l.deref(), r.deref());
        List<DBSPExpression> leftKeyFields = Linq.map(
                decomposition.comparisons,
                c -> l.deref().field(c.leftColumn).applyCloneIfNeeded().cast(c.commonType));
        List<DBSPExpression> rightKeyFields = Linq.map(
                decomposition.comparisons,
                c -> r.deref().field(c.rightColumn).applyCloneIfNeeded().cast(c.commonType));
        DBSPExpression leftKey = new DBSPTupleExpression(node, leftKeyFields);
        DBSPExpression rightKey = new DBSPTupleExpression(node, rightKeyFields);

        @Nullable
        RexNode leftOver = decomposition.getLeftOver();
        DBSPExpression condition = null;
        DBSPExpression originalCondition = null;
        if (leftOver != null) {
            DBSPVariablePath t = lr.getType().ref().var("t");
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.compiler);
            condition = expressionCompiler.compile(leftOver);
            if (condition.getType().mayBeNull)
                condition = ExpressionCompiler.wrapBoolIfNeeded(condition);
            originalCondition = condition;
            condition = new DBSPClosureExpression(CalciteObject.create(join.getCondition()), condition, t.asParameter());
        }
        DBSPVariablePath k = leftKey.getType().ref().var("k");

        DBSPClosureExpression toLeftKey = new DBSPTupleExpression(
                leftKey, DBSPTupleExpression.flatten(l.deref()))
                .closure(l.asParameter());
        DBSPIndexOperator leftIndex = new DBSPIndexOperator(
                node, toLeftKey,
                this.makeIndexedZSet(leftKey.getType(), leftElementType), false, filteredLeft);
        this.circuit.addOperator(leftIndex);

        DBSPClosureExpression toRightKey = new DBSPTupleExpression(
                rightKey, DBSPTupleExpression.flatten(r.deref()))
                .closure(r.asParameter());
        DBSPIndexOperator rIndex = new DBSPIndexOperator(
                node, toRightKey,
                this.makeIndexedZSet(rightKey.getType(), rightElementType), false, filteredRight);
        this.circuit.addOperator(rIndex);

        DBSPClosureExpression makeTuple = lr.closure(k.asParameter(), l.asParameter(), r.asParameter());
        DBSPOperator joinResult = new DBSPStreamJoinOperator(node, this.makeZSet(lr.getType()),
                makeTuple, left.isMultiset || right.isMultiset, leftIndex, rIndex);

        // Save the result of the inner join here
        DBSPOperator inner = joinResult;
        if (originalCondition != null) {
            // Apply additional filters
            DBSPBoolLiteral bLit = originalCondition.as(DBSPBoolLiteral.class);
            if (bLit == null || bLit.value == null || !bLit.value) {
                // Technically if blit.value == null or !blit.value then
                // the filter is false, and the result is empty.  But hopefully
                // the calcite optimizer won't allow that.
                this.circuit.addOperator(joinResult);
                inner = new DBSPFilterOperator(node, condition, joinResult);
                joinResult = inner;
            }
            // if bLit it true we don't need to filter.
        }

        if (!resultType.sameType(lr.getType())) {
            // For outer joins additional columns may become nullable.
            DBSPVariablePath t = new DBSPVariablePath("t", lr.getType().ref());
            DBSPExpression[] casts = new DBSPExpression[lr.size()];
            for (int index = 0; index < lr.size(); index++) {
                casts[index] = t.deref().field(index).applyCloneIfNeeded().cast(resultType.getFieldType(index));
            }
            DBSPTupleExpression allFields = new DBSPTupleExpression(casts);
            this.circuit.addOperator(joinResult);
            joinResult = new DBSPMapOperator(node, allFields.closure(t.asParameter()),
                    this.makeZSet(resultType), joinResult);
        }

        // Handle outer joins
        DBSPOperator result = joinResult;
        DBSPVariablePath joinVar = lr.getType().ref().var("j");
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
            this.circuit.addOperator(result);
            // project the join on the left columns
            DBSPClosureExpression toLeftColumns =
                    DBSPTupleExpression.flatten(joinVar.deref())
                            .slice(0, leftColumns)
                            .pointwiseCast(leftResultType).closure(joinVar.asParameter());
            DBSPOperator joinLeftColumns = new DBSPMapOperator(
                    node, toLeftColumns, this.makeZSet(leftResultType), inner);
            this.circuit.addOperator(joinLeftColumns);
            DBSPOperator distJoin = new DBSPStreamDistinctOperator(node, joinLeftColumns);
            this.circuit.addOperator(distJoin);

            // subtract from left relation
            DBSPOperator leftCast = left;
            if (!leftResultType.sameType(leftElementType)) {
                DBSPClosureExpression castLeft =
                    DBSPTupleExpression.flatten(l.deref())
                            .pointwiseCast(leftResultType).closure(l.asParameter()
                );
                leftCast = new DBSPMapOperator(node, castLeft, this.makeZSet(leftResultType), left);
                this.circuit.addOperator(leftCast);
            }
            DBSPOperator sub = new DBSPSubtractOperator(node, leftCast, distJoin);
            this.circuit.addOperator(sub);
            DBSPStreamDistinctOperator dist = new DBSPStreamDistinctOperator(node, sub);
            this.circuit.addOperator(dist);

            // fill nulls in the right relation fields
            DBSPTupleExpression rEmpty = new DBSPTupleExpression(
                    Linq.map(rightElementType.tupFields,
                             et -> DBSPLiteral.none(et.setMayBeNull(true)), DBSPExpression.class));
            DBSPVariablePath lCasted = leftResultType.ref().var("l");
            DBSPClosureExpression leftRow = DBSPTupleExpression.flatten(lCasted.deref(), rEmpty).closure(
                    lCasted.asParameter());
            DBSPOperator expand = new DBSPMapOperator(node, leftRow, this.makeZSet(resultType), dist);
            this.circuit.addOperator(expand);
            result = new DBSPSumOperator(node, result, expand);
        }
        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
            this.circuit.addOperator(result);

            // project the join on the right columns
            DBSPClosureExpression toRightColumns =
                    DBSPTupleExpression.flatten(joinVar.deref())
                            .slice(leftColumns, totalColumns)
                            .pointwiseCast(rightResultType).closure(
                    joinVar.asParameter());
            DBSPOperator joinRightColumns = new DBSPMapOperator(
                    node, toRightColumns, this.makeZSet(rightResultType), inner);
            this.circuit.addOperator(joinRightColumns);
            DBSPOperator distJoin = new DBSPStreamDistinctOperator(node, joinRightColumns);
            this.circuit.addOperator(distJoin);

            // subtract from right relation
            DBSPOperator rightCast = right;
            if (!rightResultType.sameType(rightElementType)) {
                DBSPClosureExpression castRight =
                        DBSPTupleExpression.flatten(r.deref())
                                .pointwiseCast(rightResultType).closure(
                        r.asParameter());
                rightCast = new DBSPMapOperator(node, castRight, this.makeZSet(rightResultType), right);
                this.circuit.addOperator(rightCast);
            }
            DBSPOperator sub = new DBSPSubtractOperator(node, rightCast, distJoin);
            this.circuit.addOperator(sub);
            DBSPStreamDistinctOperator dist = new DBSPStreamDistinctOperator(node, sub);
            this.circuit.addOperator(dist);

            // fill nulls in the left relation fields
            DBSPTupleExpression lEmpty = new DBSPTupleExpression(
                    Linq.map(leftElementType.tupFields,
                            et -> DBSPLiteral.none(et.setMayBeNull(true)), DBSPExpression.class));
            DBSPVariablePath rCasted = rightResultType.ref().var("r");
            DBSPClosureExpression rightRow =
                    DBSPTupleExpression.flatten(lEmpty, rCasted.deref()).closure(
                    rCasted.asParameter());
            DBSPOperator expand = new DBSPMapOperator(node,
                    rightRow, this.makeZSet(resultType), dist);
            this.circuit.addOperator(expand);
            result = new DBSPSumOperator(node, result, expand);
        }

        this.assignOperator(join, Objects.requireNonNull(result));
    }

    @Nullable
    ModifyTableTranslation modifyTableTranslation;

    /**
     * Visit a LogicalValue: a SQL literal, as produced by a VALUES expression.
     * This can be invoked by a DDM statement, or by a SQL query that computes a constant result.
     */
    public void visitLogicalValues(LogicalValues values) {
        CalciteObject node = CalciteObject.create(values);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(null, this.compiler);
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
                    if (lit.isNull)
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
            DBSPOperator constant = new DBSPConstantOperator(node, result, false);
            this.assignOperator(values, constant);
        }
    }

    public void visitIntersect(LogicalIntersect intersect) {
        CalciteObject node = CalciteObject.create(intersect);
        // Intersect is a special case of join.
        List<RelNode> inputs = intersect.getInputs();
        RelNode input = intersect.getInput(0);
        DBSPOperator previous = this.getInputAs(input, false);

        if (inputs.isEmpty())
            throw new UnsupportedException(node);
        if (inputs.size() == 1) {
            Utilities.putNew(this.nodeOperator, intersect, previous);
            return;
        }

        DBSPType inputRowType = this.convertType(input.getRowType(), false);
        DBSPTypeTuple resultType = this.convertType(intersect.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPVariablePath t = inputRowType.ref().var("t");
        DBSPExpression entireKey =
                new DBSPTupleExpression(
                        t.deref().applyClone(),
                        new DBSPRawTupleExpression()).closure(
                t.asParameter());
        DBSPVariablePath l = new DBSPTypeRawTuple().ref().var("l");
        DBSPVariablePath r = new DBSPTypeRawTuple().ref().var("r");
        DBSPVariablePath k = inputRowType.ref().var("k");

        DBSPClosureExpression closure = k.deref().applyClone().closure(
                k.asParameter(), l.asParameter(), r.asParameter());
        for (int i = 1; i < inputs.size(); i++) {
            DBSPOperator previousIndex = new DBSPIndexOperator(
                    node, entireKey,
                    this.makeIndexedZSet(inputRowType, new DBSPTypeRawTuple()),
                    previous.isMultiset, previous);
            this.circuit.addOperator(previousIndex);
            DBSPOperator inputI = this.getInputAs(intersect.getInput(i), false);
            DBSPOperator index = new DBSPIndexOperator(
                    node, entireKey.deepCopy(),
                    this.makeIndexedZSet(inputRowType, new DBSPTypeRawTuple()),
                    inputI.isMultiset, inputI);
            this.circuit.addOperator(index);
            previous = new DBSPStreamJoinOperator(node, this.makeZSet(resultType),
                    closure, false, previousIndex, index);
            this.circuit.addOperator(previous);
        }
        Utilities.putNew(this.nodeOperator, intersect, previous);
    }

    DBSPExpression compileWindowBound(RexWindowBound bound, DBSPType boundType, ExpressionCompiler eComp) {
        IsNumericType numType = boundType.to(IsNumericType.class);
        DBSPExpression numericBound;
        if (bound.isUnbounded())
            numericBound = numType.getMaxValue();
        else if (bound.isCurrentRow())
            numericBound = numType.getZero();
        else {
            DBSPExpression value = eComp.compile(Objects.requireNonNull(bound.getOffset()));
            numericBound = value.cast(boundType);
        }
        String beforeAfter = bound.isPreceding() ? "Before" : "After";
        return new DBSPConstructorExpression(
                new DBSPPath("RelOffset", beforeAfter).toExpression(),
                DBSPTypeAny.getDefault(), numericBound);
    }

    /**
     * If this is not null, the parent LogicalFilter should use this implementation
     * instead of generating code.
     */
    @Nullable DBSPOperator filterImplementation = null;

    public void generateNestedTopK(LogicalWindow window, Window.Group group, int limit, SqlKind kind) {
        DBSPIndexedTopKOperator.TopKNumbering numbering = switch (kind) {
            case RANK -> RANK;
            case DENSE_RANK -> DENSE_RANK;
            case ROW_NUMBER -> ROW_NUMBER;
            default -> throw new UnimplementedException("Ranking function " + kind + " not yet implemented",
                    CalciteObject.create(window));
        };

        // This code duplicates code from the SortNode.
        CalciteObject node = CalciteObject.create(window);
        RelNode input = window.getInput();
        DBSPOperator opInput = this.getOperator(input);

        DBSPType inputRowType = this.convertType(input.getRowType(), false);
        DBSPVariablePath t = inputRowType.ref().var("t");
        DBSPExpression[] fields = new DBSPExpression[group.keys.cardinality()];
        int ix = 0;
        for (int field : group.keys.toList()) {
            fields[ix++] = t.deref().field(field);
        }
        DBSPExpression groupKeys =
                new DBSPTupleExpression(
                        new DBSPTupleExpression(fields),
                        DBSPTupleExpression.flatten(t.deref())).closure(t.asParameter());
        DBSPOperator index = new DBSPIndexOperator(
                node, groupKeys,
                this.makeIndexedZSet(groupKeys.getType(), inputRowType),
                opInput.isMultiset, opInput);
        this.circuit.addOperator(index);

        // Generate comparison function for sorting the vector
        DBSPComparatorExpression comparator = new DBSPNoComparatorExpression(node, inputRowType);
        for (RelFieldCollation collation : group.orderKeys.getFieldCollations()) {
            int field = collation.getFieldIndex();
            RelFieldCollation.Direction direction = collation.getDirection();
            boolean ascending = switch (direction) {
                case ASCENDING -> true;
                case DESCENDING -> false;
                default -> throw new UnimplementedException(node);
            };
            comparator = new DBSPFieldComparatorExpression(node, comparator, field, ascending);
        }

        // The rank must be added at the end of the input tuple (that's how Calcite expects it).
        DBSPVariablePath left = new DBSPVariablePath("left", new DBSPTypeInteger(
                node, 64, true, false));
        DBSPVariablePath right = new DBSPVariablePath("right", inputRowType.ref());
        List<DBSPExpression> flattened = DBSPTypeTupleBase.flatten(right.deref());
        flattened.add(left);
        DBSPTupleExpression tuple = new DBSPTupleExpression(flattened, false);
        DBSPClosureExpression outputProducer = tuple.closure(left.asParameter(), right.asParameter());

        // TopK operator.
        // Since TopK is always incremental we have to wrap it into a D-I pair
        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, index);
        this.circuit.addOperator(diff);
        DBSPI32Literal limitValue = new DBSPI32Literal(limit);
        DBSPIndexedTopKOperator topK = new DBSPIndexedTopKOperator(
                node, numbering, comparator, limitValue, outputProducer, diff);
        this.circuit.addOperator(topK);
        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, topK);
        this.circuit.addOperator(integral);
        // We must drop the index we built.
        DBSPDeindexOperator deindex = new DBSPDeindexOperator(node, integral);
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
        if (compared instanceof RexInputRef) {
            RexInputRef ri = (RexInputRef) compared;
            if (ri.getIndex() != expectedIndex)
                return -1;
        }
        if (limit instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) limit;
            SqlTypeName type = literal.getType().getSqlTypeName();
            if (SqlTypeName.INT_TYPES.contains(type)) {
                Integer value = literal.getValueAs(Integer.class);
                Objects.requireNonNull(value);
                if (!eq)
                    value++;
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

    public void visitWindow(LogicalWindow window) {
        CalciteObject node = CalciteObject.create(window);

        DBSPTypeTuple windowResultType = this.convertType(window.getRowType(), false).to(DBSPTypeTuple.class);
        RelNode inputNode = window.getInput();
        DBSPOperator input = this.getInputAs(window.getInput(0), true);
        DBSPTypeTuple inputRowType = this.convertType(inputNode.getRowType(), false).to(DBSPTypeTuple.class);
        DBSPVariablePath inputRowRefVar = inputRowType.ref().var("t");
        ExpressionCompiler eComp = new ExpressionCompiler(inputRowRefVar, window.constants, this.compiler);
        int windowFieldIndex = inputRowType.size();
        DBSPVariablePath previousRowRefVar = inputRowRefVar;

        DBSPTypeTuple currentTupleType = inputRowType;
        DBSPOperator lastOperator = input;

        // Special handling for the following pattern:
        // LogicalFilter(condition=[<=(RANK_COLUMN, LIMIT)])
        // LogicalWindow(window#0=[window(partition ... order by ... aggs [RANK()])])
        // This is compiled into a TopK over each group.
        // There is no way this can be expressed otherwise in SQL or using RelNode.
        // This works for RANK, DENSE_RANK, and ROW_NUMBER.
        if (window.groups.size() == 1) {
            Window.Group group = window.groups.get(0);
            if (group.aggCalls.size() == 1) {
                Window.RexWinAggCall agg = group.aggCalls.get(0);
                SqlOperator operator = agg.getOperator();
                if (operator instanceof SqlRankFunction) {
                    SqlRankFunction rank = (SqlRankFunction) operator;
                    // Aggregate functions seem always to be at the end.
                    // The window always seems to collect all fields.
                    int rankIndex = inputRowType.size();
                    RelNode parent = this.getParent();
                    if (parent instanceof LogicalFilter) {
                        LogicalFilter filter = (LogicalFilter) parent;
                        RexNode condition = filter.getCondition();
                        if (condition instanceof RexCall) {
                            int limit = this.isLimit((RexCall) condition, rankIndex);
                            if (limit >= 0) {
                                this.generateNestedTopK(window, group, limit, rank.kind);
                                return;
                            }
                        }
                    }
                }
            }
        }

        for (Window.Group group: window.groups) // noinspection GrazieInspection
        {
            if (lastOperator != input)
                this.circuit.addOperator(lastOperator);
            List<RelFieldCollation> orderKeys = group.orderKeys.getFieldCollations();
            // Sanity checks
            if (orderKeys.size() > 1)
                throw new UnimplementedException("ORDER BY not yet supported with multiple columns", node);
            RelFieldCollation collation = orderKeys.get(0);
            if (collation.getDirection() != RelFieldCollation.Direction.ASCENDING)
                throw new UnimplementedException("OVER only supports ascending sorting", node);
            int orderColumnIndex = collation.getFieldIndex();
            DBSPExpression orderField = inputRowRefVar.deref().field(orderColumnIndex);
            DBSPType sortType = inputRowType.tupFields[orderColumnIndex];
            if (!sortType.is(DBSPTypeInteger.class) &&
                    !sortType.is(DBSPTypeTimestamp.class) &&
                    !sortType.is(DBSPTypeDate.class))
                throw new UnimplementedException("OVER currently requires an integer type for ordering "
                        + "and cannot handle " + sortType, node);
            if (sortType.mayBeNull) {
                RelDataTypeField relDataTypeField = inputNode.getRowType().getFieldList().get(orderColumnIndex);
                throw new UnimplementedException("OVER currently does not support sorting on nullable column " +
                        Utilities.singleQuote(relDataTypeField.getName()) + ":", node);
            }

            // Create window description
            DBSPExpression lb = this.compileWindowBound(group.lowerBound, sortType, eComp);
            DBSPExpression ub = this.compileWindowBound(group.upperBound, sortType, eComp);
            DBSPExpression windowExpr = new DBSPConstructorExpression(
                    new DBSPPath("RelRange", "new").toExpression(),
                    DBSPTypeAny.getDefault(), lb, ub);

            // Map each row to an expression of the form: |t| (partition, (order, (*t).clone()))
            List<Integer> partitionKeys = group.keys.toList();
            List<DBSPExpression> expressions = Linq.map(partitionKeys,
                    inputRowRefVar.deepCopy().deref()::field);
            DBSPTupleExpression partition = new DBSPTupleExpression(node, expressions);
            DBSPExpression orderAndRow = new DBSPTupleExpression(
                    orderField, inputRowRefVar.deepCopy().deref().applyClone());
            DBSPExpression mapExpr = new DBSPRawTupleExpression(partition, orderAndRow);
            DBSPClosureExpression mapClo = mapExpr.closure(inputRowRefVar.asParameter());
            DBSPOperator mapIndex = new DBSPMapIndexOperator(node, mapClo,
                    this.makeIndexedZSet(partition.getType(), orderAndRow.getType()), input);
            this.circuit.addOperator(mapIndex);

            List<AggregateCall> aggregateCalls = group.getAggregateCalls(window);
            List<DBSPType> types = Linq.map(aggregateCalls, c -> this.convertType(c.type, false));
            DBSPTypeTuple tuple = new DBSPTypeTuple(types);
            DBSPAggregate fd = this.createAggregate(window, aggregateCalls, tuple, inputRowType, 0);

            // Compute aggregates for the window
            DBSPTypeTuple aggResultType = fd.defaultZeroType().to(DBSPTypeTuple.class);
            // This operator is always incremental, so create the non-incremental version
            // of it by adding a D and an I around it.
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, mapIndex);
            this.circuit.addOperator(diff);
            DBSPWindowAggregateOperator windowAgg = new DBSPWindowAggregateOperator(
                    node, null, fd,
                    windowExpr,
                    this.makeIndexedZSet(
                            new DBSPTypeTuple(partition.getType(), sortType),
                            aggResultType), diff);
            this.circuit.addOperator(windowAgg);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, windowAgg);
            this.circuit.addOperator(integral);

            // Join the previous result with the aggregate
            // First index the aggregate.
            DBSPExpression partAndOrder = new DBSPTupleExpression(
                    partition.applyCloneIfNeeded(),
                    orderField.applyCloneIfNeeded());
            DBSPExpression indexedInput = new DBSPTupleExpression(
                    partAndOrder, previousRowRefVar.deepCopy().deref().applyClone());
            DBSPExpression partAndOrderClo = indexedInput.closure(previousRowRefVar.asParameter());
            DBSPOperator indexInput = new DBSPIndexOperator(node, partAndOrderClo,
                    this.makeIndexedZSet(partAndOrder.getType(), previousRowRefVar.getType().deref()),
                    lastOperator.isMultiset, lastOperator);
            this.circuit.addOperator(indexInput);

            DBSPVariablePath key = partAndOrder.getType().ref().var("k");
            DBSPVariablePath left = currentTupleType.ref().var("l");
            DBSPVariablePath right = aggResultType.ref().var("r");
            DBSPExpression[] allFields = new DBSPExpression[
                    currentTupleType.size() + aggResultType.size()];
            for (int i = 0; i < currentTupleType.size(); i++)
                allFields[i] = left.deref().field(i).applyCloneIfNeeded();
            for (int i = 0; i < aggResultType.size(); i++) {
                // Calcite is very smart and sometimes infers non-nullable result types
                // for these aggregates.  So we have to cast the results to whatever
                // Calcite says they will be.
                allFields[i + currentTupleType.size()] = right.deref().field(i).applyCloneIfNeeded().cast(
                        windowResultType.getFieldType(windowFieldIndex));
                windowFieldIndex++;
            }
            DBSPTupleExpression addExtraFieldBody = new DBSPTupleExpression(allFields);
            DBSPClosureExpression addExtraField =
                    addExtraFieldBody.closure(key.asParameter(), left.asParameter(), right.asParameter());
            lastOperator = new DBSPStreamJoinOperator(node, this.makeZSet(addExtraFieldBody.getType()),
                    addExtraField, indexInput.isMultiset || windowAgg.isMultiset, indexInput, integral);
            currentTupleType = addExtraFieldBody.getType().to(DBSPTypeTuple.class);
            previousRowRefVar = currentTupleType.ref().var("t");
        }
        this.assignOperator(window, lastOperator);
    }

    private RelNode getParent() {
        return Utilities.last(this.ancestors);
    }

    public void visitSort(LogicalSort sort) {
        CalciteObject node = CalciteObject.create(sort);
        RelNode input = sort.getInput();
        DBSPOperator opInput = this.getOperator(input);
        if (this.options.languageOptions.ignoreOrderBy && sort.fetch == null) {
            Utilities.putNew(this.nodeOperator, sort, opInput);
            return;
        }
        if (sort.offset != null)
            throw new UnimplementedException(node);

        DBSPExpression limit = null;
        if (sort.fetch != null) {
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(null, this.compiler);
            // We expect the limit to be a constant
            limit = expressionCompiler.compile(sort.fetch);
        }

        DBSPType inputRowType = this.convertType(input.getRowType(), false);
        DBSPVariablePath t = inputRowType.ref().var("t");
        DBSPExpression emptyGroupKeys =
                new DBSPTupleExpression(
                        new DBSPRawTupleExpression(),
                        DBSPTupleExpression.flatten(t.deref())).closure(t.asParameter());
        DBSPOperator index = new DBSPIndexOperator(
                node, emptyGroupKeys,
                this.makeIndexedZSet(new DBSPTypeRawTuple(), inputRowType),
                opInput.isMultiset, opInput);
        this.circuit.addOperator(index);

        // Generate comparison function for sorting the vector
        DBSPComparatorExpression comparator = new DBSPNoComparatorExpression(node, inputRowType);
        for (RelFieldCollation collation : sort.getCollation().getFieldCollations()) {
            int field = collation.getFieldIndex();
            RelFieldCollation.Direction direction = collation.getDirection();
            boolean ascending = switch (direction) {
                case ASCENDING -> true;
                case DESCENDING -> false;
                default -> throw new UnimplementedException(node);
            };
            comparator = new DBSPFieldComparatorExpression(node, comparator, field, ascending);
        }

        if (sort.fetch != null) {
            // TopK operator.
            // Since TopK is always incremental we have to wrap it into a D-I pair
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, index);
            this.circuit.addOperator(diff);
            DBSPIndexedTopKOperator topK = new DBSPIndexedTopKOperator(
                    node, DBSPIndexedTopKOperator.TopKNumbering.ROW_NUMBER,
                    comparator, limit, null, diff);
            this.circuit.addOperator(topK);
            DBSPIntegrateOperator integral = new DBSPIntegrateOperator(node, topK);
            this.circuit.addOperator(integral);
            // If we ignore ORDER BY this is the result.
            if (this.options.languageOptions.ignoreOrderBy) {
                // We must drop the index we built.
                DBSPDeindexOperator deindex = new DBSPDeindexOperator(node, integral);
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
        DBSPVariablePath accum = vecType.ref(true).var("a");
        DBSPVariablePath row = inputRowType.ref().var("v");
        // An element with weight 'w' is pushed 'w' times into the vector
        DBSPExpression wPush = new DBSPApplyExpression(node,
                "weighted_push", new DBSPTypeVoid(), accum, row, this.compiler.weightVar);
        DBSPExpression push = wPush.closure(
                accum.asParameter(), row.asParameter(),
                this.compiler.weightVar.asParameter());
        DBSPExpression constructor =
                new DBSPPath(
                        new DBSPSimplePathSegment("Fold",
                                DBSPTypeAny.getDefault(),
                                new DBSPTypeUser(node, USER, "UnimplementedSemigroup",
                                        false, DBSPTypeAny.getDefault()),
                                DBSPTypeAny.getDefault(),
                                DBSPTypeAny.getDefault()),
                        new DBSPSimplePathSegment("new")).toExpression();

        DBSPExpression folder = constructor.call(zero, push);
        DBSPStreamAggregateOperator agg = new DBSPStreamAggregateOperator(node,
                this.makeIndexedZSet(new DBSPTypeRawTuple(), new DBSPTypeVec(inputRowType, false)),
                folder, null, index, false);
        this.circuit.addOperator(agg);

        DBSPSortExpression sorter = new DBSPSortExpression(node, inputRowType, comparator);
        DBSPOperator result = new DBSPMapOperator(
                node, sorter, this.makeZSet(vecType), agg);
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
                this.visitIfMatches(node, Uncollect.class, this::visitUncollect);
        if (!success)
            throw new UnimplementedException(CalciteObject.create(node));
    }

    InputColumnMetadata convertMetadata(RelColumnMetadata metadata) {
        DBSPType type = this.convertType(metadata.getType(), false);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(null, this.compiler);
        DBSPExpression lateness = null;
        if (metadata.lateness != null)
            lateness = expressionCompiler.compile(metadata.lateness);
        DBSPExpression defaultValue = null;
        if (metadata.defaultValue != null)
            defaultValue = expressionCompiler.compile(metadata.defaultValue).cast(type);
        return new InputColumnMetadata(metadata.getNode(), metadata.getName(), type,
                metadata.isPrimaryKey, lateness, defaultValue);
    }

    @SuppressWarnings("UnusedReturnValue")
    @Nullable
    public DBSPNode compile(FrontEndStatement statement) {
        if (statement.is(CreateViewStatement.class)) {
            CreateViewStatement view = statement.to(CreateViewStatement.class);
            RelNode rel = view.getRelNode();
            Logger.INSTANCE.belowLevel(this, 2)
                    .append(CalciteCompiler.getPlan(rel))
                    .newline();
            this.go(rel);
            // TODO: connect the result of the query compilation with
            // the fields of rel; for now we assume that these are 1/1
            DBSPOperator op = this.getOperator(rel);
            DBSPOperator o;
            if (this.generateOutputForNextView) {
                this.metadata.addView(view);
                DBSPType originalRowType = this.convertType(view.getRelNode().getRowType(), true);
                DBSPTypeStruct struct = originalRowType.to(DBSPTypeStruct.class).rename(view.relationName);
                o = new DBSPSinkOperator(
                        view.getCalciteObject(), view.relationName,
                        view.statement,
                        struct, statement.comment, op);
            } else {
                // We may already have a node for this output
                DBSPOperator previous = this.circuit.getOutput(view.relationName);
                if (previous != null)
                    return previous;
                o = new DBSPNoopOperator(view.getCalciteObject(), op, statement.comment, view.relationName);
            }
            this.circuit.addOperator(o);
            return o;
        } else if (statement.is(CreateTableStatement.class) ||
                statement.is(DropTableStatement.class)) {
            boolean success = this.tableContents.execute(statement);
            if (!success)
                return null;
            CreateTableStatement create = statement.as(CreateTableStatement.class);
            if (create != null) {
                // We create an input for the circuit.
                String tableName = create.relationName;
                CreateTableStatement def = this.tableContents.getTableDefinition(tableName);
                DBSPType rowType = def.getRowTypeAsTuple(this.compiler.getTypeCompiler());
                DBSPTypeStruct originalRowType = def.getRowTypeAsStruct(this.compiler.getTypeCompiler());
                CalciteObject identifier = CalciteObject.EMPTY;
                if (create.node instanceof SqlCreateTable) {
                    SqlCreateTable sct = (SqlCreateTable)create.node;
                    identifier = CalciteObject.create(sct.name);
                }
                List<InputColumnMetadata> metadata = Linq.map(create.columns, this::convertMetadata);
                InputTableMetadata tableMeta = new InputTableMetadata(metadata);
                DBSPSourceMultisetOperator result = new DBSPSourceMultisetOperator(
                        create.getCalciteObject(), identifier, this.makeZSet(rowType), originalRowType,
                        def.statement, tableMeta, tableName);
                this.circuit.addOperator(result);
                this.metadata.addTable(create);
            }
            return null;
        } else if (statement.is(TableModifyStatement.class)) {
            TableModifyStatement modify = statement.to(TableModifyStatement.class);
            // The type of the data must be extracted from the modified table
            if (!(modify.node instanceof SqlInsert))
                throw new UnimplementedException(statement.getCalciteObject());
            SqlInsert insert = (SqlInsert) statement.node;
            CreateTableStatement def = this.tableContents.getTableDefinition(modify.tableName);
            this.modifyTableTranslation = new ModifyTableTranslation(
                    modify, def, insert.getTargetColumnList(), this.compiler);
            if (modify.rel instanceof LogicalTableScan) {
                // Support for INSERT INTO table (SELECT * FROM otherTable)
                LogicalTableScan scan = (LogicalTableScan) modify.rel;
                List<String> name = scan.getTable().getQualifiedName();
                String sourceTable = name.get(name.size() - 1);
                DBSPZSetLiteral data = this.tableContents.getTableContents(sourceTable);
                this.tableContents.addToTable(modify.tableName, data);
                this.modifyTableTranslation = null;
                return new DBSPZSetLiteral(data);
            } else if (modify.rel instanceof LogicalValues) {
                this.go(modify.rel);
                DBSPZSetLiteral result = this.modifyTableTranslation.getTranslation();
                this.tableContents.addToTable(modify.tableName, result);
                this.modifyTableTranslation = null;
                return result;
            } else if (modify.rel instanceof LogicalProject) {
                // The Calcite optimizer is not able to convert a projection
                // of a VALUES statement that contains ARRAY constructors,
                // because there is no array literal in Calcite.  These expressions
                // are left as projections in the code.  We only handle
                // the case where all project expressions are "constants".
                DBSPZSetLiteral result = this.compileConstantProject((LogicalProject) modify.rel);
                this.tableContents.addToTable(modify.tableName, result);
                this.modifyTableTranslation = null;
                return result;
            }
        }
        throw new UnimplementedException(statement.getCalciteObject());
    }

    private DBSPZSetLiteral compileConstantProject(LogicalProject project) {
        // Specialization of the visitor's visit method for LogicalProject
        // Does not produce a DBSPOperator, but only a literal.
        CalciteObject node = CalciteObject.create(project);
        DBSPType outputElementType = this.convertType(project.getRowType(), false);
        DBSPTypeTuple tuple = outputElementType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(project.getInput().getRowType(), false);
        DBSPVariablePath row = inputType.ref().var("t");  // should not be used
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(row, this.compiler);
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
}
