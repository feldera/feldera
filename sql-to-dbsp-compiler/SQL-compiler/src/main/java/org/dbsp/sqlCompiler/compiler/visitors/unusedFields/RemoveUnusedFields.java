package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEqualityComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Analyze functions in operators and discover unused fields.
 * Rewrite such operators as a composition of a map followed by a version of the original operator.
 *
 * <p>An unused field is a field of an input parameter which does not affect the output of the function.
 * An example is f = |x| x.1.  Here x.0 is an unused fields.
 * Such functions are decomposed into two functions such that f = g(h),
 * where h is a projection which removes the unused field h = |x| x.1 in this case
 * and g is a compressed version of the function f, g = |x| x.0 in this case.
 * Most of this work is done by the {@link FindUsedFields} visitor.
 */
public class RemoveUnusedFields extends CircuitCloneVisitor {
    public final FindUsedFields find;
    final AnalyzedSet<DBSPOperator> operatorsAnalyzed;
    /** Functions analyzed before which have no unused fields.  An operator whose inputs
     * changed is a new operator with the same function, which needs no new analysis. */
    final AnalyzedSet<DBSPExpression> fullyUsedFunctions;
    private Map<Long, RustCircuitPass.MapInputLayout> nativeMapFields = Map.of();

    public RemoveUnusedFields(DBSPCompiler compiler, AnalyzedSet<DBSPOperator> operatorsAnalyzed) {
        super(compiler, false);
        this.find = new FindUsedFields(compiler);
        this.operatorsAnalyzed = operatorsAnalyzed;
        this.fullyUsedFunctions = new AnalyzedSet<>();
    }

    OutputPort getProjection(CalciteRelNode node, FieldUseMap fieldMap, OutputPort input) {
        OutputPort source = this.mapped(input);
        if (!fieldMap.hasUnusedFields())
            return source;

        DBSPType inputType = input.getOutputIndexedZSetType().getKVRefType();
        var columns = fieldMap.deref().getUsedFields();
        DBSPClosureExpression projection = joinInputProjection(node, inputType, columns);
        DBSPMapIndexOperator map = new DBSPMapIndexOperator(node, projection, source);
        this.addOperator(map);
        return map.outputPort();
    }

    static DBSPClosureExpression joinInputProjection(CalciteObject node, DBSPType inputType, List<Integer> columns) {
        DBSPVariablePath var = inputType.var();
        DBSPExpression field1 = var.field(1).deref();
        boolean nullable = field1.getType().mayBeNull;
        List<DBSPExpression> resultFields = Linq.map(columns,
                f -> field1.deepCopy().unwrapIfNullable(field1.getNode(), "Field should not be NULL")
                        .field(f).applyCloneIfNeeded());
        DBSPExpression rightSide = new DBSPTupleExpression(resultFields, nullable);
        if (nullable)
            rightSide = new DBSPIfExpression(node, field1.is_null(), rightSide.getType().none(), rightSide);

        DBSPRawTupleExpression raw = new DBSPRawTupleExpression(
                DBSPTupleExpression.flatten(var.field(0).deref()),
                rightSide);
        return raw.closure(var);
    }

    boolean done(DBSPSimpleOperator operator) {
        return this.operatorsAnalyzed.done(operator);
    }

    /** True if the operator or its function were analyzed before */
    boolean done(DBSPSimpleOperator operator, DBSPClosureExpression function) {
        return this.done(operator) || this.fullyUsedFunctions.contains(function);
    }

    /** Record that the analysis of 'function' found no unused fields */
    void noUnusedFields(DBSPClosureExpression function) {
        this.fullyUsedFunctions.done(function);
    }

    private void applyJoinProjection(DBSPSimpleOperator join, RustCircuitPass.JoinInputProjection plan) {
        var inputs = new ArrayList<OutputPort>(join.inputs.size());
        for (int i = 0; i < join.inputs.size(); i++) {
            var input = this.mapped(join.inputs.get(i));
            var projection = plan.inputs().get(i);
            if (projection != null) {
                var map = new DBSPMapIndexOperator(join.getRelNode(), projection, input);
                this.addOperator(map);
                input = map.outputPort();
            }
            inputs.add(input);
        }
        var reader = Objects.requireNonNull(plan.reader());
        if (join instanceof DBSPAsofJoinOperator asof) {
            this.map(asof, new DBSPAsofJoinOperator(asof.getRelNode(), asof.getOutputZSetType(), reader,
                    plan.timestamps().get(0), plan.timestamps().get(1), asof.comparator, asof.isLeft,
                    inputs.get(0), inputs.get(1)));
        } else {
            this.map(join, join.with(reader, join.outputType, inputs, false).to(DBSPSimpleOperator.class));
        }
    }

    boolean processJoin(DBSPSimpleOperator join) {
        var function = join.getClosureFunction();
        if (this.done(join, function))
            return false;
        if (this.compiler.options.ioOptions.rustOptimizerPasses) {
            var plan = RustCircuitPass.narrowJoinInputs(this.compiler, join);
            if (plan.reader() == null) {
                this.noUnusedFields(function);
                return false;
            }
            this.applyJoinProjection(join, plan);
            return true;
        }

        var uses = this.find.findUsedFields(function);
        var rewriter = uses.getFieldRewriter(this.compiler, 1);
        var rows = new ArrayList<FieldUseMap>(join.inputs.size());
        for (int i = 1; i < function.parameters.length; i++)
            rows.add(rewriter.getUseMap(function.parameters[i]));
        Utilities.enforce(rows.size() == join.inputs.size());
        if (rows.stream().noneMatch(row -> row.hasUnusedFields(1))) {
            this.noUnusedFields(function);
            return false;
        }
        var inputs = new ArrayList<OutputPort>(rows.size());
        for (int i = 0; i < rows.size(); i++)
            inputs.add(this.getProjection(join.getRelNode(), rows.get(i), join.inputs.get(i)));
        rewriter.parameterFullyUsed(function.parameters[0]);
        this.map(join, join.with(rewriter.rewriteClosure(function), join.outputType, inputs, false)
                .to(DBSPSimpleOperator.class));
        return true;
    }

    @Override
    public void postorder(DBSPAsofJoinOperator join) {
        boolean done = false;
        DBSPClosureExpression joinFunction = join.getClosureFunction();
        if (!this.done(join)) {
            Utilities.enforce(joinFunction.parameters.length == 3);
            DBSPParameter left = joinFunction.parameters[1];
            DBSPParameter right = joinFunction.parameters[2];
            if (this.compiler.options.ioOptions.rustOptimizerPasses) {
                var plan = RustCircuitPass.narrowJoinInputs(this.compiler, join);
                if (plan.reader() == null)
                    super.postorder(join);
                else
                    this.applyJoinProjection(join, plan);
                return;
            }
            var useMap = this.find.findUsedFields(join.fieldUseClosure());
            RewriteFields rw = useMap.getFieldRewriter(this.compiler, 1);
            FieldUseMap leftRemap = rw.getUseMap(left);
            FieldUseMap rightRemap = rw.getUseMap(right);
            if (leftRemap.hasUnusedFields(1) || rightRemap.hasUnusedFields(1)) {
                // Parameter 0 does not emit fields in the body of the function, leave it unchanged
                rw.parameterFullyUsed(joinFunction.parameters[0]);
                OutputPort leftMap = getProjection(join.getRelNode(), leftRemap, join.left());
                OutputPort rightMap = getProjection(join.getRelNode(), rightRemap, join.right());
                DBSPClosureExpression newJoinFunction = rw.rewriteClosure(joinFunction);
                int leftTimestampIndex = leftRemap.deref().getNewIndex(join.leftTimestampIndex);
                int rightTimestampIndex = rightRemap.deref().getNewIndex(join.rightTimestampIndex);
                DBSPSimpleOperator replacement =
                        new DBSPAsofJoinOperator(join.getRelNode(), join.getOutputZSetType(),
                                newJoinFunction, leftTimestampIndex, rightTimestampIndex,
                                join.comparator, join.isLeft, leftMap, rightMap);
                this.map(join, replacement);
                done = true;
            }
        }

        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPStarJoinOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPStarJoinIndexOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPJoinOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPLeftJoinOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPLeftJoinFilterMapOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator join) {
        boolean done = this.processJoin(join);
        if (!done)
            super.postorder(join);
    }

    /** Given an operator with an IndexedZSet input, project the value part
     * to keep only the specified fields */
    DBSPMapIndexOperator projectValue(DBSPUnaryOperator operator, FieldUseMap fm) {
        OutputPort source = this.mapped(operator.input());
        DBSPType inputType = operator.input().getOutputIndexedZSetType().getKVRefType();
        DBSPClosureExpression projection = this.compiler.options.ioOptions.rustOptimizerPasses
                ? RustCircuitPass.projectUsedFields(this.compiler, fm.getType(), fm.usageAtDepth(1), inputType)
                : indexedValueProjection(this.compiler, inputType, Objects.requireNonNull(fm.getProjection(1)));

        int size = operator.getType().getToplevelFieldCount();
        DBSPMapIndexOperator adjust = new DBSPMapIndexOperator(operator.getRelNode(), projection, source)
                .addAnnotation(new IsProjection(size), DBSPMapIndexOperator.class);
        this.addOperator(adjust);
        return adjust;
    }

    static DBSPClosureExpression indexedValueProjection(DBSPCompiler compiler, DBSPType input,
            DBSPClosureExpression projection) {
        DBSPVariablePath var = input.var();
        return new DBSPRawTupleExpression(
                ExpressionCompiler.expandTuple(var.getNode(), var.field(0).deref()),
                projection.call(var.field(1)))
                .reduce(compiler)
                .closure(var);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
        DBSPClosureExpression closure = operator.getClosureFunction();
        if (this.done(operator, closure)) {
            super.postorder(operator);
            return;
        }
        Utilities.enforce(closure.parameters.length == 1);
        var useMap = this.find.findUsedFields(closure);

        if (!useMap.hasUnusedFields(1)) {
            this.noUnusedFields(closure);
            super.postorder(operator);
            return;
        }
        // closure = compressed \circ projection
        RewriteFields rw = useMap.getFieldRewriter(this.compiler, 1);
        FieldUseMap fm = rw.getUseMap(closure.parameters[0]);
        DBSPClosureExpression compressed = rw.rewriteClosure(closure);
        if (EquivalenceContext.equiv(closure, compressed)) {
            // This optimization achieves nothing
            this.noUnusedFields(closure);
            super.postorder(operator);
            return;
        }

        DBSPMapIndexOperator adjust = this.projectValue(operator, fm);
        DBSPSimpleOperator result = new DBSPAggregateLinearPostprocessOperator(
                operator.getRelNode(), operator.getOutputIndexedZSetType(), compressed,
                operator.postProcess, adjust.outputPort());
        this.map(operator, result);
    }

    /** Mark in 'rowFields' the fields of the sorted row that the comparator reads.
     * @return false if the comparator is not a chain of field comparisons. */
    static boolean setComparedFieldsToUsed(DBSPComparatorExpression comparator, FieldUseMap rowFields) {
        while (!comparator.is(DBSPNoComparatorExpression.class)) {
            DBSPFieldComparatorExpression byField = comparator.as(DBSPFieldComparatorExpression.class);
            if (byField == null)
                return false;
            rowFields.setUsed(byField.fieldNo);
            comparator = byField.source;
        }
        return true;
    }

    /** Rebuild a chain of field comparisons over the row type with the unused fields removed. */
    static DBSPComparatorExpression remapComparedFields(
            DBSPComparatorExpression comparator, FieldUseMap rowFields, DBSPType newRowType) {
        if (comparator.is(DBSPNoComparatorExpression.class))
            return new DBSPNoComparatorExpression(comparator.getNode(), newRowType);
        DBSPFieldComparatorExpression byField = comparator.to(DBSPFieldComparatorExpression.class);
        DBSPComparatorExpression source = remapComparedFields(byField.source, rowFields, newRowType);
        int newFieldNo = rowFields.getNewIndex(byField.fieldNo);
        return new DBSPFieldComparatorExpression(
                byField.getNode(), source, newFieldNo, byField.ascending, byField.nullsFirst);
    }

    private boolean markComparedFields(DBSPComparatorExpression rank, DBSPComparatorExpression ties,
                                       FieldUseMap fields) {
        if (!this.compiler.options.ioOptions.rustOptimizerPasses)
            return setComparedFieldsToUsed(rank, fields) && setComparedFieldsToUsed(ties, fields);
        var columns = RustCircuitPass.topKComparedFields(this.compiler, rank, ties);
        if (columns == null)
            return false;
        columns.forEach(fields::setUsed);
        return true;
    }

    @Override
    public void postorder(DBSPIndexedTopKOperator operator) {
        // The rank comparator, tie comparator and output producer all read the input row.
        DBSPComparatorExpression comparator = operator.getFunction().as(DBSPComparatorExpression.class);
        DBSPComparatorExpression equality = operator.equalityComparator.comparator;
        if (comparator == null || this.done(operator)) {
            super.postorder(operator);
            return;
        }

        DBSPClosureExpression producer = operator.outputProducer
                .ensureTree(this.compiler).to(DBSPClosureExpression.class);
        Utilities.enforce(producer.parameters.length == 2);
        ParameterFieldUse useMap = this.find.findUsedFields(producer);
        // The rank (parameter 0) is a scalar; mark it used
        useMap.get(producer.parameters[0]).setUsed();
        FieldUseMap rowUse = useMap.get(producer.parameters[1]);
        FieldUseMap rowFields = rowUse.deref();
        if (!this.markComparedFields(comparator, equality, rowFields) || !rowUse.hasUnusedFields(1)) {
            super.postorder(operator);
            return;
        }

        RewriteFields rw = useMap.getFieldRewriter(this.compiler, 1);
        DBSPClosureExpression newProducer = rw.rewriteClosure(producer);
        DBSPMapIndexOperator adjust = this.projectValue(operator, rowUse);
        DBSPType newRowType = adjust.getOutputIndexedZSetType().elementType;
        var orders = this.compiler.options.ioOptions.rustOptimizerPasses
                ? RustCircuitPass.projectTopKComparators(this.compiler, comparator, equality,
                        newRowType, rowFields.usageAtDepth(1))
                : Pair.of(remapComparedFields(comparator, rowFields, newRowType),
                        remapComparedFields(equality, rowFields, newRowType));
        DBSPEqualityComparatorExpression newEquality = new DBSPEqualityComparatorExpression(
                operator.equalityComparator.getNode(), orders.right);
        DBSPSimpleOperator result = new DBSPIndexedTopKOperator(
                operator.getRelNode(), operator.numbering, orders.left, operator.limit,
                newEquality, newProducer, adjust.outputPort())
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        DBSPFlatmap flatmap = operator.getFunction().as(DBSPFlatmap.class);
        if (flatmap == null) {
            // At this point in the compilation process we expect all FlatMapOperators to have a Flatmap expression.
            // Later there will be FlatMapOperators introduced to
            // represent chains including Filters.
            super.postorder(operator);
            return;
        }

        var narrowed = this.compiler.options.ioOptions.rustOptimizerPasses
                ? RustCircuitPass.trimFlatmapInput(this.compiler, flatmap)
                : this.trimFlatmapInput(flatmap);
        if (narrowed == null) {
            super.postorder(operator);
            return;
        }
        int width = operator.input().getOutputZSetElementType().to(DBSPTypeTuple.class).getToplevelFieldCount();
        var adjust = new DBSPMapOperator(operator.getRelNode(), narrowed.left, this.mapped(operator.input()))
                .addAnnotation(new IsProjection(width), DBSPSimpleOperator.class);
        this.addOperator(adjust);
        this.map(operator, new DBSPFlatMapOperator(operator.getRelNode(), narrowed.right,
                operator.getOutputZSetType(), adjust.outputPort()));
    }

    @Nullable
    private Pair<DBSPClosureExpression, DBSPFlatmap> trimFlatmapInput(DBSPFlatmap flatmap) {
        var uses = this.find.findUsedFields(flatmap.collectionExpression);
        var row = uses.get(flatmap.collectionExpression.parameters[0]).deref();
        flatmap.passthroughIndexes.forEach(row::setUsed);
        if (!row.hasUnusedFields(1))
            return null;
        var projection = Objects.requireNonNull(row.borrow().getProjection(1));
        var reader = uses.getFieldRewriter(this.compiler, 1).rewriteClosure(flatmap.collectionExpression);
        var indexes = Linq.map(flatmap.passthroughIndexes, row::getNewIndex);
        var replacement = new DBSPFlatmap(flatmap.getNode(),
                Objects.requireNonNull(row.compressedType(1)).to(DBSPTypeTuple.class),
                reader, indexes, flatmap.postProcess, flatmap.ordinalityIndexType, flatmap.shuffle);
        return Pair.of(projection, replacement);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        if (this.done(operator)) {
            super.postorder(operator);
            return;
        }
        var list = operator.aggregateList;
        if (list == null) {
            // This can only happen for ORDER BY when --ignoreOrder is not supplied
            super.postorder(operator);
            return;
        }

        Map<IAggregate, ParameterFieldUse> useMaps = new HashMap<>();
        FieldUseMap fum = new FieldUseMap(list.rowVar.getType(), false);
        // FindUnusedFields converts closures to trees, so it allocates new aggregates
        List<IAggregate> treeified = new ArrayList<>();
        for (IAggregate aggregate: list.aggregates) {
            FindUsedFields finder = new FindUsedFields(this.compiler);
            FindUsedFields.AggregateUseMap used = finder.computeUsedFields(aggregate);
            Utilities.putNew(useMaps, used.aggregate(), used.useMap());
            treeified.add(used.aggregate());
            fum = fum.reduce(used.useMap().get(used.parameter()));
        }

        if (!fum.hasUnusedFields(1)) {
            super.postorder(operator);
            return;
        }

        List<IAggregate> results = new ArrayList<>();
        for (IAggregate aggregate: treeified) {
            ParameterFieldUse useMap = Utilities.getExists(useMaps, aggregate);
            for (DBSPParameter param: aggregate.getRowVariableReferences()) {
                useMap.updateParameterValue(param, fum);
            }
            RewriteFields fieldRewriter = useMap.getFieldRewriter(this.compiler, 1);
            results.add(fieldRewriter.rewriteAggregate(aggregate));
        }

        DBSPVariablePath rowVar = new DBSPVariablePath(list.rowVar.variable,
                Objects.requireNonNull(fum.compressedType(1)));
        DBSPMapIndexOperator adjust = this.projectValue(operator, fum);
        DBSPAggregateList newList = new DBSPAggregateList(operator.aggregateList.getNode(), rowVar, results);
        DBSPSimpleOperator result = new DBSPStreamAggregateOperator(operator.getRelNode(), operator.getOutputIndexedZSetType(),
                null, newList, adjust.outputPort())
                .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        if (!(operator.getFunction() instanceof DBSPClosureExpression) || !this.splitMap(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        if (!this.splitMap(operator))
            super.postorder(operator);
    }

    private boolean splitMap(DBSPUnaryOperator map) {
        if (map.hasAnnotation(a -> a.is(IsProjection.class)) || this.done(map, map.getClosureFunction()))
            return false;
        var closure = map.getClosureFunction();
        boolean indexedInput = !map.input().outputType().is(DBSPTypeZSet.class);
        boolean indexedOutput = map instanceof DBSPMapIndexOperator;
        Pair<DBSPClosureExpression, DBSPClosureExpression> split;
        if (this.compiler.options.ioOptions.rustOptimizerPasses) {
            var layout = Utilities.getExists(this.nativeMapFields, closure.id);
            split = switch (layout.kind()) {
                case Keep -> null;
                case Project -> RustCircuitPass.splitMapInput(this.compiler, map, Objects.requireNonNull(layout.paths()));
                case Inspect -> RustCircuitPass.splitMapInput(this.compiler, map, null);
            };
        } else {
            split = this.splitMapClosure(closure, indexedInput, indexedOutput);
        }
        if (split == null) {
            this.noUnusedFields(closure);
            return false;
        }
        var source = this.mapped(map.input());
        DBSPSimpleOperator adjust = indexedInput
                ? new DBSPMapIndexOperator(map.getRelNode(), split.left, source)
                : new DBSPMapOperator(map.getRelNode(), split.left, source);
        if (indexedInput && indexedOutput)
            adjust.copyAnnotations(map);
        adjust.addAnnotation(new IsProjection(closure.getType().getToplevelFieldCount()), DBSPSimpleOperator.class);
        this.addOperator(adjust);
        DBSPSimpleOperator result = indexedOutput
                ? new DBSPMapIndexOperator(map.getRelNode(), split.right, map.getOutputIndexedZSetType(), adjust.outputPort())
                : new DBSPMapOperator(map.getRelNode(), split.right, map.getOutputZSetType(), adjust.outputPort());
        this.map(map, result);
        return true;
    }

    /** Keep the allocation order of each input and output stream combination. */
    @Nullable
    private Pair<DBSPClosureExpression, DBSPClosureExpression> splitMapClosure(
            DBSPClosureExpression closure, boolean indexedInput, boolean indexedOutput) {
        Utilities.enforce(closure.parameters.length == 1);
        var use = this.find.findUsedFields(closure);
        int depth = indexedInput ? 2 : 1;
        if (!use.hasUnusedFields(depth))
            return null;
        var rewriter = use.getFieldRewriter(this.compiler, depth);
        var fields = rewriter.getUseMap(closure.parameters[0]);
        DBSPClosureExpression projection;
        DBSPClosureExpression reader;
        if (indexedInput != indexedOutput) {
            reader = rewriter.rewriteClosure(closure);
            if (indexedInput && EquivalenceContext.equiv(closure, reader))
                return null;
            projection = Objects.requireNonNull(fields.getProjection(depth));
            if (indexedOutput && EquivalenceContext.equiv(reader, closure))
                return null;
        } else {
            projection = Objects.requireNonNull(fields.getProjection(depth));
            if (indexedInput && EquivalenceContext.equiv(closure, projection))
                return null;
            reader = rewriter.rewriteClosure(closure);
            if (!indexedInput && (EquivalenceContext.equiv(closure, projection)
                    || EquivalenceContext.equiv(closure, reader)))
                return null;
        }
        return Pair.of(projection, reader);
    }

    @Override
    public Token startVisit(IDBSPOuterNode circuit) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            this.nativeMapFields = RustCircuitPass.mapFieldDependencies(this.compiler, circuit.to(DBSPCircuit.class),
                    map -> map.getFunction() instanceof DBSPClosureExpression function
                            && !map.hasAnnotation(a -> a.is(IsProjection.class))
                            && !this.operatorsAnalyzed.contains(map) && !this.fullyUsedFunctions.contains(function));
        return super.startVisit(circuit);
    }
}
