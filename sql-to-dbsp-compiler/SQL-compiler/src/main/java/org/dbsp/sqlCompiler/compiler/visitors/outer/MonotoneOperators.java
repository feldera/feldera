package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneClosure;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneValue;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.ScalarProjection;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.TupleProjection;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.ValueProjection;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Detect almost "monotone" columns.  A column is almost monotone
 * if it is a monotone function applied to almost monotone columns. */
public class MonotoneOperators extends CircuitVisitor {
    /** For each operator the list of its output monotone columns. */
    public final Map<DBSPOperator, MonotoneValue> operatorMonotoneValue;

    @Nullable
    public MonotoneValue getMonotoneValue(DBSPOperator operator) {
        return this.operatorMonotoneValue.get(operator);
    }

    public MonotoneOperators(IErrorReporter errorReporter) {
        super(errorReporter);
        operatorMonotoneValue = new HashMap<>();
    }

    void set(DBSPOperator operator, @Nullable MonotoneValue value) {
        if (value == null || value.isEmpty())
            return;
        assert value.is(MonotoneClosure.class): "Expected a MonotoneClosure, got " + value;
        Logger.INSTANCE.belowLevel(this, 2)
                .append(operator.operation)
                .append(" => ")
                .append(value.toString())
                .newline();
        Utilities.putNew(this.operatorMonotoneValue, operator, value);
    }

    MonotoneValue identity(DBSPOperator operator, ValueProjection projection, boolean pairOfReferences) {
        DBSPType varType = projection.getType();
        DBSPExpression body;
        DBSPVariablePath var;
        if (pairOfReferences) {
            DBSPTypeTupleBase tpl = varType.to(DBSPTypeTupleBase.class);
            assert tpl.size() == 2: "Expected a pair, got " + varType;
            varType = new DBSPTypeRawTuple(tpl.tupFields[0].ref(), tpl.tupFields[1].ref());
            var = new DBSPVariablePath("t", varType);
            body = new DBSPRawTupleExpression(var.field(0).deref(), var.deepCopy().field(1).deref());
        } else {
            varType = varType.ref();
            var = new DBSPVariablePath("t", varType);
            body = var.deref();
        }
        DBSPClosureExpression closure = body.closure(var.asParameter());
        MonotoneFunctions analyzer = new MonotoneFunctions(
                this.errorReporter, operator, projection, pairOfReferences);
        return Objects.requireNonNull(analyzer.applyAnalysis(closure));
    }
    
    @Override
    public void postorder(DBSPSourceMultisetOperator node) {
        int index = 0;
        LinkedHashMap<Integer, ValueProjection> columns = new LinkedHashMap<>();
        for (InputColumnMetadata metadata: node.metadata.getColumns()) {
            if (metadata.lateness != null)
                columns.put(index, new ScalarProjection(metadata.type));
            index++;
        }
        if (columns.isEmpty())
            return;
        ValueProjection projection = new TupleProjection(
                node.getOutputZSetElementType().to(DBSPTypeTuple.class), columns);
        MonotoneValue result = this.identity(node, projection, false);
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPSourceMapOperator node) {
        int index = 0;
        int keyIndex = 0;
        LinkedHashMap<Integer, ValueProjection> keyColumns = new LinkedHashMap<>();
        LinkedHashMap<Integer, ValueProjection> valueColumns = new LinkedHashMap<>();
        DBSPTypeIndexedZSet type = node.getOutputIndexedZSetType();
        DBSPTypeTupleBase rowType = type.getKVType();
        DBSPVariablePath var = new DBSPVariablePath("t", rowType);
        DBSPExpression key = var.field(0);
        DBSPExpression value = var.field(1);
        List<DBSPExpression> keyFields = new ArrayList<>();
        List<DBSPExpression> valueFields = new ArrayList<>();
        for (InputColumnMetadata metadata: node.metadata.getColumns()) {
            if (metadata.lateness != null) {
                valueColumns.put(index, new ScalarProjection(metadata.type));
                valueFields.add(value.field(index));
                if (metadata.isPrimaryKey) {
                    keyColumns.put(keyIndex, new ScalarProjection(metadata.type));
                    keyFields.add(key.field(keyIndex));
                }
            }
            if (metadata.isPrimaryKey)
                keyIndex++;
            index++;
        }
        if (keyFields.isEmpty() && valueFields.isEmpty())
            return;
        ValueProjection keyProjection = new TupleProjection(
                rowType.tupFields[0].to(DBSPTypeTuple.class), keyColumns);
        ValueProjection valueProjection = new TupleProjection(
                rowType.tupFields[1].to(DBSPTypeTuple.class), valueColumns);
        LinkedHashMap<Integer, ValueProjection> pairs = new LinkedHashMap<>();
        pairs.put(0, keyProjection);
        pairs.put(1, valueProjection);
        ValueProjection pairProjection = new TupleProjection(rowType, pairs);
        MonotoneValue result = this.identity(node, pairProjection, true);
        this.set(node, result);
    }

    void identity(DBSPUnaryOperator node) {
        MonotoneValue input = this.getMonotoneValue(node.input());
        if (input == null)
            return;
        ValueProjection projection = input.getProjection();
        boolean pairOfReferences = node.getType().is(DBSPTypeIndexedZSet.class);
        MonotoneValue output = this.identity(node, projection, pairOfReferences);
        this.set(node, output);
    }

    @Override
    public void postorder(DBSPIndexOperator node) {
        MonotoneValue inputValue = this.getMonotoneValue(node.input());
        if (inputValue == null)
            return;

        DBSPExpression function = node.getFunction();
        MonotoneFunctions mm = new MonotoneFunctions(this
                .errorReporter, node, inputValue.getProjection(), false);
        MonotoneValue result = mm.applyAnalysis(function);
        if (result == null)
            return;
        this.set(node, result);
    }

    // TODO map_index

    @Override
    public void postorder(DBSPMapOperator node) {
        MonotoneValue inputValue = this.getMonotoneValue(node.input());
        if (inputValue == null)
            return;
        ValueProjection inputProjection = inputValue.getProjection();
        boolean pairOfReferences = node.input().getType().is(DBSPTypeIndexedZSet.class);
        MonotoneFunctions mm = new MonotoneFunctions(
                this.errorReporter, node, inputProjection, pairOfReferences);
        MonotoneValue result = mm.applyAnalysis(node.getFunction());
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPDeindexOperator node) {
        MonotoneValue input = this.getMonotoneValue(node.inputs.get(0));
        if (input == null)
            return;
        MonotoneTuple tuple = input.to(MonotoneTuple.class);
        MonotoneValue value = tuple.field(1);
        if (value == null)
            return;
        this.set(node, value);
    }

    @Override
    public void postorder(DBSPIntegrateOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPDelayedIntegralOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPDistinctOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPNoopOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPDifferentiateOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPUpsertFeedbackOperator node) {
        // TODO
        // this.identity(node);
    }

    @Override
    public void postorder(DBSPFilterOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPPrimitiveAggregateOperator node) {
        // Input type is IndexedZSet<key, tuple>
        // Output type is IndexedZSet(key, aggregateType)
        MonotoneValue inputValue = this.getMonotoneValue(node.inputs.get(0));
        if (inputValue == null)
            return;
        ValueProjection projection = inputValue.getProjection();
        TupleProjection tuple = projection.to(TupleProjection.class);
        ValueProjection tuple0 = tuple.field(0);
        // Drop field 1 of the value projection.
        if (tuple0 == null || tuple0.isEmpty())
            return;

        DBSPTypeIndexedZSet ix = node.getOutputIndexedZSetType();
        DBSPTypeTupleBase outputValueType = ix.getKVType();

        assert tuple0.getType().sameType(outputValueType.tupFields[0]) :
                "Types differ " + tuple0.getType() + " and " + outputValueType.tupFields[0];
        DBSPTypeRawTuple varType = projection.getType().to(DBSPTypeRawTuple.class);
        assert varType.size() == 2 : "Expected a pair, got " + varType;
        varType = new DBSPTypeRawTuple(varType.tupFields[0].ref(), varType.tupFields[1].ref());
        DBSPVariablePath var = new DBSPVariablePath("t", varType);
        DBSPExpression body = new DBSPTupleExpression(var.field(0).deref(), new DBSPTupleExpression());
        DBSPClosureExpression closure = body.closure(var.asParameter());
        MonotoneFunctions analyzer = new MonotoneFunctions(
                this.errorReporter, node, projection, true);
        MonotoneValue result = analyzer.applyAnalysis(closure);
        this.set(node, Objects.requireNonNull(result));
    }
}
