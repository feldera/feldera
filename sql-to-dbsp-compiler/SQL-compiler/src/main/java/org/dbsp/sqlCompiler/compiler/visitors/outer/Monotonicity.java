package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneClosureType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Outer visitor for monotonicity dataflow analysis.
 * Detect almost "monotone" columns.  A column is almost monotone
 * if it is a monotone function applied to almost monotone columns. */
public class Monotonicity extends CircuitVisitor {
    /** For each operator the list of its output monotone columns. */
    public final Map<DBSPOperator, MonotoneExpression> monotonicity;

    @Nullable
    public MonotoneExpression getMonotoneExpression(DBSPOperator operator) {
        return this.monotonicity.get(operator);
    }

    public Monotonicity(IErrorReporter errorReporter) {
        super(errorReporter);
        monotonicity = new HashMap<>();
    }

    void set(DBSPOperator operator, @Nullable MonotoneExpression value) {
        if (value == null || !value.mayBeMonotone())
            return;
        Logger.INSTANCE.belowLevel(this, 2)
                .append(operator.operation)
                .append(" ")
                .append(operator.getIdString())
                .append(" => ")
                .append(value.toString())
                .newline();
        Utilities.putNew(this.monotonicity, operator, value);
    }

    @Nullable
    MonotoneExpression identity(DBSPOperator operator, IMaybeMonotoneType projection, boolean pairOfReferences) {
        if (!projection.mayBeMonotone())
            return null;
        DBSPType varType = projection.getType();
        DBSPExpression body;
        DBSPVariablePath var;
        if (pairOfReferences) {
            DBSPTypeRawTuple tpl = varType.to(DBSPTypeRawTuple.class);
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
        // Invoke inner visitor.
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.errorReporter, operator, projection, pairOfReferences);
        return Objects.requireNonNull(analyzer.applyAnalysis(closure));
    }
    
    @Override
    public void postorder(DBSPSourceMultisetOperator node) {
        List<IMaybeMonotoneType> fields = new ArrayList<>();
        for (InputColumnMetadata metadata: node.metadata.getColumns()) {
            IMaybeMonotoneType columnType = new NonMonotoneType(metadata.type);
            if (metadata.lateness != null)
                columnType = new MonotoneType(metadata.type);
            fields.add(columnType);
        }
        IMaybeMonotoneType projection = new PartiallyMonotoneTuple(fields, false);
        MonotoneExpression result = this.identity(node, projection, false);
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPViewOperator node) {
        // If the view has LATENESS declarations, we use these.
        // Otherwise, we treat it as an identity function.
        // We could do better: merge the input with the declared lateness.
        // This is still TODO.
        DBSPTypeTuple tuple = node.getOutputZSetElementType().as(DBSPTypeTuple.class);
        if (tuple == null) {
            // This must be an ORDER BY node
            return;
        }

        if (node.hasLateness()) {
            // Trust the annotations, and forget what we know about the input.
            // This code parallels DBSPSourceMultisetOperator
            List<IMaybeMonotoneType> fields = new ArrayList<>();
            for (ViewColumnMetadata metadata: node.metadata) {
                IMaybeMonotoneType columnType = new NonMonotoneType(metadata.getType());
                if (metadata.lateness != null)
                    columnType = new MonotoneType(metadata.getType());
                fields.add(columnType);
            }
            IMaybeMonotoneType projection = new PartiallyMonotoneTuple(fields, false);
            MonotoneExpression result = this.identity(node, projection, false);
            this.set(node, result);
        } else {
            // Treat this like an identity function.
            this.identity(node);
        }

        /*
        This is an attempt to blend the two monotonicities, but the other passes
        are confused by this result.  The main problem is that there is an output
        which is monotone which is extracted out of "thin air", since the corresponding
        tuple input field is not monotone.

        DBSPVariablePath var = new DBSPVariablePath("t", tuple);
        PartiallyMonotoneTuple monotoneInput = null;
        if (inputFunction != null)
            monotoneInput = getBodyType(inputFunction).to(PartiallyMonotoneTuple.class);

        MonotoneExpression result;
        List<IMaybeMonotoneType> fieldTypes = new ArrayList<>();
        List<DBSPExpression> allFields = new ArrayList<>();
        List<DBSPExpression> monotoneFields = new ArrayList<>();

        int index = 0;
        for (DBSPTypeStruct.Field field: struct.fields.values()) {
            // Iterate over struct so we can get field names.
            DBSPType type = tuple.getFieldType(index);
            boolean isMonotone = monotoneInput != null && monotoneInput.getField(index).mayBeMonotone();
            DBSPExpression lateness = null;
            for (ViewColumnMetadata meta: node.metadata) {
                if (meta.columnName.equalsIgnoreCase(field.name)) {
                    lateness = meta.getLateness();
                }
            }
            if (lateness != null)
                isMonotone = true;
            IMaybeMonotoneType fieldType;
            DBSPExpression expression = null;
            if (isMonotone) {
                fieldType = new MonotoneType(type);
                expression = var.field(index);
                monotoneFields.add(expression);
            } else {
                fieldType = new NonMonotoneType(type);
            }
            allFields.add(var.field(index));
            fieldTypes.add(fieldType);
            index++;
        }

        DBSPParameter param = var.asParameter();
        result = new MonotoneExpression(
                new DBSPTupleExpression(allFields, false).closure(param),
                new MonotoneClosureType(new PartiallyMonotoneTuple(fieldTypes, false), param, param),
                new DBSPTupleExpression(monotoneFields, false).closure(param));
        if (result.mayBeMonotone())
            this.set(node, result);
         */
    }

    @Override
    public void postorder(DBSPSourceMapOperator node) {
        List<IMaybeMonotoneType> keyColumns = new ArrayList<>();
        List<IMaybeMonotoneType> valueColumns = new ArrayList<>();
        for (InputColumnMetadata metadata: node.metadata.getColumns()) {
            IMaybeMonotoneType columnType = new NonMonotoneType(metadata.type);
            if (metadata.lateness != null) {
                columnType = new MonotoneType(metadata.type);
            }
            if (metadata.isPrimaryKey) {
                keyColumns.add(columnType);
            }
            valueColumns.add(columnType);
        }
        IMaybeMonotoneType keyProjection = new PartiallyMonotoneTuple(keyColumns, false);
        IMaybeMonotoneType valueProjection = new PartiallyMonotoneTuple(valueColumns, false);
        IMaybeMonotoneType pairProjection = new PartiallyMonotoneTuple(
                Linq.list(keyProjection, valueProjection), false);
        MonotoneExpression result = this.identity(node, pairProjection, true);
        this.set(node, result);
    }

    void identity(DBSPUnaryOperator node) {
        MonotoneExpression input = this.getMonotoneExpression(node.input());
        if (input == null)
            return;
        boolean pairOfReferences = node.getType().is(DBSPTypeIndexedZSet.class);
        MonotoneExpression output = this.identity(node, getBodyType(input), pairOfReferences);
        this.set(node, output);
    }

    /** Given an expression that is expected to represent a closure,
     * get the monotonicity information of its body. */
    static IMaybeMonotoneType getBodyType(MonotoneExpression expression) {
        MonotoneClosureType closure = expression.getMonotoneType().to(MonotoneClosureType.class);
        return closure.getBodyType();
    }

    @Override
    public void postorder(DBSPMapIndexOperator node) {
        MonotoneExpression inputFunction = this.getMonotoneExpression(node.input());
        if (inputFunction == null)
            return;

        DBSPExpression function = node.getFunction();
        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(this
                .errorReporter, node, getBodyType(inputFunction), false);
        MonotoneExpression result = mm.applyAnalysis(function);
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPMapOperator node) {
        MonotoneExpression inputFunction = this.getMonotoneExpression(node.input());
        if (inputFunction == null)
            return;
        boolean pairOfReferences = node.input().getType().is(DBSPTypeIndexedZSet.class);
        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(
                this.errorReporter, node, getBodyType(inputFunction), pairOfReferences);
        MonotoneExpression result = mm.applyAnalysis(node.getFunction());
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPDeindexOperator node) {
        MonotoneExpression inputFunction = this.getMonotoneExpression(node.input());
        if (inputFunction == null)
            return;
        PartiallyMonotoneTuple tuple = inputFunction.getMonotoneType().to(PartiallyMonotoneTuple.class);
        IMaybeMonotoneType value = tuple.getField(1);
        if (!value.mayBeMonotone())
            return;
        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(
                this.errorReporter, node, getBodyType(inputFunction), true);
        MonotoneExpression result = mm.applyAnalysis(node.getFunction());
        if (result == null)
            return;
        this.set(node, result);
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

    public void aggregate(DBSPOperator node) {
        // Input type is IndexedZSet<key, tuple>
        // Output type is IndexedZSet(key, aggregateType)
        MonotoneExpression inputValue = this.getMonotoneExpression(node.inputs.get(0));
        if (inputValue == null)
            return;
        IMaybeMonotoneType projection = Monotonicity.getBodyType(inputValue);
        PartiallyMonotoneTuple tuple = projection.to(PartiallyMonotoneTuple.class);
        IMaybeMonotoneType tuple0 = tuple.getField(0);
        // Drop field 1 of the value projection.
        if (!tuple0.mayBeMonotone())
            return;

        DBSPTypeIndexedZSet ix = node.getOutputIndexedZSetType();
        DBSPTypeTupleBase outputValueType = ix.getKVType();

        assert tuple0.getType().sameType(outputValueType.tupFields[0]) :
                "Types differ " + tuple0.getType() + " and " + outputValueType.tupFields[0];
        DBSPTypeTupleBase varType = projection.getType().to(DBSPTypeTupleBase.class);
        assert varType.size() == 2 : "Expected a pair, got " + varType;
        varType = new DBSPTypeRawTuple(varType.tupFields[0].ref(), varType.tupFields[1].ref());
        DBSPVariablePath var = new DBSPVariablePath("t", varType);
        DBSPExpression body = new DBSPRawTupleExpression(var.field(0).deref(), new DBSPTupleExpression());
        DBSPClosureExpression closure = body.closure(var.asParameter());
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.errorReporter, node, projection, true);
        MonotoneExpression result = analyzer.applyAnalysis(closure);
        this.set(node, Objects.requireNonNull(result));
    }

    @Override
    public void postorder(DBSPPrimitiveAggregateOperator node) {
        this.aggregate(node);
    }
}
