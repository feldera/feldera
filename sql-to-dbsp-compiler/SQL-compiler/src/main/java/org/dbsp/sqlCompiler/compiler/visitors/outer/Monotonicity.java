package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctIncrementalOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneClosureType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Outer visitor for monotonicity dataflow analysis.
 * Detect almost "monotone" columns.  A column is almost monotone
 * if it is a monotone function applied to almost monotone columns.
 * Monotonicity is computed on the expanded operator graph.
 * See the ExpandOperators class. */
public class Monotonicity extends CircuitVisitor {
    public static class MonotonicityInformation {
        /** For each operator the list of its output monotone columns. */
        final Map<DBSPOperator, MonotoneExpression> monotonicity;
        /** List of operators in the expanded graph. */
        final Set<DBSPOperator> expandedGraph;

        MonotonicityInformation() {
            this.monotonicity = new HashMap<>();
            this.expandedGraph = new HashSet<>();
        }

        void setCircuit(DBSPCircuit circuit) {
            for (DBSPOperator op: circuit.circuit.getAllOperators())
                this.expandedGraph.add(op);
        }

        void put(DBSPOperator operator, MonotoneExpression expression) {
            assert this.expandedGraph.contains(operator);
            Utilities.putNew(this.monotonicity, operator, expression);
        }

        @Nullable
        MonotoneExpression get(DBSPOperator operator) {
            if (!this.expandedGraph.contains(operator))
                throw new InternalCompilerError("Querying operator that is not in the expanded graph " + operator);
            return this.monotonicity.get(operator);
        }
    }

    MonotonicityInformation info;

    @Nullable
    public MonotoneExpression getMonotoneExpression(DBSPOperator operator) {
        return this.info.get(operator);
    }

    public Monotonicity(IErrorReporter errorReporter) {
        super(errorReporter);
        this.info = new MonotonicityInformation();
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
        this.info.put(operator, value);
    }

    @Nullable
    MonotoneExpression identity(DBSPOperator operator, IMaybeMonotoneType projection, boolean pairOfReferences) {
        if (!projection.mayBeMonotone())
            return null;
        DBSPType varType = projection.getType();
        DBSPExpression body;
        DBSPVariablePath var;
        MonotoneTransferFunctions.ArgumentKind argumentType;
        if (pairOfReferences) {
            DBSPTypeRawTuple tpl = varType.to(DBSPTypeRawTuple.class);
            assert tpl.size() == 2: "Expected a pair, got " + varType;
            varType = new DBSPTypeRawTuple(tpl.tupFields[0].ref(), tpl.tupFields[1].ref());
            var = varType.var();
            body = new DBSPRawTupleExpression(var.field(0).deref(), var.deepCopy().field(1).deref());
            argumentType = MonotoneTransferFunctions.ArgumentKind.IndexedZSet;
        } else {
            varType = varType.ref();
            var = varType.var();
            body = var.deref();
            argumentType = MonotoneTransferFunctions.ArgumentKind.ZSet;
        }
        DBSPClosureExpression closure = body.closure(var.asParameter());
        // Invoke inner visitor.
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.errorReporter, operator, argumentType, projection);
        return Objects.requireNonNull(analyzer.applyAnalysis(closure));
    }

    @Override
    public void postorder(DBSPHopOperator node) {
        MonotoneExpression input = this.getMonotoneExpression(node.input());
        if (input == null)
            return;
        if (!input.mayBeMonotone())
            return;
        IMaybeMonotoneType projection = getBodyType(input);
        DBSPTypeTuple varType = projection.getType().to(DBSPTypeTuple.class);

        DBSPVariablePath var = new DBSPVariablePath(varType.ref());
        DBSPExpression[] fields = new DBSPExpression[varType.size() + 2];
        DBSPType timestampType = varType.tupFields[node.timestampIndex];
        for (int i = 0; i < varType.size(); i++) {
            fields[i] = var.deepCopy().deref().field(i);
        }
        fields[varType.size()] = new DBSPApplyExpression("hop_start_timestamp",  timestampType,
                fields[node.timestampIndex].deepCopy(), node.interval, node.size, node.start);
        fields[varType.size() + 1] = fields[varType.size()].deepCopy();
        DBSPExpression body = new DBSPTupleExpression(fields);
        DBSPClosureExpression closure = body.closure(var.asParameter());
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.errorReporter, node, MonotoneTransferFunctions.ArgumentKind.ZSet, projection);
        MonotoneExpression result = analyzer.applyAnalysis(closure);
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator node) {
        List<IMaybeMonotoneType> fields = new ArrayList<>();
        for (InputColumnMetadata metadata: node.metadata.getColumns()) {
            IMaybeMonotoneType columnType = NonMonotoneType.nonMonotone(metadata.type);
            if (metadata.lateness != null)
                columnType = new MonotoneType(metadata.type);
            fields.add(columnType);
        }
        IMaybeMonotoneType projection = new PartiallyMonotoneTuple(fields, false, false);
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
            for (ViewColumnMetadata metadata: node.metadata.columns) {
                IMaybeMonotoneType columnType = new NonMonotoneType(metadata.getType());
                if (metadata.lateness != null)
                    columnType = new MonotoneType(metadata.getType());
                fields.add(columnType);
            }
            IMaybeMonotoneType projection = new PartiallyMonotoneTuple(fields, false, false);
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
            IMaybeMonotoneType columnType = NonMonotoneType.nonMonotone(metadata.type);
            if (metadata.lateness != null) {
                columnType = new MonotoneType(metadata.type);
            }
            if (metadata.isPrimaryKey) {
                keyColumns.add(columnType);
            }
            valueColumns.add(columnType);
        }
        IMaybeMonotoneType keyProjection = new PartiallyMonotoneTuple(keyColumns, false, false);
        IMaybeMonotoneType valueProjection = new PartiallyMonotoneTuple(valueColumns, false, false);
        IMaybeMonotoneType pairProjection = new PartiallyMonotoneTuple(
                Linq.list(keyProjection, valueProjection), false, false);
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

        IMaybeMonotoneType sourceType = getBodyType(inputFunction);
        DBSPExpression function = node.getFunction();
        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(this
                .errorReporter, node,
                MonotoneTransferFunctions.ArgumentKind.fromType(node.input().outputType),
                sourceType);
        MonotoneExpression result = mm.applyAnalysis(function);
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator node) {
        MonotoneExpression left = this.getMonotoneExpression(node.left());
        MonotoneExpression right = this.getMonotoneExpression(node.right());
        if (left == null && right == null)
            return;
        PartiallyMonotoneTuple leftType;
        if (left != null) {
            leftType = getBodyType(left).to(PartiallyMonotoneTuple.class);
        } else {
            DBSPType source = node.left().getOutputIndexedZSetType().getKVType();
            leftType = NonMonotoneType.nonMonotone(source).to(PartiallyMonotoneTuple.class);
        }
        PartiallyMonotoneTuple rightType;
        if (right != null) {
            rightType = getBodyType(right).to(PartiallyMonotoneTuple.class);
        } else {
            DBSPType source = node.right().getOutputIndexedZSetType().getKVType();
            rightType = NonMonotoneType.nonMonotone(source).to(PartiallyMonotoneTuple.class);
        }

        IMaybeMonotoneType leftKeyType = leftType.getField(0);
        IMaybeMonotoneType rightKeyType = rightType.getField(0);
        IMaybeMonotoneType leftValueType = leftType.getField(1);
        IMaybeMonotoneType rightValueType = rightType.getField(1);

        IMaybeMonotoneType keyType = leftKeyType.union(rightKeyType);
        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(
                this.errorReporter, node,
                MonotoneTransferFunctions.ArgumentKind.Join,
                keyType, leftValueType, rightValueType);
        MonotoneExpression result = mm.applyAnalysis(node.getFunction());
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPMapOperator node) {
        MonotoneExpression inputFunction = this.getMonotoneExpression(node.input());
        if (inputFunction == null)
            return;
        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(
                this.errorReporter, node,
                MonotoneTransferFunctions.ArgumentKind.fromType(node.input().getType()),
                getBodyType(inputFunction));
        MonotoneExpression result = mm.applyAnalysis(node.getFunction());
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPSumOperator node) {
        List<MonotoneExpression> inputFunctions = Linq.map(node.inputs, this::getMonotoneExpression);
        // All the inputs must be monotone for the result to have a chance of being monotone
        if (inputFunctions.contains(null))
            return;
        if (Linq.any(inputFunctions, f -> !f.mayBeMonotone()))
            return;

        List<IMaybeMonotoneType> types = Linq.map(inputFunctions, Monotonicity::getBodyType);
        IMaybeMonotoneType resultType = IMaybeMonotoneType.intersection(types);
        if (!resultType.mayBeMonotone())
            return;

        boolean pairOfReferences = node.getType().is(DBSPTypeIndexedZSet.class);
        // This function is not exactly correct, since it has a single input,
        // whereas sum has N inputs, but it will only be used for its result type.
        MonotoneExpression result = this.identity(node, resultType, pairOfReferences);
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
                this.errorReporter, node,
                MonotoneTransferFunctions.ArgumentKind.IndexedZSet, getBodyType(inputFunction));
        MonotoneExpression result = mm.applyAnalysis(node.getFunction());
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPDistinctOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPDistinctIncrementalOperator node) {
        // The right input is the delta.
        MonotoneExpression input = this.getMonotoneExpression(node.right());
        if (input == null)
            return;
        boolean pairOfReferences = node.getType().is(DBSPTypeIndexedZSet.class);
        MonotoneExpression output = this.identity(node, getBodyType(input), pairOfReferences);
        this.set(node, output);
    }

    @Override
    public void postorder(DBSPNoopOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPFilterOperator node) {
        this.identity(node);
    }

    /** Create a NoExpression with the specified type.
     * NoExpressions are never monotone, so we can use them as placeholders for
     * expressions that we do not want to look at. */
    static DBSPExpression makeNoExpression(DBSPType type) {
        if (type.is(DBSPTypeBaseType.class) || type.is(DBSPTypeVec.class) || type.is(DBSPTypeMap.class)) {
            return new NoExpression(type);
        } else if (type.is(DBSPTypeTupleBase.class)) {
            DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
            DBSPExpression[] fields = Linq.map(tuple.tupFields, Monotonicity::makeNoExpression, DBSPExpression.class);
            return tuple.makeTuple(fields);
        } else if (type.is(DBSPTypeRef.class)) {
            return makeNoExpression(type.ref()).borrow();
        } else {
            throw new UnimplementedException(type.getNode());
        }
    }

    @Override
    public void postorder(DBSPPrimitiveAggregateOperator node) {
        // Input type is IndexedZSet<key, tuple>
        // Output type is IndexedZSet<key, aggregateType>
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
        DBSPVariablePath var = varType.var();
        DBSPExpression body = new DBSPRawTupleExpression(
                var.field(0).deref(),
                makeNoExpression(ix.elementType));
        DBSPClosureExpression closure = body.closure(var.asParameter());
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.errorReporter, node, MonotoneTransferFunctions.ArgumentKind.IndexedZSet, projection);
        MonotoneExpression result = analyzer.applyAnalysis(closure);
        this.set(node, Objects.requireNonNull(result));
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator node) {
        // Input type is IndexedZSet<timestamp, tuple>
        // Output type is IndexedZSet<partition, Tup2<timestamp, aggregateType>>
        // If the input timestamp is monotone, the output timestamp is too.
        MonotoneExpression inputValue = this.getMonotoneExpression(node.input());
        if (inputValue == null)
            return;
        IMaybeMonotoneType inputProjection = Monotonicity.getBodyType(inputValue);
        PartiallyMonotoneTuple tuple = inputProjection.to(PartiallyMonotoneTuple.class);
        IMaybeMonotoneType timestamp = tuple.getField(0);
        if (!timestamp.mayBeMonotone())
            return;

        DBSPType timestampType = timestamp.getType();
        DBSPTypeIndexedZSet ix = node.getOutputIndexedZSetType();
        DBSPTypeTupleBase outputValueType = ix.elementType.to(DBSPTypeTupleBase.class);

        assert timestamp.getType().sameType(outputValueType.tupFields[0]) :
                "Types differ " + timestampType + " and " + outputValueType.tupFields[0];
        DBSPTypeTupleBase varType = inputProjection.getType().to(DBSPTypeTupleBase.class);
        assert varType.size() == 2 : "Expected a pair, got " + varType;
        varType = new DBSPTypeRawTuple(varType.tupFields[0].ref(), varType.tupFields[1].ref());
        DBSPVariablePath var = varType.var();
        DBSPExpression lowerBound = ExpressionCompiler.makeBinaryExpression(node.getNode(),
                timestampType, DBSPOpcode.SUB, var.field(0).deref(), node.lower.representation);

        DBSPExpression body =
                new DBSPRawTupleExpression(
                        makeNoExpression(ix.keyType),
                        new DBSPTupleExpression(
                                lowerBound,
                                makeNoExpression(outputValueType.tupFields[1]).some()));
        DBSPClosureExpression closure = body.closure(var.asParameter());
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.errorReporter, node, MonotoneTransferFunctions.ArgumentKind.IndexedZSet, inputProjection);
        MonotoneExpression result = analyzer.applyAnalysis(closure);
        this.set(node, Objects.requireNonNull(result));
    }

    @Override
    public void startVisit(IDBSPOuterNode node) {
        super.startVisit(node);
        this.info.setCircuit(node.to(DBSPCircuit.class));
    }
}
