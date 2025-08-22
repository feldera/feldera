package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctIncrementalOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneClosureType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.ToIndentableString;
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
 * Monotonicity is computed *on the expanded operator graph*.
 * See the ExpandOperators class. */
public class Monotonicity extends CircuitVisitor {
    public static class MonotonicityInformation {
        /** For each operator the list of its output monotone columns. */
        final Map<OutputPort, MonotoneExpression> monotonicity;
        /** List of operators in the expanded graph. */
        final Set<DBSPOperator> expandedGraph;

        MonotonicityInformation() {
            this.monotonicity = new HashMap<>();
            this.expandedGraph = new HashSet<>();
        }

        void markExpanded(DBSPOperator operator) {
            this.expandedGraph.add(operator);
            if (operator.is(DBSPNestedOperator.class)) {
                for (DBSPOperator op : operator.to(DBSPNestedOperator.class).getAllOperators())
                    this.markExpanded(op);
            }
        }

        void setCircuit(DBSPCircuit circuit) {
            for (DBSPOperator op : circuit.getAllOperators()) {
                this.markExpanded(op);
            }
        }

        void put(OutputPort operator, MonotoneExpression expression) {
            Utilities.enforce(this.expandedGraph.contains(operator.node()));
            Utilities.putNew(this.monotonicity, operator, expression);
        }

        @Nullable
        MonotoneExpression get(OutputPort port) {
            if (!this.expandedGraph.contains(port.node()))
                throw new InternalCompilerError("Querying operator that is not in the expanded graph " + port.node());
            return this.monotonicity.get(port);
        }

        @Nullable
        MonotoneExpression get(DBSPSimpleOperator operator) {
            return this.get(operator.outputPort());
        }
    }

    final MonotonicityInformation info;

    @Nullable
    public MonotoneExpression getMonotoneExpression(DBSPSimpleOperator operator) {
        return this.info.get(operator.outputPort());
    }

    @Nullable
    public MonotoneExpression getMonotoneExpression(OutputPort port) {
        if (port.node().is(DBSPSimpleOperator.class))
            return this.getMonotoneExpression(port.simpleNode());
        return null;
    }

    public Monotonicity(DBSPCompiler compiler) {
        super(compiler);
        this.info = new MonotonicityInformation();
    }

    void set(DBSPSimpleOperator operator, @Nullable MonotoneExpression value) {
        if (value == null || !value.mayBeMonotone())
            return;
        Logger.INSTANCE.belowLevel(this, 2)
                .append(operator.operation)
                .append(" ")
                .appendSupplier(operator::getIdString)
                .append(" => ")
                .appendSupplier(value::toString)
                .newline();
        this.info.put(operator.outputPort(), value);
    }

    @Nullable
    MonotoneExpression identity(DBSPSimpleOperator operator, IMaybeMonotoneType projection, boolean pairOfReferences) {
        if (!projection.mayBeMonotone())
            return null;
        DBSPType varType = projection.getType();
        DBSPExpression body;
        DBSPVariablePath var;
        MonotoneTransferFunctions.ArgumentKind argumentType;
        if (pairOfReferences) {
            DBSPTypeTupleBase tpl = varType.to(DBSPTypeTupleBase.class);
            Utilities.enforce(tpl.size() == 2, "Expected a pair, got " + varType);
            varType = tpl.makeRelatedTupleType(Linq.list(tpl.tupFields[0].ref(), tpl.tupFields[1].ref()));
            var = varType.var();
            body = tpl.makeTuple(var.field(0).deref(), var.deepCopy().field(1).deref());
            argumentType = MonotoneTransferFunctions.ArgumentKind.IndexedZSet;
        } else {
            varType = varType.ref();
            var = varType.var();
            body = var.deref();
            argumentType = MonotoneTransferFunctions.ArgumentKind.ZSet;
        }
        DBSPClosureExpression closure = body.closure(var);
        // Invoke inner visitor.
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.compiler(), operator, argumentType, projection);
        return Objects.requireNonNull(analyzer.applyAnalysis(closure));
    }

    @Override
    public void postorder(DBSPDelayedIntegralOperator integral) {
        // normally this operator is not monotone, but it is when applied to
        // the 'NOW' input (or an indexed version of it).
        boolean monotone = integral.hasAnnotation(a -> a.is(AlwaysMonotone.class));
        if (monotone) {
            this.identity(integral);
        }
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
        DBSPClosureExpression closure = body.closure(var);
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.compiler(), node, MonotoneTransferFunctions.ArgumentKind.ZSet, projection);
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
    public void postorder(DBSPSinkOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPViewOperator node) {
        // If the view has LATENESS declarations, we use these.
        // Otherwise, we treat it as an identity function.
        // We could do better: merge the input with the declared lateness.
        // This is still TODO: https://github.com/feldera/feldera/issues/1906
        DBSPTypeTuple tuple = node.getOutputZSetElementType().as(DBSPTypeTuple.class);
        if (tuple == null) {
            // This must be an ORDER BY node
            return;
        }

        if (node.hasLateness()) {
            Utilities.enforce(node.metadata.columns.size() == tuple.size());
            // Trust the annotations, and forget what we know about the input.
            // This code parallels DBSPSourceMultisetOperator
            List<IMaybeMonotoneType> fields = new ArrayList<>();
            int index = 0;
            for (ViewColumnMetadata metadata: node.metadata.columns) {
                DBSPType viewColType = tuple.getFieldType(index);
                index++;
                IMaybeMonotoneType columnType = NonMonotoneType.nonMonotone(viewColType);
                if (metadata.lateness != null)
                    columnType = new MonotoneType(viewColType);
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
                Linq.list(keyProjection, valueProjection), true, false);
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
                .compiler(), node,
                MonotoneTransferFunctions.ArgumentKind.fromType(node.input().outputType()),
                sourceType);
        MonotoneExpression result = mm.applyAnalysis(function);
        if (result == null)
            return;
        this.set(node, result);
    }

    public void processJoinBase(DBSPJoinBaseOperator node) {
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
                this.compiler(), node,
                MonotoneTransferFunctions.ArgumentKind.Join,
                keyType, leftValueType, rightValueType);
        MonotoneExpression result = mm.applyAnalysis(node.getFunction());
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator node) {
        this.processJoinBase(node);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator node) {
        this.processJoinBase(node);
    }

    @Override
    public void postorder(DBSPAntiJoinOperator node) {
        // Preserve monotonicity of left input
        MonotoneExpression input = this.getMonotoneExpression(node.left());
        if (input == null)
            return;
        MonotoneExpression output = this.identity(node, getBodyType(input), true);
        this.set(node, output);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator node) {
        // Preserve monotonicity of left input
        MonotoneExpression input = this.getMonotoneExpression(node.left());
        if (input == null)
            return;
        MonotoneExpression output = this.identity(node, getBodyType(input), true);
        this.set(node, output);
    }

    @Override
    public void postorder(DBSPFlatMapOperator node) {
        MonotoneExpression inputFunction = this.getMonotoneExpression(node.input());
        if (inputFunction == null)
            return;
        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(
                this.compiler(), node,
                MonotoneTransferFunctions.ArgumentKind.fromType(node.input().outputType()),
                getBodyType(inputFunction));
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
                this.compiler(), node,
                MonotoneTransferFunctions.ArgumentKind.fromType(node.input().outputType()),
                getBodyType(inputFunction));
        MonotoneExpression result = mm.applyAnalysis(node.getFunction());
        if (result == null)
            return;
        this.set(node, result);
    }

    void sumOrDifference(DBSPSimpleOperator node) {
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
    public void postorder(DBSPSumOperator node) {
        this.sumOrDifference(node);
    }

    @Override
    public void postorder(DBSPSubtractOperator node) {
        this.sumOrDifference(node);
    }

    @Override
    public void postorder(DBSPDeindexOperator node) {
        MonotoneExpression inputFunction = this.getMonotoneExpression(node.input());
        if (inputFunction == null)
            return;
        PartiallyMonotoneTuple tuple = getBodyType(inputFunction).to(PartiallyMonotoneTuple.class);
        IMaybeMonotoneType value = tuple.getField(1);
        if (!value.mayBeMonotone())
            return;
        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(
                this.compiler(), node,
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
    public void postorder(DBSPUpsertFeedbackOperator node) {
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
    public void postorder(DBSPNegateOperator node) {
        this.identity(node);
    }

    @Override
    public void postorder(DBSPWindowOperator node) {
        // Identity just for the left input
        MonotoneExpression input = this.getMonotoneExpression(node.left());
        if (input == null)
            return;
        boolean pairOfReferences = node.getType().is(DBSPTypeIndexedZSet.class);
        MonotoneExpression output = this.identity(node, getBodyType(input), pairOfReferences);
        this.set(node, output);
    }

    @Override
    public void postorder(DBSPLagOperator node) {
        MonotoneExpression input = this.getMonotoneExpression(node.input());
        if (input == null)
            return;
        IMaybeMonotoneType projection = Monotonicity.getBodyType(input);
        // The lag operator function has 2 inputs, although the operator has
        // a single input.  The first parameter is actually fed from the
        // operator's input.  The second parameter is fed from the lagged version of
        // the input.  Moreover, the parameter operates always over indexed
        // Z-sets, but the function's input is just the Z-set part.
        //
        // Let's say the function of lag is |x, y| f(x, y).
        // We build a new function transfer = |kx| (*kx.0, f(kx.1, No)) and analyze this one.
        DBSPClosureExpression function = node.getClosureFunction();
        Utilities.enforce(function.parameters.length == 2);

        DBSPTypeIndexedZSet inputType = node.input().getOutputIndexedZSetType();
        DBSPVariablePath kx = new DBSPTypeRawTuple(inputType.keyType.ref(), inputType.elementType.ref()).var();
        DBSPExpression noExpression = new NoExpression(function.parameters[1].type);
        DBSPExpression dataPart = function.call(kx.field(1), noExpression);
        DBSPExpression transfer = new DBSPRawTupleExpression(
                kx.field(0).deref(), dataPart).closure(kx).reduce(this.compiler())
                .ensureTree(this.compiler());
        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(
                this.compiler(), node,
                MonotoneTransferFunctions.ArgumentKind.IndexedZSet, projection);
        MonotoneExpression result = mm.applyAnalysis(transfer);
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPAsofJoinOperator node) {
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

        IMaybeMonotoneType leftValueMonoType = leftType.getField(1);
        IMaybeMonotoneType rightValueMonoType = rightType.getField(1);
        IMaybeMonotoneType leftKeyMonoType = leftType.getField(0);
        IMaybeMonotoneType rightKeyMonoType = rightType.getField(0);
        IMaybeMonotoneType keyMonoType = leftKeyMonoType.union(rightKeyMonoType);

        // We expect ASOF joins to look like projections.
        Projection projection = new Projection(this.compiler(), true);
        DBSPClosureExpression function = node.getClosureFunction();
        projection.apply(function);
        Utilities.enforce(projection.isProjection && projection.hasIoMap());
        Projection.IOMap ioMap = projection.getIoMap();
        DBSPTypeTupleBase keyType = node.getKeyType().to(DBSPTypeTupleBase.class);

        // We don't directly analyze the ASOF JOIN function; we make up
        // a function that is similar:
        // - we treat timestamp fields specially
        // - we use the type from the input, and not from the function parameter
        //   (the parameter fields may be nullable)
        DBSPVariablePath k = keyType.ref().var();
        DBSPVariablePath l = function.parameters[1].getType().var();
        DBSPVariablePath r = node.right().getOutputIndexedZSetType().elementType.ref().var();

        DBSPExpression leftTsField = l.deepCopy().deref().field(node.leftTimestampIndex);
        DBSPExpression rightTsField = r.deepCopy().deref().field(node.rightTimestampIndex);
        DBSPExpression min = ExpressionCompiler.makeBinaryExpression(node.getNode(), rightTsField.getType(),
                DBSPOpcode.MIN, leftTsField, rightTsField);

        List<DBSPExpression> fields = new ArrayList<>();
        for (var fai : ioMap.fields()) {
            int input = fai.inputIndex();
            int index = fai.fieldIndex();
            DBSPExpression expr;
            switch (input) {
                case 0:
                    expr = k.field(index);
                    break;
                case 1:
                    expr = l.deepCopy().deref().field(index);
                    if (index != node.leftTimestampIndex) {
                        // Since this is not a streaming join, we can't say anything about
                        // non-timestamp fields
                        expr = new NoExpression(expr.getType());
                    } else {
                        expr = min.deepCopy();
                    }
                    break;
                case 2:
                    expr = r.deepCopy().deref().field(index).castToNullable();
                    if (index != node.rightTimestampIndex) {
                        // Since this is not a streaming join, we can't say anything about
                        // non-timestamp fields
                        expr = new NoExpression(expr.getType());
                    } else {
                        expr = min.deepCopy().castToNullable();
                    }
                    break;
                default:
                    throw new InternalCompilerError("Unexpected input index " + input);
            }
            fields.add(expr);
        }
        DBSPTupleExpression tuple = new DBSPTupleExpression(fields, false);
        DBSPClosureExpression closure = tuple.closure(k, l, r);

        MonotoneTransferFunctions mm = new MonotoneTransferFunctions(
                this.compiler(), node,
                MonotoneTransferFunctions.ArgumentKind.Join,
                keyMonoType, leftValueMonoType, rightValueMonoType);
        MonotoneExpression result = mm.applyAnalysis(closure);
        if (result == null)
            return;
        this.set(node, result);
    }

    @Override
    public void postorder(DBSPFilterOperator node) {
        MonotoneExpression input = this.getMonotoneExpression(node.input());
        if (input == null)
            return;
        boolean pairOfReferences = node.getType().is(DBSPTypeIndexedZSet.class);
        if (pairOfReferences) {
            // TODO: if this is useful we should implement it
            this.identity(node);
            return;
        }

        // Find out if the filter condition is a conjunction of comparisons.
        // Extract all the comparisons of the form column >= expression.
        ComparisonsAnalyzer comparisons = new ComparisonsAnalyzer(node.getFunction());
        if (!comparisons.isEmpty()) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(node::toString)
                    .append(" comparisons: ")
                    .appendSupplier(comparisons::toString)
                    .newline();
        }

        IMaybeMonotoneType projection = getBodyType(input);
        if (!projection.mayBeMonotone())
            return;
        DBSPTypeTupleBase tuple = projection.getType().to(DBSPTypeTupleBase.class);
        DBSPVariablePath var = node.getClosureFunction().parameters[0].asVariable();
        DBSPExpression[] fields = new DBSPExpression[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
            DBSPExpression field = var.deepCopy().deref().field(i);
            List<DBSPExpression> comp = comparisons.getLowerBounds(i);
            for (DBSPExpression exp: comp) {
                field = ExpressionCompiler.makeBinaryExpression(node.getNode(),
                        field.getType(), DBSPOpcode.MAX, field, exp);
            }
            fields[i] = field;
        }
        DBSPClosureExpression closure = tuple.makeTuple(fields).closure(var);
        MonotoneTransferFunctions.ArgumentKind argumentType = MonotoneTransferFunctions.ArgumentKind.ZSet;
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.compiler(), node, argumentType, projection);
        MonotoneExpression output = Objects.requireNonNull(analyzer.applyAnalysis(closure));
        this.set(node, output);
    }

    /** Create a NoExpression with the specified type.
     * NoExpressions are never monotone, so we can use them as placeholders for
     * expressions that we do not want to look at.
     * *DOES NOT WORK PROPERLY FOR EMPTY TUPLES. */
    static DBSPExpression makeNoExpression(DBSPType type) {
        if (type.is(DBSPTypeBaseType.class) || type.is(DBSPTypeArray.class) || type.is(DBSPTypeMap.class)) {
            return new NoExpression(type);
        } else if (type.is(DBSPTypeTupleBase.class)) {
            // Tricky if the tuple is empty, this *will* be monotone!
            DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
            DBSPExpression[] fields = Linq.map(tuple.tupFields, Monotonicity::makeNoExpression, DBSPExpression.class);
            return tuple.makeTuple(fields);
        } else if (type.is(DBSPTypeRef.class)) {
            return makeNoExpression(type.ref()).borrow();
        } else {
            throw new InternalCompilerError("Monotonicity information for type " + type.asSqlString(),
                    type.getNode());
        }
    }

    public void aggregate(DBSPSimpleOperator node) {
        // Input type is IndexedZSet<key, tuple>
        // Output type is IndexedZSet<key, aggregateType>
        MonotoneExpression inputValue = this.getMonotoneExpression(node.inputs.get(0));
        if (inputValue == null)
            return;
        IMaybeMonotoneType projection = Monotonicity.getBodyType(inputValue);
        PartiallyMonotoneTuple tuple = projection.to(PartiallyMonotoneTuple.class);
        IMaybeMonotoneType tuple0 = tuple.getField(0);
        if (!tuple0.mayBeMonotone())
            return;

        DBSPTypeIndexedZSet ix = node.getOutputIndexedZSetType();
        DBSPTypeTupleBase outputValueType = ix.getKVType();

        Utilities.enforce(tuple0.getType().sameType(outputValueType.tupFields[0]),
                "Types differ " + tuple0.getType() + " and " + outputValueType.tupFields[0]);
        DBSPTypeTupleBase varType = projection.getType().to(DBSPTypeTupleBase.class);
        Utilities.enforce(varType.size() == 2, "Expected a pair, got " + varType);
        varType = new DBSPTypeRawTuple(varType.tupFields[0].ref(), varType.tupFields[1].ref());
        DBSPVariablePath var = varType.var();
        // Drop field 1 of the value projection.
        DBSPExpression body = new DBSPRawTupleExpression(
                var.field(0).deref(),
                makeNoExpression(ix.elementType));
        DBSPClosureExpression closure = body.closure(var);
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.compiler(), node, MonotoneTransferFunctions.ArgumentKind.IndexedZSet, projection);
        MonotoneExpression result = analyzer.applyAnalysis(closure);
        this.set(node, Objects.requireNonNull(result));
    }

    @Override
    public void postorder(DBSPPrimitiveAggregateOperator node) {
        this.aggregate(node);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator node) {
        this.aggregate(node);
    }

    @Override
    public void postorder(DBSPChainAggregateOperator node) {
        if (node.annotations.first(AlwaysMonotone.class) != null) {
            // This is the NOW operator; treat it as if it has a LATENESS of 0
            List<IMaybeMonotoneType> fields = new ArrayList<>();
            DBSPTypeIndexedZSet ix = node.getOutputIndexedZSetType();

            IMaybeMonotoneType key = new PartiallyMonotoneTuple(Linq.list(), false, false);
            DBSPTypeTuple tuple = ix.getElementTypeTuple();
            for (DBSPType fieldType: tuple.tupFields) {
                IMaybeMonotoneType columnType = new MonotoneType(fieldType);
                fields.add(columnType);
            }
            IMaybeMonotoneType value = new PartiallyMonotoneTuple(fields, false, false);
            IMaybeMonotoneType pair = new PartiallyMonotoneTuple(Linq.list(key, value), true, false);
            MonotoneExpression result = this.identity(node, pair, true);
            this.set(node, result);
            return;
        }

        // TODO: for MAX the output is always monotone, but we cannot use this information.
        // https://github.com/feldera/feldera/issues/2805
        this.aggregate(node);
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

        Utilities.enforce(timestamp.getType().sameType(outputValueType.tupFields[0]), "Types differ " + timestampType + " and " + outputValueType.tupFields[0]);
        DBSPTypeTupleBase varType = inputProjection.getType().to(DBSPTypeTupleBase.class);
        Utilities.enforce(varType.size() == 2, "Expected a pair, got " + varType);
        varType = new DBSPTypeRawTuple(varType.tupFields[0].ref(), varType.tupFields[1].ref());
        DBSPVariablePath var = varType.var();
        DBSPExpression lowerBound = ExpressionCompiler.makeBinaryExpression(node.getNode(),
                timestampType, DBSPOpcode.SUB, var.field(0).deref(), node.lower.representation);

        DBSPExpression body =
                new DBSPRawTupleExpression(
                        makeNoExpression(ix.keyType),
                        new DBSPTupleExpression(
                                lowerBound,
                                makeNoExpression(outputValueType.tupFields[1]).someIfNeeded()));
        DBSPClosureExpression closure = body.closure(var);
        MonotoneTransferFunctions analyzer = new MonotoneTransferFunctions(
                this.compiler(), node, MonotoneTransferFunctions.ArgumentKind.IndexedZSet, inputProjection);
        MonotoneExpression result = analyzer.applyAnalysis(closure);
        this.set(node, Objects.requireNonNull(result));
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.info.setCircuit(node.to(DBSPCircuit.class));
        return super.startVisit(node);
    }

    /** Helper class for analyzing filter functions.
     * This extracts all comparisons of the form t.column CMP expression
     * from a list of conjunctions. */
    private static class ComparisonsAnalyzer {
        /** A comparison between an output column and an expression which
         * may be monotone */
        static class Comparison implements ToIndentableString {
            /** Output column involved in comparison */
            public final int columnIndex;
            /** Expression that is compared with column; column >= expression */
            public final DBSPExpression comparedTo;
            /** Parameter that represents the row.  Only used for debugging. */
            final DBSPParameter parameter;
            /** Comparison operator */
            final DBSPOpcode opcode;

            Comparison(int columnIndex, DBSPExpression comparedTo, DBSPOpcode opcode, DBSPParameter parameter) {
                Utilities.enforce(columnIndex >= 0);
                this.opcode = opcode;
                this.columnIndex = columnIndex;
                this.comparedTo = comparedTo;
                this.parameter = parameter;
            }

            @Override
            public IIndentStream toString(IIndentStream builder) {
                return builder
                        .append(this.parameter.name)
                        .append(this.columnIndex)
                        .append(this.opcode.toString())
                        .append(this.comparedTo);
            }

            @Override
            public String toString() {
                return this.parameter.name + "." + this.columnIndex +
                        " " + this.opcode + " " + this.comparedTo;
            }
        }

        public final List<Comparison> comparisons = new ArrayList<>();

        /** Analyze the condition of a filter and decompose it into a conjunction of comparisons */
        public ComparisonsAnalyzer(DBSPExpression closure) {
            DBSPClosureExpression clo = closure.to(DBSPClosureExpression.class);
            Utilities.enforce(clo.parameters.length == 1);
            DBSPParameter param = clo.parameters[0];
            DBSPExpression expression = clo.body;
            if (expression.is(DBSPUnaryExpression.class)) {
                DBSPUnaryExpression unary = expression.to(DBSPUnaryExpression.class);
                // If the filter is wrap_bool(expression), analyze expression
                if (unary.opcode == DBSPOpcode.WRAP_BOOL)
                    expression = unary.source;
            }
            this.complete = this.analyzeConjunction(expression, param);
        }

        public boolean isEmpty() {
            return this.comparisons.isEmpty();
        }

        @Override
        public String toString() {
            return this.comparisons.toString();
        }

        /** Check if `expression` is a reference to a column (field) of a given `parameter`.
         * Return the column index if it is, or -1 otherwise. */
        public static int isColumn(DBSPExpression expression, DBSPParameter param) {
            DBSPFieldExpression field = expression.as(DBSPFieldExpression.class);
            if (field == null)
                return -1;
            DBSPDerefExpression deref = field.expression.as(DBSPDerefExpression.class);
            if (deref == null)
                return -1;
            DBSPVariablePath var = deref.expression.as(DBSPVariablePath.class);
            if (var == null)
                return -1;
            if (var.variable.equals(param.name))
                return field.fieldNo;
            return -1;
        }

        /** If `larger` is a column reference, create a new comparison and add it to the list
         *
         * @return True if the expression is a comparison that has been added. */
        boolean addIfRightIsColumn(DBSPExpression smaller, DBSPExpression larger, DBSPOpcode opcode, DBSPParameter param) {
            int index = isColumn(larger, param);
            if (index < 0)
                return false;
            Comparison comp = new Comparison(index, smaller, opcode, param);
            this.comparisons.add(comp);
            return true;
        }

        static DBSPOpcode inverse(DBSPOpcode opcode) {
            return switch (opcode) {
                case TS_ADD -> DBSPOpcode.TS_SUB;
                case TS_SUB -> DBSPOpcode.TS_ADD;
                case ADD -> DBSPOpcode.SUB;
                case SUB -> DBSPOpcode.ADD;
                case LT -> DBSPOpcode.GTE;
                case GTE -> DBSPOpcode.LT;
                case LTE -> DBSPOpcode.GT;
                case GT -> DBSPOpcode.LTE;
                default -> throw new InternalCompilerError(opcode.toString());
            };
        }

        /** If 'larger' is an expression that adds or subtracts a constant from a column, create a
         * new comparison and add it to the list.
         *
         * @return True if the expression is a comparison that has been added. */
        boolean addIfOffsetOfColumn(DBSPExpression smaller, DBSPExpression larger, DBSPOpcode opcode, DBSPParameter param) {
            DBSPBinaryExpression binary = larger.as(DBSPBinaryExpression.class);
            if (binary == null)
                return false;
            if (binary.opcode != DBSPOpcode.ADD && binary.opcode != DBSPOpcode.SUB &&
                binary.opcode != DBSPOpcode.TS_ADD && binary.opcode != DBSPOpcode.TS_SUB)
                return false;
            DBSPOpcode inverse = inverse(binary.opcode);
            int column = isColumn(binary.left, param);
            if (column >= 0 && binary.right.is(DBSPLiteral.class)) {
                // col + constant, col - constant
                DBSPExpression newSmaller =
                        ExpressionCompiler.makeBinaryExpression(larger.getNode(), larger.getType(),
                                inverse, smaller, binary.right);
                Comparison comp = new Comparison(column, newSmaller, opcode, param);
                this.comparisons.add(comp);
                return true;
            }

            if ((binary.opcode == DBSPOpcode.SUB) || (binary.opcode == DBSPOpcode.TS_SUB))
                return false;

            // constant + col
            column = isColumn(binary.right, param);
            if (column >= 0 && binary.left.is(DBSPLiteral.class)) {
                DBSPExpression newSmaller =
                        ExpressionCompiler.makeBinaryExpression(larger.getNode(), larger.getType(),
                                inverse, smaller, binary.left);
                Comparison comp = new Comparison(column, newSmaller, opcode, param);
                this.comparisons.add(comp);
                return true;
            }

            return false;
        }

        /** Check if `expression` is a comparison and if so add it to the list.
         * Return true if added. */
        boolean findComparison(DBSPBinaryExpression binary, DBSPParameter param) {
            switch (binary.opcode) {
                case LTE:
                case LT: {
                    boolean added = this.addIfRightIsColumn(binary.left, binary.right, inverse(binary.opcode), param);
                    if (added)
                        return true;
                    added = this.addIfOffsetOfColumn(binary.left, binary.right, inverse(binary.opcode), param);
                    return added;
                }
                case GTE:
                case GT: {
                    boolean added = this.addIfRightIsColumn(binary.right, binary.left, binary.opcode, param);
                    if (added)
                        return true;
                    added = this.addIfOffsetOfColumn(binary.right, binary.left, binary.opcode, param);
                    return added;
                }
                case EQ: {
                    // add both ways
                    boolean added = this.addIfRightIsColumn(binary.left, binary.right, binary.opcode, param);
                    added = added || this.addIfRightIsColumn(binary.right, binary.left, binary.opcode, param);
                    if (added)
                        return true;
                    added = this.addIfOffsetOfColumn(binary.left, binary.right, binary.opcode, param);
                    if (added)
                        return true;
                    return this.addIfOffsetOfColumn(binary.right, binary.left, binary.opcode, param);
                }
                default:
                    return false;
            }
        }

        boolean analyzeConjunction(DBSPExpression expression, DBSPParameter param) {
            DBSPBinaryExpression binary = expression.as(DBSPBinaryExpression.class);
            if (binary == null)
                return false;
            if (binary.opcode == DBSPOpcode.AND) {
                boolean foundLeft = this.analyzeConjunction(binary.left, param);
                boolean foundRight = this.analyzeConjunction(binary.right, param);
                return foundLeft && foundRight;
            } else {
                return this.findComparison(binary, param);
            }
        }

        /** True is the entire expression is composed of legal comparisons */
        final boolean complete;

        /** Get all the expressions that are below the specified output column */
        List<DBSPExpression> getLowerBounds(int columnIndex) {
            return Linq.map(
                    Linq.where(this.comparisons, c -> c.columnIndex == columnIndex),
                    c -> c.comparedTo);
        }
    }
}
