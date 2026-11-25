package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRowNumberOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPAsymmetricFieldComparatorExpression.Collation;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Maybe;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Finds, for each output port of an operator, the fields that the operators downstream use.
 * The analysis does not change the circuit.  It visits the operators in reverse topological
 * order; each operator converts the used fields of its output into used fields of its inputs.
 *
 * <p>Each view of a recursive circuit keeps all its fields: the readers of the circuit and the
 * next iteration of the recursion read the view.  One visit of the operators of the recursive
 * circuit is thus sufficient.
 *
 * <p>The use map of a port describes the type {@link OutputPort#getOutputRowRefType}:
 * {@code &T} for a ZSet, {@code (&K, &V)} for an IndexedZSet.  A function with one parameter
 * that reads the port has a parameter of this type. */
public class FindUsedOutputFields extends CircuitVisitor {
    /** How an operator converts the used fields of its output into used fields of its inputs */
    public enum Kind {
        /** Map or MapIndex: the fields that the function reads to compute the used output fields */
        MAP,
        /** Join: all key fields, and the value fields that the function reads to compute
         * the used output fields */
        JOIN,
        /** Filter: the used output fields, and the fields that the predicate reads */
        FILTER,
        /** Antijoin: the output rows are the rows of the left input.  The left input needs all
         * key fields and the used output value fields; the right input needs only the key fields. */
        ANTIJOIN,
        /** Window: the output rows are the rows of the left input.  The left input needs all
         * key fields and the used output value fields. */
        WINDOW,
        /** Deindex: the output rows are the values of the input.  The input needs no key fields
         * and the used output fields. */
        DEINDEX,
        /** RowNumber or Rank: the output key is the input key.  The output producer computes the
         * output value from the rank and the input value.  The input needs all key fields, the
         * value fields that the comparators read, and the value fields that the output producer
         * reads to compute the used output fields. */
        NUMBERING,
        /** Integrate, Differentiate, Negate, Noop, Sum, Subtract, Delay: each input needs the
         * used output fields */
        IDENTITY,
        /** delta0 of a recursive circuit: the input, a port of the enclosing circuit, needs the
         * used output fields */
        DELTA,
        /** All fields */
        ALL
    }

    /** Key: an output port of an operator of the top-level circuit or of a recursive circuit;
     * value: the union of the fields that the readers of the port use, with at least one unused
     * field. */
    Map<OutputPort, FieldUseMap> used;
    /** Ports with all fields used */
    Set<OutputPort> allUsed;
    final FindUsedFields find;

    public FindUsedOutputFields(DBSPCompiler compiler) {
        super(compiler);
        this.used = new HashMap<>();
        this.allUsed = new HashSet<>();
        this.find = new FindUsedFields(compiler);
    }

    /** True if the port carries a ZSet or an IndexedZSet */
    private boolean hasRows(OutputPort port) {
        return port.outputType().is(DBSPTypeZSet.class) || port.outputType().is(DBSPTypeIndexedZSet.class);
    }

    public Kind kind(DBSPOperator operator) {
        DBSPSimpleOperator simple = operator.as(DBSPSimpleOperator.class);
        if (simple == null || !this.hasRows(simple.outputPort()))
            return Kind.ALL;
        if (operator.is(DBSPWindowOperator.class))
            // The right input carries the window bounds, not rows
            return Kind.WINDOW;
        for (OutputPort input: simple.inputs)
            if (!this.hasRows(input))
                return Kind.ALL;
        DBSPExpression function = simple.function;
        boolean unary = function != null && function.is(DBSPClosureExpression.class) &&
                function.to(DBSPClosureExpression.class).parameters.length == 1;
        if (operator.is(DBSPMapOperator.class) || operator.is(DBSPMapIndexOperator.class))
            return unary ? Kind.MAP : Kind.ALL;
        // The joins that have one function, with the parameters (key, left value, right value)
        if (operator.is(DBSPJoinOperator.class)
                || operator.is(DBSPJoinIndexOperator.class)
                || operator.is(DBSPStreamJoinOperator.class)
                || operator.is(DBSPStreamJoinIndexOperator.class)
                || operator.is(DBSPLeftJoinOperator.class)
                || operator.is(DBSPLeftJoinIndexOperator.class))
            return Kind.JOIN;
        if (operator.is(DBSPFilterOperator.class))
            return unary ? Kind.FILTER : Kind.ALL;
        if (operator.is(DBSPAntiJoinOperator.class) || operator.is(DBSPStreamAntiJoinOperator.class))
            return Kind.ANTIJOIN;
        if (operator.is(DBSPDeindexOperator.class))
            return Kind.DEINDEX;
        if (operator.is(DBSPRowNumberOperator.class) || operator.is(DBSPRankOperator.class))
            return Kind.NUMBERING;
        if (operator.is(DBSPDeltaOperator.class))
            return Kind.DELTA;
        if (operator.is(DBSPIntegrateOperator.class)
                || operator.is(DBSPDifferentiateOperator.class)
                || operator.is(DBSPNegateOperator.class)
                || operator.is(DBSPNoopOperator.class)
                || operator.is(DBSPSumOperator.class)
                || operator.is(DBSPSubtractOperator.class))
            return Kind.IDENTITY;
        if (operator.is(DBSPDelayOperator.class) && function == null)
            return Kind.IDENTITY;
        return Kind.ALL;
    }

    private boolean isTupleConstructor(DBSPExpression expression) {
        DBSPBaseTupleExpression tuple = expression.as(DBSPBaseTupleExpression.class);
        return tuple != null && tuple.fields != null;
    }

    /** True if 'part', the key or the value that a function builds, has no fields to drop
     * or is a tuple constructor */
    private boolean canNarrowPart(DBSPExpression part) {
        return !part.getType().is(DBSPTypeTupleBase.class) || this.isTupleConstructor(part);
    }

    /** True if the function of a MAP or JOIN operator can drop output fields without a copy of
     * an expression: the function builds each tuple of the output row with a tuple constructor */
    public boolean canNarrowOutput(DBSPSimpleOperator operator) {
        DBSPExpression body = operator.getClosureFunction().body;
        if (operator.outputType.is(DBSPTypeZSet.class))
            return body.is(DBSPTupleExpression.class) && this.isTupleConstructor(body);
        DBSPRawTupleExpression raw = body.as(DBSPRawTupleExpression.class);
        return raw != null && raw.fields != null && raw.fields.length == 2 &&
                this.canNarrowPart(raw.fields[0]) && this.canNarrowPart(raw.fields[1]);
    }

    /** The composition of 'function' with the projection that keeps only the fields used in
     * 'outputUse'.
     * @param outputUse  Use map of the port that carries the result of 'function'. */
    public DBSPClosureExpression restrictToUsed(DBSPClosureExpression function, FieldUseMap outputUse) {
        if (!outputUse.getType().is(DBSPTypeRawTuple.class)) {
            DBSPClosureExpression projection = Objects.requireNonNull(outputUse.getProjection(1));
            return projection.applyAfter(this.compiler, function, Maybe.YES);
        }
        DBSPClosureExpression projection = Objects.requireNonNull(outputUse.getProjection(2));
        DBSPExpression argument = new DBSPRawTupleExpression(
                function.body.field(0).borrow(),
                function.body.field(1).borrow());
        return projection.call(argument).reduce(this.compiler).closure(function.parameters);
    }

    /** The fields of 'port' that its readers use; null if they use all fields.
     * Valid after the analysis. */
    @Nullable
    public FieldUseMap getUsed(OutputPort port) {
        return this.used.get(port);
    }

    /** The fields that an antijoin or a window needs from its left input: all key fields, and
     * the value fields that the readers of the operator use; null if it needs all fields. */
    @Nullable
    public FieldUseMap leftInputUse(DBSPSimpleOperator operator) {
        FieldUseMap outputUse = this.used.get(operator.outputPort());
        if (outputUse == null)
            return null;
        DBSPTypeRawTuple rowType = operator.inputs.get(0).getOutputIndexedZSetType().getKVRefType();
        FieldUseMap key = new FieldUseMap(rowType.tupFields[0], true);
        FieldUseMap use = FieldUseMap.list(rowType, Linq.list(key, outputUse.field(1)));
        return use.hasUnusedFields() ? use : null;
    }

    /** The fields that a filter needs from its input: the fields that the predicate reads and
     * the fields that the readers of the filter use; null if it needs all fields. */
    @Nullable
    public FieldUseMap filterInputUse(DBSPSimpleOperator filter) {
        FieldUseMap outputUse = this.used.get(filter.outputPort());
        if (outputUse == null)
            return null;
        DBSPClosureExpression predicate = filter.getClosureFunction();
        ParameterFieldUse reads = this.find.findUsedFields(predicate);
        FieldUseMap use = outputUse.reduce(reads.get(predicate.parameters[0]))
                .project(FieldUseMap.depth(filter.inputs.get(0)));
        return use.hasUnusedFields() ? use : null;
    }

    /** The output producer of a RowNumber or Rank operator */
    private DBSPClosureExpression outputProducer(DBSPSimpleOperator operator) {
        DBSPRowNumberOperator rowNumber = operator.as(DBSPRowNumberOperator.class);
        if (rowNumber != null)
            return rowNumber.outputProducer;
        return operator.to(DBSPRankOperator.class).outputProducer;
    }

    /** The fields of the output of a RowNumber or Rank operator that the replacement computes:
     * all key fields, and the value fields that the readers use; null if it computes all fields.
     * The replacement computes all fields if the output producer does not build a tuple. */
    @Nullable
    public FieldUseMap numberingOutputUse(DBSPSimpleOperator operator) {
        FieldUseMap outputUse = this.used.get(operator.outputPort());
        if (outputUse == null)
            return null;
        FieldUseMap valueUse = outputUse.field(1);
        DBSPExpression produced = this.outputProducer(operator).body;
        if (!valueUse.hasUnusedFields() || !this.isTupleConstructor(produced))
            return null;
        DBSPTypeRawTuple rowType = operator.outputPort().getOutputIndexedZSetType().getKVRefType();
        // The key of the output is the key of the input
        FieldUseMap key = new FieldUseMap(rowType.tupFields[0], true);
        return FieldUseMap.list(rowType, Linq.list(key, valueUse));
    }

    /** The output producer of a RowNumber or Rank operator, restricted to the output fields that
     * the replacement computes */
    public DBSPClosureExpression usedProducer(DBSPSimpleOperator operator) {
        DBSPClosureExpression producer = this.outputProducer(operator);
        FieldUseMap outputUse = this.numberingOutputUse(operator);
        if (outputUse == null)
            return producer;
        return this.restrictToUsed(producer, outputUse.field(1));
    }

    /** The fields that a RowNumber or Rank operator needs from its input: all key fields, the
     * value fields that its comparators read, and the value fields that its output producer
     * reads to compute the used output fields; null if it needs all fields. */
    @Nullable
    public FieldUseMap numberingInputUse(DBSPSimpleOperator operator) {
        DBSPClosureExpression producer = this.usedProducer(operator);
        FieldUseMap valueUse = this.find.findUsedFields(producer).get(producer.parameters[1]);
        DBSPRankOperator rank = operator.as(DBSPRankOperator.class);
        if (rank != null) {
            DBSPClosureExpression projection = rank.projectionFunc;
            FieldUseMap projected = this.find.findUsedFields(projection).get(projection.parameters[0]);
            valueUse = valueUse.reduce(projected);
            FieldUseMap valueFields = valueUse.deref();
            for (Collation collation: rank.rankCmpFunc.comparisons)
                valueFields.setUsed(collation.leftField());
        }
        DBSPComparatorExpression comparator = operator.getFunction().to(DBSPComparatorExpression.class);
        if (!RemoveUnusedFields.setComparedFieldsToUsed(comparator, valueUse.deref()))
            return null;
        OutputPort input = operator.inputs.get(0);
        DBSPTypeRawTuple rowType = input.getOutputIndexedZSetType().getKVRefType();
        // The operator groups the rows by the key
        FieldUseMap key = new FieldUseMap(rowType.tupFields[0], true);
        FieldUseMap use = FieldUseMap.list(rowType, Linq.list(key, valueUse))
                .project(FieldUseMap.depth(input));
        return use.hasUnusedFields() ? use : null;
    }

    private void setAllUsed(OutputPort port) {
        this.allUsed.add(port);
        this.used.remove(port);
    }

    /** Add the fields in 'use' to the used fields of 'port' */
    private void addUse(OutputPort port, FieldUseMap use) {
        if (this.allUsed.contains(port))
            return;
        Utilities.enforce(use.getType().sameType(port.getOutputRowRefType()),
                () -> "Use map " + use + " does not describe the rows of " + port);
        // Below the depth of the port a field is either used or unused as a whole
        FieldUseMap union = use.project(FieldUseMap.depth(port));
        FieldUseMap previous = this.used.get(port);
        if (previous != null)
            union = previous.reduce(union);
        if (union.hasUnusedFields())
            this.used.put(port, union);
        else
            this.setAllUsed(port);
    }

    /** The function of a MAP or JOIN operator, restricted to the output fields used downstream
     * if the operator can drop the other output fields */
    private DBSPClosureExpression usedPart(DBSPSimpleOperator operator) {
        DBSPClosureExpression function = operator.getClosureFunction();
        FieldUseMap outputUse = this.used.get(operator.outputPort());
        if (outputUse == null || !this.canNarrowOutput(operator))
            return function;
        return this.restrictToUsed(function, outputUse);
    }

    /** Add to the used fields of the inputs of 'operator' the fields that 'operator' needs.
     * All readers of 'operator' have been visited. */
    private void propagate(DBSPOperator operator) {
        switch (this.kind(operator)) {
            case MAP: {
                DBSPSimpleOperator map = operator.to(DBSPSimpleOperator.class);
                DBSPClosureExpression function = this.usedPart(map);
                ParameterFieldUse reads = this.find.findUsedFields(function);
                this.addUse(map.inputs.get(0), reads.get(function.parameters[0]));
                break;
            }
            case JOIN: {
                DBSPSimpleOperator join = operator.to(DBSPSimpleOperator.class);
                DBSPClosureExpression function = this.usedPart(join);
                Utilities.enforce(function.parameters.length == 3);
                ParameterFieldUse reads = this.find.findUsedFields(function);
                for (int i = 0; i < 2; i++) {
                    OutputPort input = join.inputs.get(i);
                    DBSPTypeRawTuple rowType = input.getOutputIndexedZSetType().getKVRefType();
                    // The join compares the keys, so it uses all key fields
                    FieldUseMap key = new FieldUseMap(rowType.tupFields[0], true);
                    FieldUseMap value = reads.get(function.parameters[i + 1]);
                    this.addUse(input, FieldUseMap.list(rowType, Linq.list(key, value)));
                }
                break;
            }
            case FILTER: {
                DBSPSimpleOperator filter = operator.to(DBSPSimpleOperator.class);
                OutputPort input = filter.inputs.get(0);
                FieldUseMap inputUse = this.filterInputUse(filter);
                if (inputUse == null)
                    this.setAllUsed(input);
                else
                    this.addUse(input, inputUse);
                break;
            }
            case ANTIJOIN: {
                DBSPSimpleOperator antiJoin = operator.to(DBSPSimpleOperator.class);
                OutputPort left = antiJoin.inputs.get(0);
                FieldUseMap leftUse = this.leftInputUse(antiJoin);
                if (leftUse == null)
                    this.setAllUsed(left);
                else
                    this.addUse(left, leftUse);
                OutputPort right = antiJoin.inputs.get(1);
                DBSPTypeRawTuple rowType = right.getOutputIndexedZSetType().getKVRefType();
                FieldUseMap key = new FieldUseMap(rowType.tupFields[0], true);
                FieldUseMap value = new FieldUseMap(rowType.tupFields[1], false);
                this.addUse(right, FieldUseMap.list(rowType, Linq.list(key, value)));
                break;
            }
            case WINDOW: {
                DBSPSimpleOperator window = operator.to(DBSPSimpleOperator.class);
                OutputPort left = window.inputs.get(0);
                FieldUseMap leftUse = this.leftInputUse(window);
                if (leftUse == null)
                    this.setAllUsed(left);
                else
                    this.addUse(left, leftUse);
                this.setAllUsed(window.inputs.get(1));
                break;
            }
            case DEINDEX: {
                DBSPSimpleOperator deindex = operator.to(DBSPSimpleOperator.class);
                OutputPort input = deindex.inputs.get(0);
                DBSPTypeRawTuple rowType = input.getOutputIndexedZSetType().getKVRefType();
                FieldUseMap outputUse = this.used.get(deindex.outputPort());
                FieldUseMap key = new FieldUseMap(rowType.tupFields[0], false);
                FieldUseMap value = outputUse != null ? outputUse : new FieldUseMap(rowType.tupFields[1], true);
                this.addUse(input, FieldUseMap.list(rowType, Linq.list(key, value)));
                break;
            }
            case NUMBERING: {
                DBSPSimpleOperator numbering = operator.to(DBSPSimpleOperator.class);
                OutputPort input = numbering.inputs.get(0);
                FieldUseMap inputUse = this.numberingInputUse(numbering);
                if (inputUse == null)
                    this.setAllUsed(input);
                else
                    this.addUse(input, inputUse);
                break;
            }
            case IDENTITY, DELTA: {
                DBSPSimpleOperator simple = operator.to(DBSPSimpleOperator.class);
                FieldUseMap outputUse = this.used.get(simple.outputPort());
                for (OutputPort input: simple.inputs) {
                    if (outputUse == null)
                        this.setAllUsed(input);
                    else
                        this.addUse(input, outputUse);
                }
                break;
            }
            case ALL:
                for (OutputPort input: operator.inputs)
                    this.setAllUsed(input);
                break;
        }
    }

    /** Make all fields used for each port that an operator in 'operators' reads, if the source
     * of the port is not earlier in 'operators'.  The reverse visit reaches such a source
     * before the operator that reads the port. */
    private void useAllOutOfOrder(List<DBSPOperator> operators) {
        // Key: an operator in 'operators'; value: its index
        Map<DBSPOperator, Integer> position = new HashMap<>();
        for (int i = 0; i < operators.size(); i++)
            Utilities.putNew(position, operators.get(i), i);
        for (int i = 0; i < operators.size(); i++) {
            DBSPOperator operator = operators.get(i);
            if (this.kind(operator) == Kind.DELTA)
                // A delta0 reads a port of the enclosing circuit
                continue;
            for (OutputPort input: operator.inputs) {
                Integer source = position.get(input.node());
                if (source == null || source >= i)
                    this.setAllUsed(input);
            }
        }
    }

    /** Visit in reverse the operators of a circuit.
     * @param operators  The operators of the top-level circuit or of a recursive circuit,
     *                   in topological order. */
    private void propagateAll(List<DBSPOperator> operators) {
        this.useAllOutOfOrder(operators);
        for (int i = operators.size() - 1; i >= 0; i--) {
            DBSPOperator operator = operators.get(i);
            DBSPNestedOperator nested = operator.as(DBSPNestedOperator.class);
            if (nested == null) {
                this.propagate(operator);
                continue;
            }
            // Each view of the recursive circuit keeps all its fields
            for (OutputPort output: nested.internalOutputs)
                if (output != null)
                    this.setAllUsed(output);
            this.propagateAll(Linq.list(nested.getAllOperators()));
        }
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        this.propagateAll(circuit.allOperators);
        Logger.INSTANCE.belowLevel(this, 2)
                .appendSupplier(() -> this.used.toString())
                .newline();
        return VisitDecision.STOP;
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.used = new HashMap<>();
        this.allUsed = new HashSet<>();
        return super.startVisit(node);
    }
}
