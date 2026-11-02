package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApply2Operator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyNOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAtomicSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledKeyFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInternOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPositiveOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRowNumberOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeighOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeightValidatorOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Lineage;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Finds, for each output port, whether the port may "produce" a multiset for any input data.
 * A collection is a multiset if it holds a row with a weight other than 1; otherwise it is a set.
 * 
 * In a non-incremental circuit a port "produces" the collection corresponding to the input data.
 * In an incremental circuit where a port produces a DELTA, we consider as "produced" 
 * the INTEGRAL of the respective DELTA.
 * 
 * <p>The analysis is conservative: a port reported as a set is a set, but a port reported as a
 * multiset may still be a set.
 * 
 * <p>A set mapped through an injective closure produces another set.
 */
public class MultisetAnalysis extends CircuitVisitor {
    /** Key: an output port.  Value: true if the port may produce a multiset. */
    private final Map<OutputPort, Boolean> multiset = new HashMap<>();
    /** Operators whose output ports are in {@link #multiset}. */
    private final Set<DBSPOperator> analyzed = new HashSet<>();

    public MultisetAnalysis(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.multiset.clear();
        this.analyzed.clear();
        return super.startVisit(node);
    }

    /** Analyzes {@code operator} and, inputs first, every operator it depends on that has not
     * been analyzed. */
    private void analyzeWithInputs(DBSPOperator operator) {
        // This is a strange way of using a visitor: this code actually
        // recurses on the circuit structure instead of traversing the
        // nodes in topological order, computing only nodes which 
        // are not yet known.  This is useful because this "visitor"
        // is sometimes called to analyze incomplete circuits, 
        // where only a prefix of the operators is known.
        // This still relies on the "accept" method of the nodes.
        Deque<DBSPOperator> pending = new ArrayDeque<>();
        Set<DBSPOperator> inputsPushed = new HashSet<>();
        pending.push(operator);
        while (!pending.isEmpty()) {
            DBSPOperator next = pending.peek();
            if (this.analyzed.contains(next)) {
                pending.pop();
            } else if (inputsPushed.add(next)) {
                for (OutputPort input : next.inputs)
                    if (!this.analyzed.contains(input.node()))
                        pending.push(input.node());
            } else {
                pending.pop();
                next.accept(this);
            }
        }
    }

    /** True if {@code port} may produce a multiset. */
    public boolean isMultiset(OutputPort port) {
        if (!this.analyzed.contains(port.node()))
            this.analyzeWithInputs(port.node());
        return Utilities.getExists(this.multiset, port);
    }

    private void set(OutputPort port, boolean isMultiset) {
        this.multiset.put(port, isMultiset);
        this.analyzed.add(port.node());
    }

    private void set(DBSPSimpleOperator operator, boolean isMultiset) {
        this.set(operator.outputPort(), isMultiset);
    }

    private boolean anyInputMultiset(DBSPOperator operator) {
        for (OutputPort input : operator.inputs)
            if (this.isMultiset(input))
                return true;
        return false;
    }

    private void sameAsInput(DBSPSimpleOperator operator, int input) {
        this.set(operator, this.isMultiset(operator.inputs.get(input)));
    }

    /** Adds to {@code sources} the parameter fields that {@code value} copies.
     * @return false if a part of {@code value} is not a copy of a parameter field. */
    static boolean addCopiedFields(Lineage.ValueSource value, Set<Provenance.Source> sources) {
        if (value.is(Lineage.Ref.class)) {
            return addCopiedFields(value.to(Lineage.Ref.class).value(), sources);
        } else if (value.is(Lineage.Tuple.class)) {
            boolean allCopies = true;
            for (Lineage.ValueSource field : value.to(Lineage.Tuple.class).fields())
                allCopies &= addCopiedFields(field, sources);
            return allCopies;
        } else if (value.is(Lineage.Atom.class)
                && value.to(Lineage.Atom.class).value() instanceof Provenance.SourceSet set) {
            Provenance.Source source = set.single();
            if (source != null) {
                sources.add(source);
                return true;
            }
        }
        return false;
    }

    /** True if {@code closure} certainly maps arguments that differ in the parameters at
     * {@code parameters} to different results.  False if the analysis cannot tell.
     *
     * <p>Example: a map over an indexed Z-set, whose parameter is a (key, value) pair:
     * {@code |p: (&Tup1<i32>, &Tup2<i32, i32>)| Tup2::new(p.0.0, p.1.1)}.
     * The parameter has three fields: key field 0, and value fields 0 and 1.
     * The result copies key field 0 and value field 1, but not value field 0.
     * Two arguments that differ only in value field 0 give the same result,
     * so the closure is not injective.  A closure with the body
     * {@code Tup3::new(p.0.0, p.1.0, p.1.1)} would be injective.
     *
     * @param closure    Function analyzed.
     * @param parameters Indexes of the closure parameters that must be recoverable from the result. */
    boolean isCertainlyInjective(DBSPClosureExpression closure, List<Integer> parameters) {
        // 1. Give each parameter field its own source.
        //    Collect the sources of the parameter fields that must be recoverable from the result.
        //    In the example: key field 0, value field 0, and value field 1.
        Map<DBSPParameter, Lineage.ValueSource> initial = KeyAnalysis.initialValues(closure);
        Set<Provenance.Source> needed = new HashSet<>();
        for (int parameter : parameters) {
            Lineage.ValueSource value = Utilities.getExists(initial, closure.parameters[parameter]);
            if (!addCopiedFields(value, needed))
                // A parameter that is not a row, a reference to one, or a (key, value) pair
                // of an indexed Z-set: the interpreter gives its fields no sources
                return false;
        }
        // 2. Interpret the closure symbolically.  The result records the parameter field
        //    that each part of the result copies.  In the example: key field 0 and value field 1.
        //    Lowered closures can contain returns and assignments; such a closure counts as not injective.
        LosslessCastLineage interpreter = new LosslessCastLineage(this.compiler(), false);
        Lineage.ValueSource result = interpreter.analyze(closure, initial);
        if (interpreter.foundUnhandled)
            return false;
        // 3. The closure is injective if the result copies every specified parameter field:
        //    arguments that differ in such a parameter field give results that differ in its copy.
        //    The parts of the result that are not copies do not matter.
        //    In the example, value field 0 is missing.
        Set<Provenance.Source> copied = new HashSet<>();
        addCopiedFields(result, copied);
        return copied.containsAll(needed);
    }

    /** True if {@code type} has a single value: the NULL type, or an empty tuple that cannot be
     * NULL, possibly behind a reference that cannot be NULL. */
    static boolean hasSingleValue(DBSPType type) {
        if (type.is(DBSPTypeNull.class))
            return true;
        if (type.mayBeNull)
            return false;
        if (type.is(DBSPTypeRef.class))
            return hasSingleValue(type.deref());
        return type.is(DBSPTypeTupleBase.class) && type.to(DBSPTypeTupleBase.class).size() == 0;
    }

    /** True if {@code function} is a closure, and every parameter field of the
     * closure certainly reaches its result.  False if the analysis cannot tell. */
    boolean isCertainlyInjective(DBSPExpression function) {
        DBSPClosureExpression closure = function.as(DBSPClosureExpression.class);
        if (closure == null)
            return false;
        List<Integer> all = new ArrayList<>();
        for (int i = 0; i < closure.parameters.length; i++)
            // A parameter whose type has a single value is the same in all arguments
            if (!hasSingleValue(closure.parameters[i].getType()))
                all.add(i);
        return this.isCertainlyInjective(closure, all);
    }

    /** The output is a set if all inputs are sets and both functions are injective.
     * @param second A function applied to the result of {@code first}; may be null. */
    private void checkFunctionsAreInjective(DBSPSimpleOperator operator,
                                            DBSPExpression first, @Nullable DBSPExpression second) {
        boolean isMultiset = this.anyInputMultiset(operator)
                || !this.isCertainlyInjective(first)
                || (second != null && !this.isCertainlyInjective(second));
        this.set(operator, isMultiset);
    }

    @Override
    public void postorder(DBSPOperator operator) {
        throw new InternalCompilerError("No multiset rule for operator " + operator, operator);
    }

    // Sources

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPSourceMapOperator operator) {
        // A table with a primary key holds one row per key
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPViewDeclarationOperator operator) {
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPNowOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        DBSPExpression value = operator.getFunction();
        boolean isSet;
        if (value.is(DBSPZSetExpression.class))
            isSet = value.to(DBSPZSetExpression.class).isCertainlyDistinct();
        else
            isSet = value.to(DBSPIndexedZSetExpression.class).isEmpty();
        this.set(operator, !isSet);
    }

    @Override
    public void postorder(DBSPInputMapWithWaterlineOperator operator) {
        // The table: it has a primary key, so it holds one row per key
        this.set(operator.getOutput(0), false);
        // The errors: the same error can occur more than once
        this.set(operator.getOutput(1), true);
        // The waterline: a single value, not a collection
        this.set(operator.getOutput(2), false);
    }

    // Operators whose output is a set

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPBinaryDistinctOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPAggregateOperatorBase operator) {
        // One row per group
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPChainAggregateOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPPrimitiveAggregateOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessRetainKeysOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPAggregateZeroOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPUpsertFeedbackOperator operator) {
        // Each update retracts the previous value of its key: one row per key
        this.set(operator, false);
    }

    /** The output of {@code operator} is a single value, not a collection, so the answer is
     * arbitrary.  The answer is part of the persistent ids; set keeps the ids that these
     * operators had before this analysis. */
    private void singleValue(DBSPSimpleOperator operator) {
        this.set(operator, false);
    }

    @Override
    public void postorder(DBSPApplyOperator operator) {
        this.singleValue(operator);
    }

    @Override
    public void postorder(DBSPApply2Operator operator) {
        this.singleValue(operator);
    }

    @Override
    public void postorder(DBSPApplyNOperator operator) {
        this.singleValue(operator);
    }

    @Override
    public void postorder(DBSPWaterlineOperator operator) {
        this.singleValue(operator);
    }

    // Operators whose output may be a multiset for any inputs

    @Override
    public void postorder(DBSPSumOperator operator) {
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPAtomicSumOperator operator) {
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        // The result may have negative weights
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        // The result has negative weights
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        // The function of a deindex drops the key, so it is injective if the key has no fields
        this.checkFunctionsAreInjective(operator, operator.getFunction(), null);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPFlatMapIndexOperator operator) {
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        // The runtime documents one output row (p, (ts, f)) for each input row (p, (ts, v)):
        // input rows that differ only in v give equal output rows
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) {
        // Same output rows as DBSPPartitionedRollingAggregateOperator
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPWeighOperator operator) {
        // The weights of the result hold aggregated values
        this.set(operator, true);
    }

    @Override
    public void postorder(DBSPControlledKeyFilterOperator operator) {
        // The rows that pass the filter
        this.set(operator.getOutput(0), this.isMultiset(operator.inputs.get(0)));
        // The errors: the same error can occur more than once
        this.set(operator.getOutput(1), true);
    }

    // Operators whose output is a set if their data input is a set

    @Override
    public void postorder(DBSPFilterOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPPositiveOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPIntegrateOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPDifferentiateOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPDeltaOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPDelayOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPDelayedIntegralOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPWeightValidatorOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPNoopOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPInternOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPViewBaseOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPHopOperator operator) {
        // Each output row is an input row extended with the bounds of one window
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPWindowOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPAntiJoinOperator operator) {
        this.sameAsInput(operator, 0);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        this.sameAsInput(operator, 0);
    }

    // Operators that compute outputs

    @Override
    public void postorder(DBSPMapOperator operator) {
        this.checkFunctionsAreInjective(operator, operator.getFunction(), null);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        this.checkFunctionsAreInjective(operator, operator.getFunction(), null);
    }

    @Override
    public void postorder(DBSPChainOperator operator) {
        boolean isMultiset = this.anyInputMultiset(operator);
        for (DBSPChainOperator.Computation computation : operator.chain.computations()) {
            if (isMultiset)
                break;
            if (computation.kind() != DBSPChainOperator.ComputationKind.Filter
                    && !this.isCertainlyInjective(computation.closure()))
                isMultiset = true;
        }
        this.set(operator, isMultiset);
    }

    @Override
    public void postorder(DBSPLagOperator operator) {
        // One output row per input row, computed from the row and an earlier row:
        // a set if the input is a set and the input row is recoverable from the output row
        DBSPClosureExpression function = operator.getFunction().as(DBSPClosureExpression.class);
        boolean isMultiset = this.anyInputMultiset(operator)
                || function == null
                || !this.isCertainlyInjective(function, List.of(0));
        this.set(operator, isMultiset);
    }

    @Override
    public void postorder(DBSPJoinBaseOperator operator) {
        this.checkFunctionsAreInjective(operator, operator.getFunction(), null);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        this.checkFunctionsAreInjective(operator, operator.getFunction(), operator.map);
    }

    @Override
    public void postorder(DBSPLeftJoinFilterMapOperator operator) {
        this.checkFunctionsAreInjective(operator, operator.getFunction(), operator.map);
    }

    @Override
    public void postorder(DBSPStarJoinBaseOperator operator) {
        this.checkFunctionsAreInjective(operator, operator.getFunction(), null);
    }

    @Override
    public void postorder(DBSPStarJoinFilterMapOperator operator) {
        this.checkFunctionsAreInjective(operator, operator.getFunction(), operator.map);
    }

    // Operators that number the rows of each group

    @Override
    public void postorder(DBSPIndexedTopKOperator operator) {
        boolean isSet = DBSPIndexedTopKOperator.emitsDistinctRows(
                operator.numbering, operator.limit, operator.outputProducer);
        this.set(operator, !isSet);
    }

    @Override
    public void postorder(DBSPRankOperator operator) {
        // A set if each row of a group gets a different rank, and the output contains the rank
        boolean isSet = !operator.numbering.mayHaveDuplicates()
                && DBSPIndexedTopKOperator.emitsRank(operator.outputProducer);
        this.set(operator, !isSet);
    }

    @Override
    public void postorder(DBSPRowNumberOperator operator) {
        this.set(operator, !DBSPIndexedTopKOperator.emitsRank(operator.outputProducer));
    }

    @Override
    public void postorder(DBSPNestedOperator operator) {
        for (int i = 0; i < operator.outputCount(); i++) {
            // Each output applies distinct to each stream it returns
            if (operator.internalOutputs.get(i) != null)
                this.set(operator.getOutput(i), false);
        }
        this.analyzed.add(operator);
    }
}
