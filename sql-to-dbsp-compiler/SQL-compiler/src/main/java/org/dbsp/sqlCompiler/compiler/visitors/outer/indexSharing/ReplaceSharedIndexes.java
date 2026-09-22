package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Works in tandem with FindSharedIndexes; it replaces multiple
 {@link org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator} with a single wide one
 by adjusting the consumers */
class ReplaceSharedIndexes extends CircuitCloneVisitor {
    final FindSharedIndexes finder;
    /** Maps each join, star join, aggregate, or fixed-index operator that reads a shared index
     * to the shared sources of its inputs */
    final Map<DBSPSimpleOperator, ConsumerInputs> sharedInputs;
    /** Maps a builder to the wide MapIndex operator created from it */
    final Map<WideMapIndexBuilder, DBSPMapIndexOperator> sharedIndexes;

    public ReplaceSharedIndexes(DBSPCompiler compiler, FindSharedIndexes finder) {
        super(compiler, false);
        this.finder = finder;
        this.sharedInputs = new HashMap<>();
        this.sharedIndexes = new HashMap<>();
    }

    @Override
    public Token startVisit(IDBSPOuterNode circuit) {
        // Compute the groups of operators that will share inputs
        int count = 1;
        if (!this.finder.candidates.isEmpty()) {
            Logger.INSTANCE.belowLevel(ShareIndexes.class, 1)
                    .append("Shared indexes found:").newline();
        }
        for (FindSharedIndexes.Candidates candidates : this.finder.candidates)
            for (List<FindSharedIndexes.MapIndexAndConsumer> members : candidates.split(this.compiler))
                this.implement(members, count++);
        return super.startVisit(circuit);
    }

    /** Make every member read one shared index instead of the index it computes.
     * @param members  Members that share an index, in the order they are merged.
     * @param number   Number of this index in the log. */
    void implement(List<FindSharedIndexes.MapIndexAndConsumer> members, int number) {
        Logger.INSTANCE.belowLevel(ShareIndexes.class, 1)
                .append(number)
                .append(". ")
                .append(members.size() + " indexes")
                .newline();
        WideMapIndexBuilder builder = WideMapIndexBuilder.create(this.compiler, members);
        for (int i = 0; i < members.size(); i++) {
            FindSharedIndexes.MapIndexAndConsumer member = members.get(i);
            ConsumerInputs inputs = this.sharedInputs.computeIfAbsent(
                    member.consumer(), c -> new ConsumerInputs());
            inputs.set(member.inputIndex(), new SharedSource(builder, i));
        }
    }

    /** The input of a consumer at position {@code inputIndex} after sharing: the wide MapIndex
     * when the input is shared, or the clone of the original input otherwise */
    OutputPort sharedInput(ConsumerInputs inputs, int inputIndex, OutputPort original) {
        OutputPort mapped = this.mapped(original);
        SharedSource source = inputs.get(inputIndex);
        if (source == null)
            return mapped;
        // 'mapped' is the clone of the narrow MapIndex; the wide MapIndex reads the same parent
        OutputPort parent = mapped.node().inputs.get(0);
        return this.sharedIndex(source.builder(), parent).outputPort();
    }

    /** The wide MapIndex that {@code builder} describes, reading {@code input}.  The operator
     * is created and inserted in the circuit the first time a consumer connects to it. */
    DBSPMapIndexOperator sharedIndex(WideMapIndexBuilder builder, OutputPort input) {
        DBSPMapIndexOperator index = this.sharedIndexes.get(builder);
        if (index == null) {
            index = new DBSPMapIndexOperator(builder.node, builder.closure(), input);
            this.addOperator(index);
            Utilities.putNew(this.sharedIndexes, builder, index);
        }
        return index;
    }

    /** Rewrite the function of a join or star join to read the wide values of its shared inputs */
    DBSPClosureExpression rewriteJoinClosure(DBSPClosureExpression closure, ConsumerInputs inputs) {
        ParameterIndexMapSet set = new ParameterIndexMapSet();
        // Parameter 0 is the key; parameter i + 1 reads the value of input i
        for (int inputIndex = 0; inputIndex < closure.parameters.length - 1; inputIndex++) {
            SharedSource source = inputs.get(inputIndex);
            if (source == null)
                continue;
            set.add(closure.parameters[inputIndex + 1], source.getParameterRemap());
        }
        ParameterIndexRewriter rewriter = new ParameterIndexRewriter(this.compiler, set);
        return rewriter.apply(closure).to(DBSPClosureExpression.class);
    }

    /** Rewrite the aggregates to read the row from the value of the wide MapIndex */
    DBSPAggregateList rewriteAggregateList(DBSPAggregateList list, ParameterIndexMap remap) {
        List<IAggregate> aggregates = new ArrayList<>(list.size());
        for (IAggregate aggregate : list.aggregates) {
            ParameterIndexMapSet set = new ParameterIndexMapSet();
            for (DBSPParameter row : aggregate.getRowVariableReferences())
                set.add(row, remap);
            ParameterIndexRewriter rewriter = new ParameterIndexRewriter(this.compiler, set);
            aggregates.add(rewriter.apply(aggregate).to(IAggregate.class));
        }
        return new DBSPAggregateList(list.getNode(), remap.var(), aggregates);
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPLeftJoinFilterMapOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStarJoinOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStarJoinIndexOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStarJoinFilterMapOperator operator) {
        if (!this.processJoin(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        if (!this.processFixedIndex(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPAntiJoinOperator operator) {
        // The operator reads no value on input 1 and copies the one it reads on input 0, so
        // only its inputs change
        if (!this.processFixedIndex(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPIndexedTopKOperator operator) {
        if (!this.processFixedIndex(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPLagOperator operator) {
        if (!this.processFixedIndex(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPWindowOperator operator) {
        // Currently a window's keys are never Tuples, so in 
        // practice a Window will never share integrals.  This
        // code is here in care someday this changes.
        if (!this.processFixedIndex(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        if (!this.processFixedIndex(operator)) {
            super.postorder(operator);
        }
    }

    /** Make an operator that needs a fixed index read its shared index; false if it reads none.
     * The shared index reproduces the value it read before, so only its inputs change. */
    boolean processFixedIndex(DBSPSimpleOperator operator) {
        ConsumerInputs inputs = this.sharedInputs.get(operator);
        if (inputs == null)
            return false;

        List<OutputPort> sources = new ArrayList<>(operator.inputs.size());
        for (int inputIndex = 0; inputIndex < operator.inputs.size(); inputIndex++)
            sources.add(this.sharedInput(inputs, inputIndex, operator.inputs.get(inputIndex)));
        this.map(operator, operator.withInputs(sources, false).to(DBSPSimpleOperator.class));
        return true;
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        if (!this.processRollingAggregate(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        if (!this.processAggregate(operator)) {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        if (!this.processAggregate(operator)) {
            super.postorder(operator);
        }
    }

    /** Make a join or star join read its shared inputs; false if it has none */
    boolean processJoin(DBSPSimpleOperator operator) {
        ConsumerInputs inputs = this.sharedInputs.get(operator);
        if (inputs == null)
            return false;

        List<OutputPort> sources = new ArrayList<>(operator.inputs.size());
        for (int inputIndex = 0; inputIndex < operator.inputs.size(); inputIndex++)
            sources.add(this.sharedInput(inputs, inputIndex, operator.inputs.get(inputIndex)));
        // Must be done after the inputs are created
        DBSPClosureExpression joinClosure = this.rewriteJoinClosure(operator.getClosureFunction(), inputs);
        DBSPSimpleOperator newJoin = operator.with(joinClosure, operator.outputType, sources, false)
                .to(DBSPSimpleOperator.class);
        this.map(operator, newJoin);
        return true;
    }

    /** Make a rolling aggregate read its shared index; false if it reads none */
    boolean processRollingAggregate(DBSPPartitionedRollingAggregateOperator operator) {
        ConsumerInputs inputs = this.sharedInputs.get(operator);
        if (inputs == null)
            return false;

        OutputPort input = this.sharedInput(inputs, 0, operator.input());
        // Must be done after the input is created
        SharedSource source = Objects.requireNonNull(inputs.get(0));
        ParameterIndexMapSet set = new ParameterIndexMapSet();
        set.add(operator.partitioningFunction.parameters[0], source.getParameterRemap());
        ParameterIndexRewriter rewriter = new ParameterIndexRewriter(this.compiler, set);
        DBSPClosureExpression partitioning = rewriter.apply(operator.partitioningFunction)
                .to(DBSPClosureExpression.class);
        DBSPSimpleOperator result = new DBSPPartitionedRollingAggregateOperator(
                operator.getRelNode(), partitioning, operator.getAggregator(), operator.aggregateList,
                operator.lower, operator.upper, operator.getOutputIndexedZSetType(), input);
        this.map(operator, result.copyAnnotations(operator));
        return true;
    }

    boolean processAggregate(DBSPAggregateOperatorBase operator) {
        ConsumerInputs inputs = this.sharedInputs.get(operator);
        if (inputs == null)
            return false;
        if (operator.aggregateList == null)
            // A lowered aggregator has no list to rewrite; it reads an index this pass kept
            return this.processFixedIndex(operator);

        OutputPort input = this.sharedInput(inputs, 0, operator.input());
        // Must be done after the input is created
        SharedSource source = Objects.requireNonNull(inputs.get(0));
        DBSPAggregateList list = this.rewriteAggregateList(operator.getAggregateList(), source.getParameterRemap());
        DBSPTypeIndexedZSet outputType = operator.getOutputIndexedZSetType();
        DBSPSimpleOperator result;
        if (operator.is(DBSPAggregateOperator.class))
            result = new DBSPAggregateOperator(operator.getRelNode(), outputType, null, list, input);
        else if (operator.is(DBSPStreamAggregateOperator.class))
            result = new DBSPStreamAggregateOperator(operator.getRelNode(), outputType, null, list, input);
        else
            throw new InternalCompilerError("Unexpected aggregate operator " + operator);
        this.map(operator, result.copyAnnotations(operator));
        return true;
    }

    /** The shared sources of one consumer */
    static class ConsumerInputs {
        /** Maps the index of a consumer input to the shared source that replaces that input */
        final Map<Integer, SharedSource> byInput = new HashMap<>();

        void set(int inputIndex, SharedSource source) {
            Utilities.putNew(this.byInput, inputIndex, source);
        }

        @Nullable
        SharedSource get(int inputIndex) {
            return this.byInput.get(inputIndex);
        }

        @Override
        public String toString() {
            return this.byInput.toString();
        }
    }

    record SharedSource(WideMapIndexBuilder builder, int consumerIndex) {
        /** Remaps a parameter that reads the narrow value to a fresh variable that reads the wide value */
        public ParameterIndexMap getParameterRemap() {
            var newVar = this.builder.valueType().ref().var();

            // This is the list of fields from the value produced by the MapIndex that this consumer reads
            List<Integer> outputIndexes = this.builder.outputIndexes.get(this.consumerIndex);
            Map<Integer, Integer> remap = new HashMap<>();
            for (int i = 0; i < outputIndexes.size(); i++) {
                int index = outputIndexes.get(i);
                remap.put(i, index);
            }

            return new ParameterIndexMap(newVar, remap);
        }

        @Override
        public String toString() {
            return this.builder + "[" + this.consumerIndex + "]";
        }
    }

    /** {@code indexRemap} maps a field index of the replaced parameter to the field index of {@code var} */
    record ParameterIndexMap(DBSPVariablePath var, Map<Integer, Integer> indexRemap) {}

    /** {@code map} maps a closure parameter to the variable and field remapping that replace it */
    record ParameterIndexMapSet(Map<DBSPParameter, ParameterIndexMap> map) {
        public ParameterIndexMapSet() {
            this(new HashMap<>());
        }

        void add(DBSPParameter param, ParameterIndexMap map) {
            Utilities.putNew(this.map, param, map);
        }

        @Nullable
        ParameterIndexMap get(DBSPParameter param) {
            return this.map.get(param);
        }
    }
}
