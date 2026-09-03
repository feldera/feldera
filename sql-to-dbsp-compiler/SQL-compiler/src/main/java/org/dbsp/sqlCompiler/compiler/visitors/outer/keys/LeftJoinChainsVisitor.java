package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.IIncremental;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FieldUseMap;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindUsedFields;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ParameterFieldUse;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.CollectionShape;
import org.dbsp.sqlCompiler.ir.type.IndexedShape;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Part;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Finds chains of LEFT JOINs and keeps track of the columns each chain carries unchanged from
 * its left input.
 *
 * <p>A chain is a maximal sequence of left joins, each reading the previous one's output as
 * its left input, possibly through operators that compute each output row from a single input row,
 * (e.g., projections and filters).  For each chain the analysis collects two things:
 * - which columns have a copy in the chain's last output,
 * - which of these columns no operator in the chain reads.
 */
public class LeftJoinChainsVisitor extends CircuitVisitor {
    /** What one link of a chain carries.
     * @param start   Port originating the chain.
     * @param length  Number of left joins up to and including this link.
     * @param inputToOutputRemap  Maps each column of {@code start} to one of its copies produced by the current chain link.
     * @param outputToInputRemap  Maps each column produced by the current chain link that copies
     *                a column of {@code start} to that column.
     * @param read    Columns of {@code start} read by some operator between {@code start} and
     *                this link. */
    record Carried(OutputPort start, int length, Map<Column, Column> inputToOutputRemap,
                   Map<Column, Column> outputToInputRemap, Set<Column> read) {
        /** A copy of {@link #read} that the next chain step may add to; sorted, so every
         * report and rewrite iterates columns in the same order. */
        Set<Column> mutableReadCopy() {
            return new TreeSet<>(this.read);
        }

        /** Whether a column of every set of {@code key} reaches the chain's end. */
        boolean carries(KeyColumns key) {
            for (EquivalentColumnSet set : key.sets())
                if (Collections.disjoint(set.columns(), this.inputToOutputRemap.keySet()))
                    return false;
            return true;
        }
    }

    /** One operator the walk in {@link #carriedInto} crosses: its output port and the
     * transform from its input. */
    private record Step(OutputPort port, KeyAnalysis.PortTransform source) {}

    /** Left join Chain description.
     * @param start      Chain source.
     * @param length     Number of left joins in the chain.
     * @param keyCarried A key of {@code start}; all of these columns must reach the chain's end
     *                   (can be null if a key is not discovered).
     * @param onlyCarried Columns of {@code start} that reach the chain's end and that no
     *                    operator of the chain reads.
     * @param inputToOutputRemap     For each column of {@code start} that reaches the chain's end one
     *                   corresponding column of the output.
     * @param outputToInputRemap  For each column of the output that copies a column of {@code start},
     *                   that column.
     * @param end        Output of the chain's last left join. */
    public record Chain(OutputPort start, OutputPort end, int length, @Nullable KeyColumns keyCarried,
                        Set<Column> onlyCarried, Map<Column, Column> inputToOutputRemap,
                        Map<Column, Column> outputToInputRemap) {
        /** One column of {@code start} for each value of the carried key, each with a copy at
         * the chain's end; null when there is no such key. */
        @Nullable
        public List<Column> keyColumns() {
            if (this.keyCarried == null)
                return null;
            List<Column> result = new ArrayList<>();
            for (EquivalentColumnSet set : this.keyCarried.sets()) {
                Column found = null;
                for (Column column : set.columns())
                    if (this.inputToOutputRemap.containsKey(column)) {
                        found = column;
                        break;
                    }
                if (found == null)
                    return null;
                result.add(found);
            }
            return result.isEmpty() ? null : result;
        }

        /** The columns {@link DeferCarriedColumns} may defer: carried, not referenced, and not in the key. */
        public Set<Column> deferrable() {
            List<Column> key = this.keyColumns();
            if (key == null)
                return Set.of();
            Set<Column> result = new TreeSet<>(this.onlyCarried);
            key.forEach(result::remove);
            return result;
        }

        /** Whether {@code start} is indexed by exactly the carried key. */
        public boolean startIsIndexedByKey() {
            if (!(this.start.getShape() instanceof IndexedShape indexed) || this.keyCarried == null)
                return false;
            if (indexed.indexFields() != this.keyCarried.sets().size())
                return false;
            for (Column column : indexed.indexColumns())
                if (!this.keyCarried.contains(column))
                    return false;
            return true;
        }

        /** The key columns of the chain's start in the order of its index.
         * Requires {@link #startIsIndexedByKey()} to hold. */
        public List<Column> keyInIndexOrder() {
            IndexedShape indexed = (IndexedShape) this.start.getShape();
            List<Column> result = new ArrayList<>();
            for (int i = 0; i < indexed.indexFields(); i++) {
                Column column = Column.index(i);
                Column found = null;
                for (EquivalentColumnSet set : this.keyCarried.sets()) {
                    if (!set.contains(column))
                        continue;
                    for (Column member : set.columns())
                        if (this.inputToOutputRemap.containsKey(member)) {
                            found = member;
                            break;
                        }
                }
                Utilities.enforce(found != null, () -> "Index column " + column + " of the chain's start is not carried");
                result.add(found);
            }
            return result;
        }

        /** Whether deferring this chain's columns is worthwhile.
         * Heuristic cost function: do all the left joins do less work in total than the new join added? */
        public boolean worthDeferring() {
            int deferred = this.deferrable().size();
            CollectionShape endShape = this.end.getShape();
            Utilities.enforce(endShape != null, () -> "A chain ends at a collection");
            if (deferred == 0)
                return false;
            int added = endShape.width() - deferred;
            if (!this.startIsIndexedByKey())
                added += this.keyColumns().size() + deferred;
            return this.length * deferred > added;
        }
    }

    private final KeyAnalysis keys;
    /** For each port that a chain reaches after its start, what the chain carries there. */
    private final Map<OutputPort, Carried> carriedAt = new HashMap<>();
    /** Result of this analysis: maps chain outputs to chains.  Chain outputs are
     * always integrator output ports. */
    public final Map<OutputPort, Chain> chainEnd = new HashMap<>();
    /** Maps each chain's start to the integrator output port. */
    private final Map<OutputPort, OutputPort> chainStartToEnd = new HashMap<>();
    /** Ports inside some chain, neither its start nor its end.  A port in {@link #carriedAt}
     * but not in this set is the end of its chain.  A port is inside at most one chain;
     * a second consumer of the port starts a new chain there, so no two chains share a
     * link. */
    private final Set<OutputPort> insideChains = new HashSet<>();

    public LeftJoinChainsVisitor(DBSPCompiler compiler, KeyAnalysis keys) {
        super(compiler);
        this.keys = keys;
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.carriedAt.clear();
        this.chainEnd.clear();
        this.chainStartToEnd.clear();
        this.insideChains.clear();
        return super.startVisit(node);
    }

    static boolean isLeftJoin(DBSPOperator operator) {
        return operator.is(DBSPLeftJoinOperator.class) || operator.is(DBSPLeftJoinIndexOperator.class);
    }

    /** {@link KeyAnalysis#getPortSourceTransform} restricted to the ports a chain can run
     * through. */
    @Nullable
    private KeyAnalysis.PortTransform sourceTransform(OutputPort port) {
        // Chains cannot be continued through right inputs of left joins
        if (port.getShape() instanceof IndexedShape &&
            port.getOutputIndexedZSetType().elementType.mayBeNull)
            return null;
        return this.keys.getPortSourceTransform(port);
    }

    /** The columns of parameter {@code parameter} that the body of {@code closure} reads. */
    private Set<Column> columnsRead(DBSPClosureExpression closure, int parameter) {
        Set<Column> result = new TreeSet<>();
        ParameterFieldUse uses = new FindUsedFields(this.compiler()).findUsedFields(closure);
        DBSPParameter read = closure.parameters[parameter];
        FieldUseMap map = uses.get(read);
        if (map == null || map.isEmpty())
            return result;
        if (read.getType().is(DBSPTypeRawTuple.class)) {
            for (int field : map.field(0).deref().getUsedFields())
                result.add(Column.index(field));
            for (int field : map.field(1).deref().getUsedFields())
                result.add(Column.value(field));
        } else {
            for (int field : map.deref().getUsedFields())
                result.add(Column.none(field));
        }
        return result;
    }

    /** The columns of parameter {@code parameter} that {@code closure} "uses".  An
     * output column that only copies an input column does not count.
     * @param shape  Shape of the closure's output.
     * @return null when an output column is neither a copy nor an expression this analysis
     *         understands, so the columns read are unknown. */
    @Nullable
    private Set<Column> columnsComputedFrom(CollectionShape shape, DBSPClosureExpression closure, int parameter) {
        Provenance provenance = this.keys.provenance(shape, closure);
        List<DBSPExpression> expressions = KeyAnalysis.outputExpressions(shape, closure);
        List<DBSPExpression> computed = new ArrayList<>();
        for (int i = 0; i < expressions.size(); i++) {
            if (provenance.isCopy(shape.output(i)))
                continue;
            if (expressions.get(i) == null)
                return null;
            computed.add(expressions.get(i));
        }
        if (computed.isEmpty())
            return Set.of();
        DBSPClosureExpression computing = new DBSPTupleExpression(computed, false).closure(closure.parameters);
        return this.columnsRead(computing, parameter);
    }

    /** Extend a chain with one operator that computes each output row from a single input row.
     * @param port    Output of the operator to append to the chain.
     * @param source  The {@link #sourceTransform} of {@code port}. */
    private Carried extend(Carried carried, OutputPort port, KeyAnalysis.PortTransform source) {
        DBSPSimpleOperator operator = port.node().to(DBSPSimpleOperator.class);
        CollectionShape shape = port.getShape();
        DBSPClosureExpression closure = KeyAnalysis.closureOf(operator);
        Set<Column> startColumnsRead = carried.mutableReadCopy();
        // An operator without a function, a distinct or an integrator e.g., reads no column
        if (closure != null) {
            Set<Column> readByOperator;
            if (operator.is(DBSPFilterOperator.class)) {
                // A filter copies every column, but its predicate reads some of them
                readByOperator = this.columnsRead(closure, 0);
            } else {
                // A map or a map_index reads the columns it computes with, not the ones it only copies
                Utilities.enforce(shape != null && closure.parameters.length == 1,
                        () -> "Unexpected operator on a chain: " + operator);
                readByOperator = this.columnsComputedFrom(shape, closure, 0);
            }
            // When the columns read are unknown, assume every column is read
            for (var copy : carried.outputToInputRemap().entrySet())
                if (readByOperator == null || readByOperator.contains(copy.getKey()))
                    startColumnsRead.add(copy.getValue());
        }
        Map<Column, Column> outputToInput = new LinkedHashMap<>();
        Map<Column, Column> inputToOutput = new LinkedHashMap<>();
        for (var copy : carried.outputToInputRemap().entrySet()) {
            Column startColumn = copy.getValue();
            for (Column next : source.transform().copiesOf(copy.getKey())) {
                outputToInput.put(next, startColumn);
                inputToOutput.putIfAbsent(startColumn, next);
            }
        }
        return new Carried(carried.start(), carried.length(), inputToOutput, outputToInput, startColumnsRead);
    }

    /** Walks upstream from the left input of {@code join} until it meets the end of a chain
     * already recorded in {@link #carriedAt}, then extends that chain down to that input.
     * When the walk meets no chain, or meets a port inside another chain,
     * a new chain starts.
     * @return null when no chain can be started */
    private Carried carriedInto(DBSPJoinBaseOperator join) {
        // The port the walk is at
        OutputPort current = join.left();
        // Operators going upstream from the left input of this join
        List<Step> path = new ArrayList<>();
        // The current type system of the compiler does not distinguish between
        // streams that represent deltas or full collections.  A chain must start
        // only at a stream that represents a full collection.
        Utilities.enforce(join.is(IIncremental.class), () -> "Expected an incremental join: " + join);
        // left join is incremental-only, so the port feeding it is a delta
        boolean isDelta = true;
        // The most upstream collection the current passed: where a new chain starts if the current
        // meets no chain, together with the length of the path below it
        OutputPort newChainStart = null;
        int pathLengthBelowStart = -1;
        while (!this.carriedAt.containsKey(current) || this.insideChains.contains(current)) {
            KeyAnalysis.PortTransform source = this.sourceTransform(current);
            if (source == null || // Stop below an operator a chain cannot run through
                this.insideChains.contains(current)) // Stop at a port inside another chain
                break;
            if (current.node().is(DBSPDifferentiateOperator.class))
                isDelta = false;
            else if (current.node().is(DBSPIntegrateOperator.class))
                isDelta = true;
            path.add(new Step(current, source));
            current = source.port();
            if (!isDelta) {
                newChainStart = current;
                pathLengthBelowStart = path.size();
            }
        }
        Carried carried = this.insideChains.contains(current) ? null : this.carriedAt.get(current);
        if (carried != null) {
            this.insideChains.add(current);
        } else {
            if (newChainStart == null)
                return null;
            path.subList(pathLengthBelowStart, path.size()).clear();
            CollectionShape shape = newChainStart.getShape();
            Utilities.enforce(shape != null, () -> "A chain does not start at a delta");
            Map<Column, Column> identity = new LinkedHashMap<>();
            for (Column column : shape.columns())
                identity.put(column, column);
            carried = new Carried(newChainStart, 0, identity, identity, Set.of());
        }
        for (int i = path.size() - 1; i >= 0; i--)
            carried = this.extend(carried, path.get(i).port(), path.get(i).source());
        return carried;
    }

    @Override
    public void postorder(DBSPJoinBaseOperator node) {
        if (!isLeftJoin(node))
            return;
        CollectionShape output = node.outputPort().getShape();
        DBSPClosureExpression closure = KeyAnalysis.closureOf(node);
        if (output == null || closure == null || closure.parameters.length != 3)
            return;
        // Compute the chain ending at this join
        Carried carried = this.carriedInto(node);
        if (carried == null)
            return;

        // Add the contribution of this join to the chain.
        Provenance provenance = this.keys.provenance(output, closure);
        // Parameter 1 of a join function is the value of its left input as a plain tuple,
        // so its column i is value column i of the left input
        Set<Column> readByJoin = this.columnsComputedFrom(output, closure, 1);
        Set<Column> startColumnsRead = carried.mutableReadCopy();
        // The join matches on its index, so every index column of the left input is read
        for (var copy : carried.outputToInputRemap().entrySet()) {
            Column leftColumn = copy.getKey();
            boolean isRead = readByJoin == null || leftColumn.part() == Part.INDEX
                    || readByJoin.contains(Column.none(leftColumn.field()));
            if (isRead)
                startColumnsRead.add(copy.getValue());
        }
        Map<Column, Column> outputToInput = new LinkedHashMap<>();
        Map<Column, Column> inputToOutput = new LinkedHashMap<>();
        ColumnCopyTransform transform = KeyAnalysis.sideTransform(provenance, 1);
        for (var copy : carried.outputToInputRemap().entrySet()) {
            Column startColumn = copy.getValue();
            for (Column next : transform.copiesOf(copy.getKey())) {
                outputToInput.put(next, startColumn);
                inputToOutput.putIfAbsent(startColumn, next);
            }
        }
        Carried result = new Carried(carried.start(), carried.length() + 1, inputToOutput, outputToInput, startColumnsRead);
        this.carriedAt.put(node.outputPort(), result);
        this.record(result, node.outputPort());
    }

    /** Logs every chain where it ends. */
    @Override
    public void endVisit() {
        if (Logger.INSTANCE.getLoggingLevel(this.getClass()) >= 1) {
            for (var stored : this.carriedAt.entrySet()) {
                if (stored.getValue().length() < 2 || this.insideChains.contains(stored.getKey()))
                    continue;
                Chain chain = this.chainOf(stored.getValue(), stored.getKey());
                Logger.INSTANCE.belowLevel(this, 1)
                        .append("chain of ")
                        .append(chain.length())
                        .append(" left joins carries key ")
                        .appendSupplier(() -> chain.keyCarried() == null ? "none" : chain.keyCarried().toString())
                        .append(" and only carries ")
                        .append(chain.onlyCarried().size())
                        .append(" columns: ")
                        .appendSupplier(() -> chain.onlyCarried().toString())
                        .append(" at ")
                        .appendSupplier(() -> Linq.map(new ArrayList<>(chain.onlyCarried()),
                                chain.inputToOutputRemap()::get).toString())
                        .append(chain.worthDeferring() ? "; deferring pays" : "; deferring does not pay")
                        .newline();
            }
        }
        super.endVisit();
    }

    /** Records {@code end} in {@link #chainEnd} as the place to splice the chain that
     * {@code carried} describes, when deferring the chain's columns there pays. */
    private void record(Carried carried, OutputPort end) {
        if (carried.length() < 2)
            // a single left join is not a chain
            return;
        // DeferCarriedColumns inserts a join here.  The join needs a collection as input,
        // and only an integrator produces one
        if (!end.node().is(DBSPIntegrateOperator.class))
            return;
        Chain chain = this.chainOf(carried, end);
        if (!chain.worthDeferring())
            return;
        // A later integrator of the same chain replaces an earlier one
        OutputPort previous = this.chainStartToEnd.put(carried.start(), end);
        if (previous != null)
            this.chainEnd.remove(previous);
        this.chainEnd.put(end, chain);
    }

    /** Builds the {@link Chain} described by {@code carried}, ending at {@code end}. */
    private Chain chainOf(Carried carried, OutputPort end) {
        Set<Column> onlyCarried = new TreeSet<>(carried.inputToOutputRemap().keySet());
        onlyCarried.removeAll(carried.read());
        // Among the keys of the start that the chain carries, pick the one that leaves
        // the most columns deferrable.  Keys come smallest first, so ties go to the smallest
        Chain best = null;
        for (KeyColumns key : this.keys.getKeys(carried.start()).keys) {
            if (!carried.carries(key))
                continue;
            Chain candidate = new Chain(carried.start(), end, carried.length(), key,
                    onlyCarried, carried.inputToOutputRemap(), carried.outputToInputRemap());
            if (best == null || candidate.deferrable().size() > best.deferrable().size())
                best = candidate;
        }
        if (best == null)
            best = new Chain(carried.start(), end, carried.length(), null,
                    onlyCarried, carried.inputToOutputRemap(), carried.outputToInputRemap());
        return best;
    }

    /** Appends {@code node} to the chain that reaches its input. */
    @Override
    public void postorder(DBSPUnaryOperator node) {
        Carried carried = this.carriedAt.get(node.input());
        if (carried == null || this.insideChains.contains(node.input()))
            return;
        KeyAnalysis.PortTransform source = this.sourceTransform(node.outputPort());
        if (source == null)
            // An operator without a source transform, an aggregate e.g., ends the chain
            return;
        this.insideChains.add(node.input());
        Carried extended = this.extend(carried, node.outputPort(), source);
        this.carriedAt.put(node.outputPort(), extended);
        this.record(extended, node.outputPort());
    }
}
