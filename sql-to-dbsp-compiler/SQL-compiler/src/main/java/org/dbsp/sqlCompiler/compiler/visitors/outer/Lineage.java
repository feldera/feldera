package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.IColumnMetadata;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SymbolicInterpreter;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdField;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Analyze dataflow graph and compute lineage for some output columns.
 * "Lineage" traces a column to a source table. */
public class Lineage extends CircuitVisitor {
    interface LatticeValue<T extends LatticeValue<T>> {
        T union(T other);
        T intersect(T other);
        boolean isBottom();
    }

    /** Describes the source which produced a specific value */
    public interface ValueSource extends ICastable, LatticeValue<ValueSource> {
        @Override
        default boolean isBottom() {
            return this == Unknown.INSTANCE;
        }
    }

    /** Source of value is unknown */
    record Unknown() implements ValueSource {
        public static final Unknown INSTANCE = new Unknown();

        @Override
        public ValueSource intersect(ValueSource other) {
            return this;
        }

        @Override
        public ValueSource union(ValueSource other) {
            return other;
        }
    }

    /** These will be the atomic values we track through the dataflow analysis */
    record TableColumn(String table, String column) implements Comparable<TableColumn> {
        @Override
        public String toString() {
            return this.table + "." + this.column;
        }

        @Override
        public int compareTo(Lineage.TableColumn other) {
            int compare = this.table.compareTo(other.table);
            if (compare != 0)
                return compare;
            return this.column.compareTo(other.column);
        }
    }

    record ColumnSet(TreeSet<TableColumn> set) implements LatticeValue<ColumnSet> {
        public ColumnSet(TableColumn value) {
            this(new TreeSet<>());
            this.set.add(value);
        }

        @Override
        public ColumnSet union(ColumnSet other) {
            TreeSet<TableColumn> union = new TreeSet<>(this.set);
            union.addAll(other.set);
            return new ColumnSet(union);
        }

        @Override
        public ColumnSet intersect(ColumnSet other) {
            TreeSet<TableColumn> intersection = new TreeSet<>(this.set);
            intersection.retainAll(other.set);
            return new ColumnSet(intersection);
        }

        public int size() {
            return this.set.size();
        }

        @Override
        public boolean isBottom() {
            return this.set.isEmpty();
        }

        @Override
        public String toString() {
            return this.set.toString();
        }
    }

    record Atom<A extends LatticeValue<A>>(A value) implements ValueSource {
        @Override public String toString() {
            return this.value.toString();
        }

        @Override
        public ValueSource intersect(ValueSource other) {
            if (!other.is(Atom.class)) {
                return Unknown.INSTANCE;
            }
            @SuppressWarnings("unchecked")
            Atom<A> oa = (Atom<A>) other;
            A intersection = this.value.intersect(oa.value);
            if (intersection.isBottom()) {
                return Unknown.INSTANCE;
            }
            return new Atom<>(intersection);
        }

        @Override
        public ValueSource union(ValueSource other) {
            if (!other.is(Atom.class)) {
                return Unknown.INSTANCE;
            }
            @SuppressWarnings("unchecked")
            Atom<A> oa = (Atom<A>) other;
            A union = this.value.union(oa.value);
            return new Atom<>(union);
        }
    }

    /** A reference of another value */
    record Ref(ValueSource value) implements ValueSource {
        @Override
        public ValueSource intersect(ValueSource other) {
            if (other.is(Ref.class)) {
                ValueSource inf = this.value.intersect(other.to(Ref.class).value);
                if (inf.isBottom())
                    return inf;
                return new Ref(inf);
            }
            return Unknown.INSTANCE;
        }

        @Override
        public ValueSource union(ValueSource other) {
            if (other.is(Ref.class)) {
                ValueSource inf = this.value.union(other.to(Ref.class).value);
                if (inf.isBottom())
                    return inf;
                return new Ref(inf);
            }
            return Unknown.INSTANCE;
        }
    }

    /** A constant; value is usually a literal */
    record Constant(DBSPExpression value) implements ValueSource {
        @Override
        public ValueSource intersect(ValueSource other) {
            if (other.is(Constant.class) && other.to(Constant.class).value.equivalent(this.value))
                return this;
            return Unknown.INSTANCE;
        }

        @Override
        public ValueSource union(ValueSource other) {
            // TODO: this could be improved by keeping a set of constants
            if (other.is(Constant.class) && other.to(Constant.class).value.equivalent(this.value))
                return this;
            return Unknown.INSTANCE;
        }
    }

    /** A tuple of other values */
    record Tuple(List<ValueSource> fields) implements ValueSource {
        @Override
        public ValueSource intersect(ValueSource other) {
            if (!other.is(Tuple.class))
                return Unknown.INSTANCE;
            Tuple ot = other.to(Tuple.class);
            Utilities.enforce(this.fields.size() == ot.fields.size());
            boolean anyKnown = false;
            List<ValueSource> meets = new ArrayList<>();
            for (int i = 0; i < this.fields.size(); i++) {
                ValueSource inf = this.fields.get(i).intersect(ot.fields.get(i));
                anyKnown = anyKnown || !inf.isBottom();
                meets.add(inf);
            }
            if (anyKnown)
                return new Tuple(meets);
            return Unknown.INSTANCE;
        }

        @Override
        public ValueSource union(ValueSource other) {
            if (!other.is(Tuple.class))
                return Unknown.INSTANCE;
            Tuple ot = other.to(Tuple.class);
            Utilities.enforce(this.fields.size() == ot.fields.size());
            boolean anyKnown = false;
            List<ValueSource> sup = new ArrayList<>();
            for (int i = 0; i < this.fields.size(); i++) {
                ValueSource union = this.fields.get(i).union(ot.fields.get(i));
                anyKnown = anyKnown || !union.isBottom();
                sup.add(union);
            }
            if (anyKnown)
                return new Tuple(sup);
            return Unknown.INSTANCE;
        }

        public ValueSource field(int fieldNo) {
            return this.fields.get(fieldNo);
        }

        public int size() {
            return this.fields.size();
        }
    }

    final Map<OutputPort, ValueSource> lineage;

    public Lineage(DBSPCompiler compiler) {
        super(compiler);
        this.lineage = new HashMap<>();
    }

    void set(OutputPort port, ValueSource result) {
        if (result.is(Tuple.class)) {
            DBSPTypeTupleBase outputType = port.getOutputRowType().to(DBSPTypeTupleBase.class);
            Utilities.enforce(outputType.size() == result.to(Tuple.class).size());
            // System.out.println(port.node().getId() + " " + result);
        }
        Utilities.putNew(this.lineage, port, result);
    }

    /** Given the ValueSource computed by an output port, create a new ValueSource suitable to feed to the
     * next operator.  We cannot just use the ValueSource unchanged, because a stream of type Stream[T] is
     * consumed by a closure of type fn (x: |&T|) -> S. */
    ValueSource borrow(OutputPort port) {
        ValueSource produced = Utilities.getExists(this.lineage, port);
        if (produced.isBottom())
            return produced;
        boolean isZset = port.outputType().is(DBSPTypeZSet.class);
        if (isZset) {
            return new Ref(produced);
        } else {
            Utilities.enforce(produced.is(Tuple.class));
            Tuple tup = produced.to(Tuple.class);
            Utilities.enforce(tup.size() == 2);
            return new Tuple(List.of(
                    new Ref(tup.field(0)),
                    new Ref(tup.field(1))));
        }
    }

    Map<DBSPParameter, ValueSource> initialValues(DBSPClosureExpression function, ValueSource... values) {
        Map<DBSPParameter, ValueSource> result = new HashMap<>();
        Utilities.enforce(function.parameters.length == values.length);
        for (int i = 0; i < function.parameters.length; i++)
            Utilities.putNew(result, function.parameters[i], values[i]);
        return result;
    }

    void unary(DBSPUnaryOperator node) {
        if (!node.getFunction().is(DBSPClosureExpression.class)) {
            this.set(node.outputPort(), Unknown.INSTANCE);
            return;
        }

        ValueSource value = this.borrow(node.input());
        InnerLineage inner = new InnerLineage(this.compiler);
        var map = this.initialValues(node.getClosureFunction(), value);
        ValueSource result = inner.analyze(node.getClosureFunction(), map);
        this.set(node.outputPort(), result);
    }

    @Override
    public void postorder(DBSPConstantOperator node) {
        this.set(node.outputPort(), Unknown.INSTANCE);
    }

    @Override
    public void postorder(DBSPMapOperator node) {
        this.unary(node);
    }

    @Override
    public void postorder(DBSPNowOperator node) {
        this.set(node.outputPort(), Unknown.INSTANCE);
    }

    @Override
    public void postorder(DBSPDeindexOperator node) {
        this.unary(node);
    }

    @Override
    public void postorder(DBSPMapIndexOperator node) {
        this.unary(node);
    }

    void eraseValue(DBSPUnaryOperator node) {
        ValueSource value = this.borrow(node.input());
        ValueSource result = Unknown.INSTANCE;
        if (value.is(Tuple.class)) {
            Tuple tuple = value.to(Tuple.class);
            result = new Tuple(Linq.list(tuple.field(0), Unknown.INSTANCE));
        }
        this.set(node.outputPort(), result);
    }

    @Override
    public void postorder(DBSPChainAggregateOperator node) {
        this.eraseValue(node);
    }

    @Override
    public void postorder(DBSPIndexedTopKOperator node) {
        this.eraseValue(node);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator node) {
        this.eraseValue(node);
    }

    @Override
    public void postorder(DBSPAggregateZeroOperator node) {
        this.eraseValue(node);
    }

    @Override
    public void postorder(DBSPLagOperator node) {
        this.eraseValue(node);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator node) {
        this.eraseValue(node);
    }

    void copy(DBSPUnaryOperator node) {
        ValueSource value = Utilities.getExists(this.lineage, node.input());
        this.set(node.outputPort(), value);
    }

    @Override
    public void postorder(DBSPFilterOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPNegateOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPViewOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPIntegrateOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPNoopOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPDifferentiateOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPFlatMapOperator node) {
        final ValueSource source = this.borrow(node.input());
        if (source.isBottom()) {
            this.set(node.outputPort(), source);
            return;
        }
        DBSPExpression function = node.getFunction();
        if (function.is(DBSPFlatmap.class)) {
            final Tuple inputTuple = source.to(Ref.class).value.to(Tuple.class);
            List<ValueSource> resultColumns = new ArrayList<>();
            DBSPFlatmap flatmap = function.to(DBSPFlatmap.class);
            for (int index : flatmap.leftInputIndexes)
                resultColumns.add(inputTuple.field(index));
            if (flatmap.rightProjections != null) {
                for (DBSPClosureExpression clo : flatmap.rightProjections)
                    resultColumns.add(Unknown.INSTANCE);
            } else {
                final DBSPType collectionElementType = flatmap.getCollectionElementType();
                if (flatmap.ordinalityIndexType != null) {
                    // e.1, as produced by the iterator
                    if (collectionElementType.is(DBSPTypeTupleBase.class)) {
                        for (int i = 0; i < collectionElementType.to(DBSPTypeTupleBase.class).size(); i++)
                            resultColumns.add(Unknown.INSTANCE);
                    } else {
                        resultColumns.add(Unknown.INSTANCE);
                    }
                } else if (collectionElementType.is(DBSPTypeTupleBase.class)) {
                    // Calcite's UNNEST has a strange semantics:
                    // If e is a tuple type, unpack its fields here
                    DBSPTypeTupleBase tuple = collectionElementType.to(DBSPTypeTupleBase.class);
                    for (int ei = 0; ei < tuple.size(); ei++)
                        resultColumns.add(Unknown.INSTANCE);
                } else {
                    resultColumns.add(Unknown.INSTANCE);
                }
                if (flatmap.ordinalityIndexType != null) {
                    resultColumns.add(Unknown.INSTANCE);
                }
            }
            resultColumns = flatmap.shuffle.shuffle(resultColumns);
            boolean found = false;
            for (final ValueSource col : resultColumns) {
                if (!col.isBottom()) {
                    found = true;
                    break;
                }
            }
            if (found) {
                this.set(node.outputPort(), new Tuple(resultColumns));
            } else {
                this.set(node.outputPort(), Unknown.INSTANCE);
            }
        } else {
            this.set(node.outputPort(), Unknown.INSTANCE);
        }
    }

    void intersect(DBSPSimpleOperator node) {
        ValueSource inf = null;
        for (OutputPort in: node.inputs) {
            ValueSource current = Utilities.getExists(this.lineage, in);
            if (inf == null)
                inf = current;
            else
                inf = inf.intersect(current);
        }
        Utilities.enforce(inf != null);
        this.set(node.outputPort(), inf);
    }

    @Override
    public void postorder(DBSPSumOperator node) {
        this.intersect(node);
    }

    @Override
    public void postorder(DBSPHopOperator node) {
        ValueSource source = Utilities.getExists(this.lineage, node.input());
        ValueSource output;
        if (source.isBottom())
            output = source;
        else {
            // The timestamps added
            Lineage.Tuple tuple = source.to(Tuple.class);
            List<ValueSource> values = new ArrayList<>(tuple.fields());
            values.add(Unknown.INSTANCE);
            values.add(Unknown.INSTANCE);
            output = new Tuple(values);
        }
        this.set(node.outputPort(), output);
    }

    @Override
    public void postorder(DBSPSubtractOperator node) {
        this.intersect(node);
    }

    Set<ColumnSet> emitted = new HashSet<>();

    @Override
    public void postorder(DBSPJoinBaseOperator node) {
        ValueSource left = this.borrow(node.left());
        ValueSource right = this.borrow(node.right());
        ValueSource key = null;
        if (!left.isBottom()) {
            Tuple lt = left.to(Tuple.class);
            key = lt.field(0);
            left = lt.field(1);
        }
        if (!right.isBottom()) {
            Tuple rt = right.to(Tuple.class);
            if (key == null) {
                key = rt.field(0);
            } else {
                key = key.union(rt.field(0));
            }
            right = rt.field(1);
        }
        if (key == null) {
            key = Unknown.INSTANCE;
        }
        Map<DBSPParameter, ValueSource> map = new HashMap<>();
        DBSPClosureExpression clo = node.getClosureFunction();
        map.put(clo.parameters[0], key);
        map.put(clo.parameters[1], left);
        map.put(clo.parameters[2], right);
        InnerLineage inner = new InnerLineage(this.compiler);
        ValueSource result = inner.analyze(clo, map);
        this.set(node.outputPort(), result);

        // Emit output correlation information
        if (key.is(Ref.class)) {
            key = key.to(Ref.class).value;
            if (key.is(Tuple.class)) {
                Tuple tuple = key.to(Tuple.class);
                for (ValueSource value : tuple.fields) {
                    if (value.is(Atom.class)) {
                        @SuppressWarnings("unchecked")
                        Atom<ColumnSet> atom = (Atom<ColumnSet>) value;
                        ColumnSet set = atom.value();
                        if (set.size() <= 1)
                            continue;
                        if (this.emitted.contains(set))
                            continue;
                        this.emitted.add(set);
                        System.out.println("[Correlated:] " + set);
                    }
                }
            }
        }
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator node) {
        ValueSource left = this.borrow(node.left());
        this.set(node.outputPort(), left);
    }

    @Override
    public void postorder(DBSPWindowOperator node) {
        ValueSource left = this.borrow(node.left());
        this.set(node.outputPort(), left);
    }

    @Override
    public void postorder(DBSPSourceTableOperator node) {
        List<ValueSource> columns = new ArrayList<>();
        for (IColumnMetadata col: node.getColumnsMetadata()) {
            TableColumn tc = new TableColumn(node.tableName.name(), col.getColumnName().name());
            columns.add(new Atom<>(new ColumnSet(tc)));
        }
        Tuple value = new Tuple(columns);
        this.set(node.outputPort(), value);
    }

    /** Computes lineage of an expression by tracking where each field of the expression is coming from. */
    public static class InnerLineage extends SymbolicInterpreter<ValueSource> {
        final ResolveReferences resolver;

        public InnerLineage(DBSPCompiler compiler) {
            super(compiler);
            this.resolver = new ResolveReferences(compiler, false);
            initialValues = new HashMap<>();
        }

        void setUnknown(DBSPExpression expression) {
            if (this.getN(expression) != null)
                return;
            this.set(expression, Unknown.INSTANCE);
        }

        @Override
        public VisitDecision preorder(DBSPExpression expression) {
            if (this.getN(expression) != null) {
                // Already computed
                return VisitDecision.STOP;
            }
            return VisitDecision.CONTINUE;
        }

        @Override
        public void postorder(DBSPExpression expression) {
            this.setUnknown(expression);
        }

        @Override
        public void postorder(DBSPIfExpression expression) {
            if (expression.negative == null) {
                this.setUnknown(expression);
                return;
            }
            ValueSource pos = this.get(expression.positive);
            ValueSource neg = this.get(expression.negative);
            ValueSource inf = pos.intersect(neg);
            this.set(expression, inf);
        }

        @Override
        public void postorder(DBSPBorrowExpression expression) {
            ValueSource source = this.get(expression.expression);
            if (source.isBottom())
                this.setUnknown(expression);
            else
                this.set(expression, new Ref(source));
        }

        @Override
        public void postorder(DBSPDerefExpression expression) {
            ValueSource source = this.get(expression.expression);
            if (!source.is(Ref.class)) {
                this.setUnknown(expression);
            } else {
                ValueSource deref = source.to(Ref.class).value;
                this.set(expression, deref);
            }
        }

        @Override
        public void postorder(DBSPVariablePath var) {
            IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(var);
            ValueSource source = this.getCurrentValue(declaration);
            if (source == null || source.isBottom())
                this.setUnknown(var);
            else
                this.set(var, source);
        }

        @Override
        public VisitDecision preorder(DBSPLetExpression node) {
            super.preorder(node);
            // This one is done in preorder
            node.initializer.accept(this);
            ValueSource initializer = this.get(node.initializer);
            // Effects of initializer should be visible while processing consumer
            node.consumer.accept(this);
            ValueSource consumer = this.get(node.consumer);
            this.set(node, consumer);
            super.postorder(node);
            // no postorder invoked
            return VisitDecision.STOP;
        }

        @Override
        public void postorder(DBSPAssignmentExpression expression) {
            // We do not plan to analyze any function which may contain assignment expressions
            throw new InternalCompilerError("Unexpected assignment: " + expression);
        }

        @Override
        public void postorder(DBSPLetStatement statement) {
            ValueSource source = Unknown.INSTANCE;
            if (statement.initializer != null) {
                source = this.get(statement.initializer);
            }
            this.setCurrentValue(statement, source);
        }

        @Override
        public VisitDecision preorder(DBSPBlockExpression expression) {
            super.preorder(expression);
            // This one is done in preorder, to record the order of side effects
            for (DBSPStatement stat: expression.contents) {
                stat.accept(this);
            }

            if (expression.lastExpression == null) {
                this.setUnknown(expression);
            } else {
                ValueSource result = this.get(expression.lastExpression);
                this.set(expression, result);
            }
            super.postorder(expression);
            return VisitDecision.STOP;
        }

        @Override
        public void postorder(DBSPCastExpression expression) {
            DBSPType type = expression.getType();
            if (!expression.source.getType().sameTypeIgnoringNullability(type)) {
                this.setUnknown(expression);
                return;
            }
            ValueSource source = this.get(expression.source);
            this.set(expression, source);
        }

        @Override
        public void postorder(DBSPFieldExpression field) {
            ValueSource source = this.get(field.expression);
            if (source.is(Tuple.class)) {
                this.set(field, source.to(Tuple.class).field(field.fieldNo));
            } else {
                this.setUnknown(field);
            }
        }

        @Override
        public void postorder(DBSPUnwrapCustomOrdExpression expression) {
            ValueSource source = this.get(expression.expression);
            this.set(expression, source);
        }

        @Override
        public void postorder(DBSPCustomOrdField field) {
            ValueSource source = this.get(field.expression);
            if (source.is(Tuple.class)) {
                this.set(field, source.to(Tuple.class).field(field.fieldNo));
            } else {
                this.setUnknown(field);
            }
        }

        @Override
        public void postorder(DBSPCloneExpression expression) {
            ValueSource source = this.get(expression.expression);
            this.set(expression, source);
        }

        @Override
        public void postorder(DBSPUnwrapExpression expression) {
            ValueSource source = this.get(expression.expression);
            this.set(expression, source);
        }

        @Override
        public void postorder(DBSPBaseTupleExpression expression) {
            List<ValueSource> sources = new ArrayList<>();
            boolean anyKnown = false;
            if (expression.fields == null) {
                this.set(expression, new Constant(expression.getType().none()));
                return;
            }
            for (DBSPExpression field: expression.fields) {
                ValueSource source = this.get(field);
                sources.add(source);
                anyKnown = anyKnown || !source.isBottom();
            }
            if (!anyKnown) {
                this.setUnknown(expression);
            } else {
                ValueSource tuple = new Tuple(sources);
                this.set(expression, tuple);
            }
        }

        @Override
        public void postorder(DBSPLiteral expression) {
            this.set(expression, new Constant(expression));
        }

        @Override
        public void postorder(DBSPClosureExpression expression) {
            ValueSource result = this.get(expression.body);
            // Technically this should be a function
            this.set(expression, result);
            super.postorder(expression);
        }

        @Override
        public VisitDecision preorder(DBSPClosureExpression expression) {
            super.preorder(expression);
            if (this.context.isEmpty()) {
                // Outermost closure
                for (DBSPParameter param: expression.parameters) {
                    ValueSource initial = Unknown.INSTANCE;
                    if (this.initialValues.containsKey(param)) {
                        initial = Utilities.getExists(this.initialValues, param);
                    }
                    this.setCurrentValue(param, initial);
                }
            }
            return VisitDecision.CONTINUE;
        }

        Map<DBSPParameter, ValueSource> initialValues;

        @Override
        public void startVisit(IDBSPInnerNode node) {
            this.resolver.apply(node);
            super.startVisit(node);
        }

        public ValueSource analyze(DBSPClosureExpression expression, Map<DBSPParameter, ValueSource> initialValues) {
            this.initialValues = initialValues;
            return Objects.requireNonNull(this.applyAnalysis(expression));
        }
    }
}
