package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPositiveOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Lineage;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dbsp.sqlCompiler.ir.type.IndexedShape;
import org.dbsp.sqlCompiler.ir.type.CollectionShape;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Part;

/** Finds, for each output port, column sets that are keys of the collection produced by the port.
 * The values of a key identify the row.  A collection with a key has no duplicate rows.
 * The analysis is conservative: any key must be a true key, but not all keys may be detected.
 * Reporting "no keys" is always safe. */
public class KeyAnalysis extends CircuitVisitor {
    /** What is known about the collection an output port produces: its keys, and which of
     * its columns hold the same data. */
    private record PortKeys(Keys keys, ColumnEquivalence equivalence) {}

    private final Map<OutputPort, PortKeys> keys = new HashMap<>();

    public KeyAnalysis(DBSPCompiler compiler) {
        super(compiler);
    }

    public Keys getKeys(OutputPort port) {
        PortKeys known = this.keys.get(port);
        return known == null ? Keys.NONE : known.keys();
    }

    /** Which columns of {@code port} hold the same data. */
    private ColumnEquivalence getEquivalence(OutputPort port) {
        PortKeys known = this.keys.get(port);
        return known == null ? ColumnEquivalence.NONE : known.equivalence();
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.keys.clear();
        return super.startVisit(node);
    }

    /** The symbolic value the interpreter starts from for (a part of) a closure parameter:
     * a tuple with one atom per column, each atom holding the column's {@link Provenance.Source}.
     * @param type       Type of the value to build: the parameter's type, or of one half of it.
     * @param parameter  Index of the parameter within the closure.
     * @param part       Which part of the parameter is represented. */
    private static Lineage.ValueSource initialValue(DBSPType type, int parameter, Part part) {
        if (type.is(DBSPTypeRef.class))
            return new Lineage.Ref(initialValue(type.deref(), parameter, part));
        if (type.is(DBSPTypeRawTuple.class)) {
            // The (index, value) pair of an indexed Z-set
            DBSPTypeRawTuple raw = type.to(DBSPTypeRawTuple.class);
            if (part != Part.NONE || raw.size() != 2)
                return Lineage.Unknown.INSTANCE;
            return new Lineage.Tuple(List.of(
                    initialValue(raw.tupFields[0], parameter, Part.INDEX), initialValue(raw.tupFields[1], parameter, Part.VALUE)));
        }
        if (type.is(DBSPTypeTuple.class)) {
            List<Lineage.ValueSource> fields = new ArrayList<>();
            for (int i = 0; i < type.to(DBSPTypeTuple.class).size(); i++)
                fields.add(new Lineage.Atom<>(new Provenance.SourceSet(
                        new Provenance.Source(parameter, Column.of(part, i)))));
            return new Lineage.Tuple(fields);
        }
        return Lineage.Unknown.INSTANCE;
    }

    /** The source of one output column, if the column is a plain copy of an input column. */
    @Nullable
    private static Provenance.Source sourceOf(Lineage.ValueSource value) {
        if (value.is(Lineage.Ref.class))
            value = value.to(Lineage.Ref.class).value();
        if (!value.is(Lineage.Atom.class))
            return null;
        if (!(value.to(Lineage.Atom.class).value() instanceof Provenance.SourceSet sources))
            return null;
        return sources.single();
    }

    /** The symbolic values the interpreter starts from: one per closure parameter. */
    private static Map<DBSPParameter, Lineage.ValueSource> initialValues(DBSPClosureExpression closure) {
        Map<DBSPParameter, Lineage.ValueSource> initial = new HashMap<>();
        for (int i = 0; i < closure.parameters.length; i++)
            initial.put(closure.parameters[i], initialValue(closure.parameters[i].getType(), i, Part.NONE));
        return initial;
    }

    /** The fields of a symbolic tuple value of the given size. Result is all "unknown" when the
     * interpreter did not produce a tuple of that size. */
    private static List<Lineage.ValueSource> tupleFields(Lineage.ValueSource value, int size) {
        if (value.is(Lineage.Ref.class))
            value = value.to(Lineage.Ref.class).value();
        if (value.is(Lineage.Tuple.class) && value.to(Lineage.Tuple.class).size() == size)
            return value.to(Lineage.Tuple.class).fields();
        List<Lineage.ValueSource> result = new ArrayList<>();
        for (int i = 0; i < size; i++)
            result.add(Lineage.Unknown.INSTANCE);
        return result;
    }

    /** The provenance of the rows produced by a closure: for each column that is a
     * plain copy of a parameter column, that parameter column.
     * @param outputShape shape of the produced rows */
    private Provenance provenance(CollectionShape outputShape, DBSPClosureExpression closure) {
        Lineage.ValueSource result = new Lineage.InnerLineage(this.compiler(), null).analyze(closure, initialValues(closure));
        List<Lineage.ValueSource> columns = new ArrayList<>();
        if (closure.body.getType().is(DBSPTypeRawTuple.class)) {
            // A (index, value) pair; either part may be unknown to the interpreter, e.g. when empty
            Utilities.enforce(outputShape instanceof IndexedShape,
                    () -> "Closure produces pairs but shape is " + outputShape);
            IndexedShape indexed = (IndexedShape) outputShape;
            List<Lineage.ValueSource> parts = tupleFields(result, 2);
            columns.addAll(tupleFields(parts.get(0), indexed.indexFields()));
            columns.addAll(tupleFields(parts.get(1), indexed.valueFields()));
        } else {
            columns.addAll(tupleFields(result, outputShape.width()));
        }
        Map<Column, Provenance.Source> sources = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            Provenance.Source source = sourceOf(columns.get(i));
            if (source != null)
                sources.put(outputShape.output(i), source);
        }
        return new Provenance(sources);
    }

    /** The expression each output column is built from, in the column order of {@code shape}.
     * If the closure has an unexpected shape, the expression may be null. */
    private static List<DBSPExpression> outputExpressions(
            CollectionShape shape, DBSPClosureExpression closure) {
        List<DBSPExpression> result = new ArrayList<>();
        if (shape instanceof IndexedShape indexed) {
            DBSPRawTupleExpression pair = closure.body.as(DBSPRawTupleExpression.class);
            if (pair == null || pair.size() != 2)
                return unknownExpressions(shape.width());
            result.addAll(fieldExpressions(pair.get(0), indexed.indexFields()));
            result.addAll(fieldExpressions(pair.get(1), indexed.valueFields()));
        } else {
            result.addAll(fieldExpressions(closure.body, shape.width()));
        }
        return result;
    }

    /** The fields of a tuple expression, all null when {@code expression} is not a tuple
     * of {@code size} fields. */
    private static List<DBSPExpression> fieldExpressions(DBSPExpression expression, int size) {
        if (expression.is(DBSPSomeExpression.class))
            expression = expression.to(DBSPSomeExpression.class).expression;
        DBSPBaseTupleExpression tuple = expression.as(DBSPBaseTupleExpression.class);
        if (tuple == null || tuple.fields == null || tuple.size() != size)
            return unknownExpressions(size);
        List<DBSPExpression> result = new ArrayList<>();
        for (int i = 0; i < size; i++)
            result.add(tuple.get(i));
        return result;
    }

    private static List<DBSPExpression> unknownExpressions(int size) {
        List<DBSPExpression> result = new ArrayList<>();
        for (int i = 0; i < size; i++)
            result.add(null);
        return result;
    }

    /** Whether two expressions compute the same value.  Both are toplevel field
     * accesses of the body of {@code closure}, so they are evaluated in the same contex. */
    private static boolean sameValue(
            DBSPClosureExpression closure, DBSPExpression left, DBSPExpression right) {
        EquivalenceContext context = new EquivalenceContext();
        context.leftDeclaration.newContext();
        context.rightDeclaration.newContext();
        for (DBSPParameter parameter : closure.parameters) {
            context.leftDeclaration.substitute(parameter.getName(), parameter);
            context.rightDeclaration.substitute(parameter.getName(), parameter);
            context.leftToRight.substitute(parameter, parameter);
        }
        return context.equivalent(left, right);
    }

    /** Output columns that hold the same value: those computed by equal expressions.
     * E.g., SELECT f(x), f(x). */
    private static ColumnEquivalence outputEquivalence(
            CollectionShape shape, DBSPClosureExpression closure, Provenance provenance) {
        List<DBSPExpression> expressions = outputExpressions(shape, closure);
        List<DBSPExpression> computations = new ArrayList<>();
        List<List<Column>> groups = new ArrayList<>();
        for (int i = 0; i < expressions.size(); i++) {
            DBSPExpression expression = expressions.get(i);
            if (expression == null)
                continue;
            int group = -1;
            for (int j = 0; j < computations.size(); j++) {
                if (sameValue(closure, computations.get(j), expression)) {
                    group = j;
                    break;
                }
            }
            if (group < 0) {
                computations.add(expression);
                groups.add(new ArrayList<>());
                group = groups.size() - 1;
            }
            groups.get(group).add(shape.output(i));
        }
        return provenance.equivalence().merge(ColumnEquivalence.of(groups));
    }

    /** Record the keys of an operator's output, no two columns of which hold the same data. */
    private void set(DBSPSimpleOperator operator, Keys found) {
        this.set(operator, found, ColumnEquivalence.NONE);
    }

    /** Record what is known about an operator's output.
     * @param equivalence which columns of the output hold the same data */
    private void set(DBSPSimpleOperator operator, Keys found, ColumnEquivalence equivalence) {
        OutputPort port = operator.outputPort();
        CollectionShape shape = port.getShape();
        if (shape == null)
            return;
        if (found.isEmpty() && equivalence.isEmpty())
            return;
        for (KeyColumns key : found.keys)
            for (EquivalentColumnSet set : key.sets())
                for (Column column : set.columns())
                    Utilities.enforce(shape.contains(column),
                            () -> "Column " + column + " does not fit " + shape + " of " + operator);
        if (!found.isEmpty())
            Logger.INSTANCE.belowLevel(this, 1)
                    .appendSupplier(operator::getIdString)
                    .append(" ")
                    .appendSupplier(() -> operator.operation)
                    .append(" keys ")
                    .appendSupplier(found::toString)
                    .newline();
        this.keys.put(port, new PortKeys(found, equivalence));
    }

    /** The function an operator applies to its rows, when it has one and it is a closure.
     * Some operators carry another kind of expression, a comparator for instance. */
    @Nullable
    private static DBSPClosureExpression closureOf(DBSPSimpleOperator operator) {
        if (operator.function == null || !operator.function.is(DBSPClosureExpression.class))
            return null;
        return operator.function.to(DBSPClosureExpression.class);
    }

    /** The output carries the same rows as the input. */
    private void copy(DBSPUnaryOperator operator) {
        OutputPort input = operator.input();
        this.set(operator, this.getKeys(input), this.getEquivalence(input));
    }

    /** Keys of a map or map_index output: the input keys whose columns are all copied. */
    private void project(DBSPUnaryOperator operator, DBSPClosureExpression closure) {
        CollectionShape input = operator.input().getShape();
        CollectionShape output = operator.outputPort().getShape();
        if (input == null || output == null)
            return;
        Provenance provenance = this.provenance(output, closure);
        ColumnCopyTransform transform = column -> provenance.columnsReading(Provenance.Source.reading(column));
        Keys keys = this.getKeys(operator.input()).map(transform);
        ColumnEquivalence inputEquivalence = this.getEquivalence(operator.input()).after(transform);
        ColumnEquivalence equivalence = outputEquivalence(output, closure, provenance).merge(inputEquivalence);
        this.set(operator, keys, equivalence);
    }

    /** A table with a PRIMARY KEY is keyed by it, whichever operator implements the table. */
    @Override
    public void postorder(DBSPSourceMultisetOperator node) {
        List<Column> key = new ArrayList<>();
        List<InputColumnMetadata> columns = new ArrayList<>(node.metadata.getColumns());
        for (int i = 0; i < columns.size(); i++)
            if (columns.get(i).isPrimaryKey)
                key.add(Column.none(i));
        if (key.isEmpty())
            return;
        this.set(node, Keys.of(ColumnEquivalence.NONE.keyOf(key)));
    }

    /** Source maps are created by a later pass. */
    @Override
    public void postorder(DBSPSourceMapOperator node) {
        throw new InternalCompilerError("Unexpected operator", node);
    }

    /** A filter keeps the input keys.  A column that a top-level conjunct equates with a
     * constant has the same value on every remaining row, so keys no longer need it. */
    @Override
    public void postorder(DBSPFilterOperator node) {
        Keys keys = this.getKeys(node.input());
        Set<Column> fixed = this.fixedColumns(node);
        if (!fixed.isEmpty())
            keys = keys.without(fixed);
        this.set(node, keys, this.getEquivalence(node.input()));
    }

    /** Columns of the filter's input that the condition equates with a constant. */
    private Set<Column> fixedColumns(DBSPFilterOperator node) {
        Set<Column> result = new HashSet<>();
        CollectionShape input = node.input().getShape();
        DBSPClosureExpression closure = closureOf(node);
        if (input == null || closure == null)
            return result;
        Lineage.InnerLineage inner = new Lineage.InnerLineage(this.compiler(), null);
        inner.analyze(closure, initialValues(closure));
        for (DBSPExpression conjunct : closure.body.conjuncts()) {
            conjunct = conjunct.stripWrapBool();
            if (!conjunct.is(DBSPBinaryExpression.class))
                continue;
            DBSPBinaryExpression comparison = conjunct.to(DBSPBinaryExpression.class);
            if (comparison.opcode != DBSPOpcode.EQ)
                continue;
            Lineage.ValueSource left = inner.getN(comparison.left);
            Lineage.ValueSource right = inner.getN(comparison.right);
            if (left == null || right == null)
                continue;
            Lineage.ValueSource column = right.is(Lineage.Constant.class) ? left :
                    left.is(Lineage.Constant.class) ? right : null;
            if (column == null)
                continue;
            Provenance.Source source = sourceOf(column);
            Column fixed = source == null ? null : source.inputColumn(input);
            if (fixed != null)
                result.add(fixed);
        }
        return result;
    }

    /** Every row of a sum or a difference is a row of one of its inputs.  No key of a single
     * input identifies such a row, but an equality holding in every input holds here too.
     * With one input the operator is the identity. */
    private void rowUnion(DBSPSimpleOperator node) {
        if (node.inputs.size() == 1) {
            this.copy(node.to(DBSPUnaryOperator.class));
            return;
        }
        ColumnEquivalence equivalence = this.getEquivalence(node.inputs.get(0));
        for (int i = 1; i < node.inputs.size(); i++)
            equivalence = equivalence.intersect(this.getEquivalence(node.inputs.get(i)));
        this.set(node, Keys.NONE, equivalence);
    }

    @Override
    public void postorder(DBSPSumOperator node) {
        this.rowUnion(node);
    }

    @Override
    public void postorder(DBSPSubtractOperator node) {
        this.rowUnion(node);
    }

    @Override
    public void postorder(DBSPNoopOperator node) {
        this.copy(node);
    }

    /** Negating weights leaves every row in place. */
    @Override
    public void postorder(DBSPNegateOperator node) {
        this.copy(node);
    }

    /** Keeps the rows with a positive weight and drops the others, leaving the rest in place. */
    @Override
    public void postorder(DBSPPositiveOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPViewBaseOperator node) {
        this.copy(node);
        Logger.INSTANCE.belowLevel(this, 1)
                .append("view ")
                .appendSupplier(() -> node.viewName.name())
                .append(" keys ")
                .appendSupplier(() -> this.getKeys(node.input()).toString())
                .newline();
    }

    @Override
    public void postorder(DBSPIntegrateOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPDifferentiateOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPDelayOperator node) {
        this.copy(node);
    }

    @Override
    public void postorder(DBSPDelayedIntegralOperator node) {
        this.copy(node);
    }

    /** The output is a subset of the left input. */
    @Override
    public void postorder(DBSPAntiJoinOperator node) {
        this.set(node, this.getKeys(node.left()), this.getEquivalence(node.left()));
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator node) {
        this.set(node, this.getKeys(node.left()), this.getEquivalence(node.left()));
    }

    private void distinct(DBSPUnaryOperator node) {
        CollectionShape shape = node.outputPort().getShape();
        if (shape == null)
            return;
        ColumnEquivalence equivalence = this.getEquivalence(node.input());
        this.set(node, this.getKeys(node.input()).plus(equivalence.keyOf(shape.columns())), equivalence);
    }

    @Override
    public void postorder(DBSPDistinctOperator node) {
        this.distinct(node);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator node) {
        this.distinct(node);
    }

    /** One row per group.  The group columns are the index of both the input and the output,
     * so an equality among them holds here too; the value is replaced by the aggregate, so
     * an equality naming a value column does not survive. */
    private void aggregate(DBSPSimpleOperator node) {
        if (!(node.outputPort().getShape() instanceof IndexedShape shape))
            return;
        ColumnEquivalence equivalence = ColumnEquivalence.NONE;
        if (node.inputs.get(0).getShape() instanceof IndexedShape input
                && input.indexFields() == shape.indexFields()) {
            ColumnCopyTransform keepsTheIndex =
                    column -> column.part() == Part.INDEX ? List.of(column) : List.of();
            equivalence = this.getEquivalence(node.inputs.get(0)).after(keepsTheIndex);
        }
        this.set(node, Keys.of(equivalence.keyOf(shape.indexColumns())), equivalence);
    }

    @Override
    public void postorder(DBSPAggregateOperatorBase node) {
        this.aggregate(node);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator node) {
        this.aggregate(node);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessRetainKeysOperator node) {
        this.aggregate(node);
    }

    /** Computes one aggregate value per group from a delta and the integral of the input. */
    @Override
    public void postorder(DBSPPrimitiveAggregateOperator node) {
        this.aggregate(node);
    }

    /** Folds each group into one value, for append-only inputs. */
    @Override
    public void postorder(DBSPChainAggregateOperator node) {
        this.aggregate(node);
    }

    @Override
    public void postorder(DBSPMapOperator node) {
        DBSPClosureExpression closure = closureOf(node);
        if (closure != null)
            this.project(node, closure);
    }

    @Override
    public void postorder(DBSPMapIndexOperator node) {
        DBSPClosureExpression closure = closureOf(node);
        if (closure != null)
            this.project(node, closure);
    }

    /** Keeps the value fields only; keys on the index survive through their value copies. */
    @Override
    public void postorder(DBSPDeindexOperator node) {
        // Value columns become the columns of the output rows; index columns are dropped
        ColumnCopyTransform transform = column ->
                column.part() == Part.VALUE ? List.of(Column.none(column.field())) : List.of();
        Keys keys = this.getKeys(node.input()).map(transform);
        ColumnEquivalence equivalence = this.getEquivalence(node.input()).after(transform);
        this.set(node, keys, equivalence);
    }

    /** The output keeps a subset of the rows of each group; with ROW_NUMBER and a limit
     * of 1 the group's index value identifies the row. */
    @Override
    public void postorder(DBSPIndexedTopKOperator node) {
        if (!(node.outputPort().getShape() instanceof IndexedShape shape))
            return;
        // The producer computes the value tuple from (rank, input value)
        CollectionShape produced = new IndexedShape(0, shape.valueFields());
        Provenance provenance = this.provenance(produced, node.outputProducer);
        // The operator keeps the index; a value column becomes what the producer copies it to
        ColumnCopyTransform transform = column -> column.part() == Part.INDEX ?
                List.of(column) :
                provenance.columnsReading(new Provenance.Source(1, Column.none(column.field())));
        ColumnEquivalence inputEquivalence = this.getEquivalence(node.input()).after(transform);
        ColumnEquivalence equivalence =
                outputEquivalence(produced, node.outputProducer, provenance).merge(inputEquivalence);
        Keys result = this.getKeys(node.input()).map(transform);
        boolean rowNumber = node.numbering == DBSPIndexedTopKOperator.Numbering.ROW_NUMBER;
        boolean limitOne = node.limit.is(DBSPUSizeLiteral.class) &&
                node.limit.to(DBSPUSizeLiteral.class).value.equals(BigInteger.ONE);
        boolean single = rowNumber && limitOne;
        if (single)
            result = result.plus(equivalence.keyOf(shape.indexColumns()));
        this.set(node, result, equivalence);
    }

    /** Where the columns of one join input end up in the output.  A column of the shared
     * index is read through parameter 0, a value column through the parameter carrying that
     * input's values. */
    private static ColumnCopyTransform sideTransform(Provenance provenance, int valueParameter) {
        return column -> provenance.columnsReading(new Provenance.Source(
                column.part() == Part.INDEX ? 0 : valueParameter, Column.none(column.field())));
    }

    /** Joins: the join function receives (index, left value, right value).  A pair of keys,
     * one per side, is a key of the output.  When the index covers a key of the right
     * input every left row matches at most one right row, so the left keys survive alone;
     * symmetrically for inner joins, but not for left joins, whose unmatched rows all
     * carry NULLs in the right columns. */
    @Override
    public void postorder(DBSPJoinBaseOperator node) {
        if (node.is(DBSPAsofJoinOperator.class) || node.is(DBSPConcreteAsofJoinOperator.class) ||
                node.is(DBSPJoinFilterMapOperator.class) || node.is(DBSPLeftJoinFilterMapOperator.class))
            return;
        boolean leftJoin = node.is(DBSPLeftJoinOperator.class) || node.is(DBSPLeftJoinIndexOperator.class);
        CollectionShape output = node.outputPort().getShape();
        if (output == null)
            return;
        DBSPClosureExpression closure = closureOf(node);
        if (closure == null)
            return;
        Provenance provenance = this.provenance(output, closure);
        Keys left = this.getKeys(node.left());
        Keys right = this.getKeys(node.right());
        // Parameter 0 is the shared index, parameters 1 and 2 the left and right values
        ColumnCopyTransform leftTransform = sideTransform(provenance, 1);
        ColumnCopyTransform rightTransform = sideTransform(provenance, 2);
        ColumnEquivalence leftEquivalence = this.getEquivalence(node.left()).after(leftTransform);
        ColumnEquivalence rightEquivalence = this.getEquivalence(node.right()).after(rightTransform);
        ColumnEquivalence equivalence = outputEquivalence(output, closure, provenance)
                .merge(leftEquivalence).merge(rightEquivalence);
        Keys leftMapped = left.map(leftTransform);
        Keys rightMapped = right.map(rightTransform);
        List<KeyColumns> result = new ArrayList<>();
        boolean rightMatchesOnce = right.hasKeyWithinIndex();
        boolean leftMatchesOnce = left.hasKeyWithinIndex();
        if (rightMatchesOnce)
            result.addAll(leftMapped.keys);
        if (!leftJoin && leftMatchesOnce)
            result.addAll(rightMapped.keys);
        result.addAll(Keys.combinations(List.of(leftMapped, rightMapped)).keys);
        this.set(node, Keys.of(result), equivalence);
    }

    /** A global aggregate produces exactly one row, the aggregate value or the zero for an
     * empty input.  A collection of at most one row is identified by a key with no values. */
    @Override
    public void postorder(DBSPAggregateZeroOperator node) {
        this.set(node, Keys.of(KeyColumns.SINGLE_ROW));
    }

    /** A star join has N inputs sharing one index; its function receives the shared index and
     * the value of each input.  One key from every input, together, identifies an output row.
     * An input's key identifies a row on its own when the shared index covers a key of every
     * other input, since each of those then contributes at most one row. */
    @Override
    public void postorder(DBSPStarJoinBaseOperator node) {
        if (node.is(DBSPStarJoinFilterMapOperator.class))
            // The function returns an option, so its body does not describe the output rows
            return;
        CollectionShape output = node.outputPort().getShape();
        if (output == null)
            return;
        DBSPClosureExpression closure = closureOf(node);
        if (closure == null)
            return;
        Provenance provenance = this.provenance(output, closure);
        ColumnEquivalence equivalence = outputEquivalence(output, closure, provenance);
        List<Keys> mapped = new ArrayList<>();
        for (int i = 0; i < node.inputs.size(); i++) {
            OutputPort input = node.inputs.get(i);
            // Parameter 0 is the shared index, parameter i + 1 the value of input i
            ColumnCopyTransform transform = sideTransform(provenance, i + 1);
            equivalence = equivalence.merge(this.getEquivalence(input).after(transform));
            mapped.add(this.getKeys(input).map(transform));
        }
        List<KeyColumns> result = new ArrayList<>();
        for (int i = 0; i < node.inputs.size(); i++) {
            boolean othersMatchOnce = true;
            for (int j = 0; j < node.inputs.size(); j++) {
                boolean otherKeyedByIndex = this.getKeys(node.inputs.get(j)).hasKeyWithinIndex();
                if (i != j && !otherKeyedByIndex) {
                    othersMatchOnce = false;
                    break;
                }
            }
            if (othersMatchOnce)
                result.addAll(mapped.get(i).keys);
        }
        result.addAll(Keys.combinations(mapped).keys);
        this.set(node, Keys.of(result), equivalence);
    }
}
