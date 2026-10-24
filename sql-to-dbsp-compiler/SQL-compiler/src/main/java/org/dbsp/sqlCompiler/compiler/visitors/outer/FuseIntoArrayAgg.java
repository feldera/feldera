package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Expensive;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Fuses the column of a reader that consumes an ARRAY_AGG array into the postprocessing
 * function of that ARRAY_AGG.  This reduces the state stored in the aggregate output
 * integral, and in the integrals of the operators between the aggregate and the reader.
 *
 * The source patterns, where {@code g} and {@code h} are columns of the reader, and {@code empty}
 * holds the results of the aggregates on an empty set:
 * <pre>
 * pattern  := producer -> reader
 * producer := aggregate(.., |acc| to_array(acc), ..)
 *           | window_aggregate(.., |acc| to_array(acc), ..)
 *             -> map_index(|(partition, (order, agg))| ((partition, order), agg.unwrap_or(empty)))
 * reader   := map(|(k, v)| (k, .., h(v.j), .., g(v.i), ..))         or a map_index
 *           | join(|k, l, r| (k, l, .., h(r.j), .., g(r.i), ..))                  the producer can be on either side
 *           | star_join(|k, v0, .., vn| (k, v0, .., h(vs.j), .., g(vs.i), ..))    the producer on input s
 * </pre>
 * A join or star join that reads a producer on several inputs is fused one input per
 * iteration: the pass repeats until the circuit stops changing.
 * The ARRAY_AGG at index {@code i} is fused when exactly
 * <ul>
 * <li>one column {@code g} of the reader reads the array,
 * <li>{@code g} reads nothing else,
 * <li>and {@code g} is expensive, or the result of {@code g} is a scalar of fixed size.
 * </ul>
 * The result, where {@code empty.i} is field {@code i} of {@code empty}:
 * <pre>
 * pattern'  := producer' -> reader'
 * producer' := aggregate(.., |acc| g(to_array(acc)), ..)
 *            | window_aggregate(.., |acc| g(to_array(acc)), ..)
 *              -> map_index(|(partition, (order, agg))| ((partition, order), agg.unwrap_or((.., g(empty.i), ..))))
 * reader'   := map(|(k, v)| (k, .., h(v.j), .., v.i, ..))         or a map_index
 *            | join(|k, l, r| (k, l, .., h(r.j), .., r.i, ..))
 *            | star_join(|k, v0, .., vn| (k, v0, .., h(vs.j), .., vs.i, ..))
 * </pre> */
public class FuseIntoArrayAgg extends Repeat {
    public FuseIntoArrayAgg(DBSPCompiler compiler) {
        super(compiler, iteration(compiler));
    }

    /** One iteration; a join that reads a producer on several inputs fuses one per iteration */
    static CircuitTransform iteration(DBSPCompiler compiler) {
        Graph graph = new Graph(compiler);
        FindFusable find = new FindFusable(compiler, graph.getGraphs());
        // The original aggregate of each fused pair has no reader left
        return new Passes("FuseIntoArrayAgg", compiler,
                graph, find, new Fuse(compiler, find), new DeadCode(compiler, true));
    }

    /** An operator whose function reads the value of a producer through one of its parameters.
     * In the formulas below a map reader has the parameter {@code p : (&k, &v)}, and a join
     * reader the parameters {@code k, v0, .., vn}, with {@code v} the one of the producer input;
     * a prime marks a parameter of the rewritten function. */
    abstract static class Reader {
        final DBSPSimpleOperator operator;
        /** The function of the operator */
        final DBSPClosureExpression function;

        /** The declaration of each variable of the function */
        final ReferenceMap references;

        /** @param operator  Map, map_index, or join whose function is a closure. */
        Reader(DBSPCompiler compiler, DBSPSimpleOperator operator) {
            this.operator = operator;
            this.function = operator.getClosureFunction();
            ResolveReferences resolver = new ResolveReferences(compiler, false);
            resolver.apply(this.function);
            this.references = resolver.reference;
        }

        /** Whether {@code e} is a variable that refers to {@code parameter} */
        boolean isParameter(DBSPExpression e, DBSPParameter parameter) {
            return e.is(DBSPVariablePath.class) &&
                    this.references.get(e.to(DBSPVariablePath.class)) == parameter;
        }

        /** The output port of the producer that this reader consumes: the aggregate itself, or
         * the map_index that follows a window aggregate */
        abstract OutputPort producerOutput();

        /** Whether {@code e} is the variable, or the field access on the pair variable, that denotes
         * the producer value; an expression derived from it does not match: {@code e == p} or
         * {@code e == p.1} for a map, {@code e == v} for a join */
        abstract boolean isValue(DBSPExpression e);

        /** Whether {@code e} is the variable, or the field access on the pair variable, that denotes
         * a parameter other than the producer value.  The rewrite keeps such a parameter, so any
         * column but {@code g} may depend on it: {@code e == p.0} for a map, {@code e == k} or
         * {@code e == vj} with {@code vj != v} for a join */
        abstract boolean isOther(DBSPExpression e);

        /** The argument list for calling the function with {@code value} as the producer value and
         * a {@link NoExpression} for every other parameter */
        abstract DBSPExpression[] argumentsWithValue(DBSPExpression value);

        /** Creates the parameters of the rewritten function, which reads the fused producer:
         * fresh parameters with the types of the function's parameters, except that the producer
         * value has type {@code valueType}: {@code [p' : (&k, &valueType)]} for a map,
         * {@code [k', v0', .., vn']} with {@code v' : &valueType} for a join */
        abstract DBSPParameter[] parameters(DBSPType valueType);

        /** Builds the expression of the rewritten function that reads field {@code i} of the
         * producer value, the result of the aggregate at index {@code i}: {@code (*p'.1).i} for a
         * map, {@code (*v').i} for a join */
        abstract DBSPExpression valueField(DBSPParameter[] parameters, int i);

        /** The argument list for calling the original function with {@code parameters},
         * but with the parameter corresponding to the producer replaced
         * by {@code value}.  The rewritten function is {@code |parameters| original(arguments)},
         * which substitutes the arguments for the original parameters. */
        abstract DBSPExpression[] arguments(DBSPParameter[] parameters, DBSPExpression value);

        /** Rebuilds the operator with the rewritten function, with the producer input from
         * {@code producerOutput} */
        abstract DBSPSimpleOperator with(DBSPClosureExpression function, OutputPort producerOutput,
                                         CircuitCloneVisitor visitor);
    }

    /** A map or map_index over an indexed Z-set; its function has one parameter, of type
     * {@code (&key, &value)}, the references to the key and to the value */
    static class MapReader extends Reader {
        final DBSPParameter param;

        MapReader(DBSPCompiler compiler, DBSPUnaryOperator operator) {
            super(compiler, operator);
            Utilities.enforce(this.function.parameters.length == 1,
                    () -> "Map with " + this.function.parameters.length + " parameters: " + operator);
            this.param = this.function.parameters[0];
            Utilities.enforce(this.param.getType().is(DBSPTypeRawTuple.class) &&
                    this.param.getType().to(DBSPTypeRawTuple.class).size() == 2,
                    () -> "Map parameter is not a (key, value) pair: " + this.param.getType());
        }

        /** {@code &k}, the type of the key half of the parameter */
        DBSPType keyType() {
            return this.param.getType().to(DBSPTypeRawTuple.class).tupFields[0];
        }

        @Override
        OutputPort producerOutput() {
            return this.operator.to(DBSPUnaryOperator.class).input();
        }

        @Override
        boolean isValue(DBSPExpression expression) {
            if (this.isParameter(expression, this.param))
                return true;
            return expression.is(DBSPFieldExpression.class) &&
                    expression.to(DBSPFieldExpression.class).fieldNo == 1 &&
                    this.isParameter(expression.to(DBSPFieldExpression.class).expression, this.param);
        }

        @Override
        boolean isOther(DBSPExpression expression) {
            return expression.is(DBSPFieldExpression.class) &&
                    expression.to(DBSPFieldExpression.class).fieldNo == 0 &&
                    this.isParameter(expression.to(DBSPFieldExpression.class).expression, this.param);
        }

        @Override
        DBSPExpression[] argumentsWithValue(DBSPExpression value) {
            return new DBSPExpression[] {
                    new DBSPRawTupleExpression(new NoExpression(this.keyType()), value) };
        }

        @Override
        DBSPParameter[] parameters(DBSPType valueType) {
            return new DBSPParameter[] { new DBSPTypeRawTuple(this.keyType(), valueType.ref()).var().asParameter() };
        }

        @Override
        DBSPExpression valueField(DBSPParameter[] parameters, int i) {
            return parameters[0].asVariable().field(1).deref().field(i);
        }

        @Override
        DBSPExpression[] arguments(DBSPParameter[] parameters, DBSPExpression value) {
            return new DBSPExpression[] { new DBSPRawTupleExpression(parameters[0].asVariable().field(0), value) };
        }

        @Override
        DBSPSimpleOperator with(DBSPClosureExpression function, OutputPort producerOutput,
                                CircuitCloneVisitor visitor) {
            DBSPSimpleOperator result;
            if (this.operator.is(DBSPMapOperator.class))
                result = new DBSPMapOperator(this.operator.getRelNode(), function, producerOutput);
            else
                result = new DBSPMapIndexOperator(this.operator.getRelNode(), function, producerOutput);
            return result.copyAnnotations(this.operator);
        }
    }

    /** A join or star join, whose parameters are the key and the values of its inputs,
     * reading the producer on input {@code inputNo} */
    static class JoinReader extends Reader {
        /** The input fed by the producer */
        final int inputNo;
        /** The parameter {@code v} of the function, the value of input {@code inputNo} */
        final DBSPParameter value;

        JoinReader(DBSPCompiler compiler, DBSPSimpleOperator join, int inputNo) {
            super(compiler, join);
            Utilities.enforce(0 <= inputNo && inputNo < join.inputs.size(), () -> "No input " + inputNo + " in " + join);
            Utilities.enforce(this.function.parameters.length == join.inputs.size() + 1,
                    () -> "Join with " + join.inputs.size() + " inputs and " +
                            this.function.parameters.length + " parameters: " + join);
            this.inputNo = inputNo;
            this.value = this.function.parameters[1 + inputNo];
        }

        @Override
        OutputPort producerOutput() {
            return this.operator.inputs.get(this.inputNo);
        }

        @Override
        boolean isValue(DBSPExpression expression) {
            return this.isParameter(expression, this.value);
        }

        @Override
        boolean isOther(DBSPExpression expression) {
            for (DBSPParameter parameter : this.function.parameters)
                if (parameter != this.value && this.isParameter(expression, parameter))
                    return true;
            return false;
        }

        @Override
        DBSPExpression[] argumentsWithValue(DBSPExpression value) {
            DBSPExpression[] result = new DBSPExpression[this.function.parameters.length];
            for (int j = 0; j < result.length; j++) {
                DBSPParameter parameter = this.function.parameters[j];
                result[j] = parameter == this.value ? value : new NoExpression(parameter.getType());
            }
            return result;
        }

        @Override
        DBSPParameter[] parameters(DBSPType valueType) {
            DBSPParameter[] result = new DBSPParameter[this.function.parameters.length];
            for (int j = 0; j < result.length; j++) {
                DBSPParameter parameter = this.function.parameters[j];
                result[j] = (parameter == this.value ? valueType.ref() : parameter.getType()).var().asParameter();
            }
            return result;
        }

        @Override
        DBSPExpression valueField(DBSPParameter[] parameters, int i) {
            return parameters[1 + this.inputNo].asVariable().deref().field(i);
        }

        @Override
        DBSPExpression[] arguments(DBSPParameter[] parameters, DBSPExpression value) {
            DBSPExpression[] result = new DBSPExpression[parameters.length];
            for (int j = 0; j < result.length; j++)
                result[j] = j == 1 + this.inputNo ? value : parameters[j].asVariable();
            return result;
        }

        @Override
        DBSPSimpleOperator with(DBSPClosureExpression function, OutputPort producerOutput,
                                CircuitCloneVisitor visitor) {
            List<OutputPort> inputs = new ArrayList<>();
            for (int j = 0; j < this.operator.inputs.size(); j++)
                inputs.add(j == this.inputNo ? producerOutput : visitor.mapped(this.operator.inputs.get(j)));
            return this.operator.with(function, this.operator.outputType, inputs, false)
                    .to(DBSPSimpleOperator.class).copyAnnotations(this.operator);
        }
    }

    /** An aggregate with a non-empty key, or a window aggregate with the map_index that follows it.
     * @param index  The map_index after a window aggregate; null for a keyed aggregate */
    record Producer(DBSPAggregateOperatorBase aggregate, @Nullable DBSPMapIndexOperator index) {
        /** The aggregate list of the producer */
        DBSPAggregateList list() {
            return Objects.requireNonNull(this.aggregate.aggregateList);
        }

        /** True for the map_index that follows a window aggregate,
         * {@code |p: (&partition, &(order, agg))| (key(partition, order), agg.unwrap_or(empty))}:
         * the key reads anything but the aggregate tuple, the value is the aggregate tuple, or
         * {@code empty} when the window is empty.
         * @param empty  The results of the aggregates of the window on an empty set. */
        static boolean isWindowIndex(DBSPCompiler compiler, DBSPMapIndexOperator index, DBSPExpression empty) {
            DBSPExpression body = index.getClosureFunction().body;
            if (!body.is(DBSPRawTupleExpression.class))
                return false;
            DBSPExpression[] fields = body.to(DBSPRawTupleExpression.class).fields;
            if (fields == null || fields.length != 2 || !fields[1].is(DBSPApplyMethodExpression.class))
                return false;
            DBSPApplyMethodExpression value = fields[1].to(DBSPApplyMethodExpression.class);
            if (!value.getFunctionName().equals("unwrap_or") || value.arguments.length != 1)
                return false;
            ISameValue argument = value.arguments[0].as(ISameValue.class);
            if (argument == null || !argument.sameValue(empty.as(ISameValue.class)))
                return false;
            MapReader reader = new MapReader(compiler, index);
            ParameterUses key = new ParameterUses(compiler, reader);
            key.apply(fields[0]);
            ParameterUses receiver = new ParameterUses(compiler, reader);
            receiver.apply(value.self);
            return key.fieldsOnly() && !key.valueFields.contains(1) && receiver.onlyField(1);
        }

        /** Finds the producer read through {@code port}: an aggregate, or a window aggregate
         * behind its map_index.  Null when there is none, or when an operator on the way has
         * another reader. */
        @Nullable
        static Producer find(DBSPCompiler compiler, OutputPort port, CircuitGraph graph) {
            DBSPOperator node = port.node();
            if (graph.getFanout(node) != 1)
                return null;
            if (node.is(DBSPMapIndexOperator.class)) {
                DBSPMapIndexOperator index = node.to(DBSPMapIndexOperator.class);
                DBSPOperator source = index.input().node();
                if (!source.is(DBSPPartitionedRollingAggregateOperator.class) || graph.getFanout(source) != 1)
                    return null;
                DBSPAggregateOperatorBase window = source.to(DBSPAggregateOperatorBase.class);
                if (window.aggregateList == null || !isWindowIndex(compiler, index, window.aggregateList.getEmptySetResult()))
                    return null;
                return new Producer(window, index);
            }
            if (node.is(DBSPStreamAggregateOperator.class) || node.is(DBSPAggregateOperator.class)) {
                DBSPAggregateOperatorBase aggregate = node.to(DBSPAggregateOperatorBase.class);
                if (aggregate.aggregateList == null)
                    return null;
                // A global aggregate also feeds the operators that produce its empty-input result
                if (aggregate.getOutputIndexedZSetType().keyType.to(DBSPTypeTupleBase.class).size() == 0)
                    return null;
                return new Producer(aggregate, null);
            }
            return null;
        }
    }

    /** A reader, its producer, and the fused columns.
     * @param g  For each fused ARRAY_AGG, the key is the index in the aggregate list,
     *           the value is the column of the reader output computed from its array. */
    record Match(Reader reader, Producer producer, Map<Integer, DBSPExpression> g) {
        /** The type of the producer value: the tuple of the results of its aggregate list */
        DBSPTypeTuple valueType() {
            return this.producer.list().getEmptySetResultType();
        }

        /** Finds the ARRAY_AGG whose column is {@code column}: its index in the aggregate list, or -1 */
        int fusedIndex(DBSPExpression column) {
            for (Map.Entry<Integer, DBSPExpression> entry : this.g.entrySet())
                if (entry.getValue() == column)
                    return entry.getKey();
            return -1;
        }

        /** Builds a reference to the producer value with the given fields; the fields missing
         * are replaced by {@link NoExpression}s */
        DBSPExpression value(Map<Integer, DBSPExpression> fields) {
            // Example: the producer value Tup2<i32?, Array?> with fields {1 -> array} gives &Tup2(_, array)
            DBSPTypeTuple type = this.valueType();
            List<DBSPExpression> all = new ArrayList<>();
            for (int j = 0; j < type.size(); j++)
                all.add(fields.getOrDefault(j, new NoExpression(type.tupFields[j])));
            return new DBSPTupleExpression(all, type.mayBeNull).borrow();
        }

        DBSPExpression call(DBSPCompiler compiler, DBSPExpression body, DBSPExpression[] arguments) {
            return body.closure(this.reader.function.parameters).call(arguments).reduce(compiler);
        }

        /** Computes {@code g(array)} for the ARRAY_AGG at index {@code i}: its column, called on
         * {@code array} alone */
        DBSPExpression g(DBSPCompiler compiler, int i, DBSPExpression array) {
            // Example: g = CARDINALITY(ARRAY_DISTINCT(p.1.1)) at index 1 of a map reader, array = to_array(acc)
            //   value:     &Tup2(_, to_array(acc))
            //   arguments: [(_, &Tup2(_, to_array(acc)))]
            //   result:    CARDINALITY(ARRAY_DISTINCT(to_array(acc)))
            DBSPExpression value = this.value(Map.of(i, array));
            DBSPExpression[] arguments = this.reader.argumentsWithValue(value);
            return this.call(compiler, Utilities.getExists(this.g, i), arguments);
        }
    }

    /** Classifies the uses of the reader inputs in an expression:
     * <ul>
     * <li>a read of a parameter other than the producer value,
     * <li>a read of a field {@code (*value).j} of the producer value,
     * <li>or any other use of the producer value, which disqualifies the expression.
     * </ul>
     * The visit stops at each matched pattern, so the result does not depend on the
     * sharing of nodes in the expression. */
    static class ParameterUses extends InnerVisitor {
        final Reader reader;
        /** The expression reads a parameter other than the producer value */
        boolean others = false;
        /** The fields {@code j} read as {@code (*value).j} */
        final Set<Integer> valueFields = new HashSet<>();
        /** The expression uses the producer value in another way than a field read */
        boolean misused = false;

        ParameterUses(DBSPCompiler compiler, Reader reader) {
            super(compiler);
            this.reader = reader;
        }

        @Override
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPFieldExpression field) {
            if (this.reader.isOther(field)) {
                this.others = true;
                return VisitDecision.STOP;
            }
            if (this.reader.isValue(field)) {
                // The value of a map, reached outside a (*p.1).j
                this.misused = true;
                return VisitDecision.STOP;
            }
            if (field.expression.is(DBSPDerefExpression.class) &&
                    this.reader.isValue(field.expression.to(DBSPDerefExpression.class).expression)) {
                this.valueFields.add(field.fieldNo);
                return VisitDecision.STOP;
            }
            return VisitDecision.CONTINUE;
        }

        @Override
        public VisitDecision preorder(DBSPVariablePath variable) {
            // Reached only outside the patterns above
            if (this.reader.isOther(variable))
                this.others = true;
            else if (this.reader.isValue(variable))
                this.misused = true;
            return VisitDecision.STOP;
        }

        /** True if every use of the value is a field {@code (*value).j} */
        boolean fieldsOnly() {
            return !this.misused;
        }

        /** True if the expression reads only the field {@code (*value).i} */
        boolean onlyField(int i) {
            return !this.misused && !this.others && this.valueFields.equals(Set.of(i));
        }
    }

    /** Finds the readers to fuse */
    static class FindFusable extends CircuitWithGraphsVisitor {
        /** Key: a reader operator; value: its match */
        final Map<DBSPOperator, Match> byReader = new HashMap<>();

        FindFusable(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
        }

        /** True for an ARRAY_AGG, recognized by its semigroup, whose result is an array; the result
         * of a fused ARRAY_AGG is the result of {@code g}.  Another aggregate with the same semigroup
         * would still be rewritten correctly, since the rewrite only composes a function with the
         * postprocessing of a Fold. */
        static boolean isArrayAgg(IAggregate aggregate) {
            if (!aggregate.is(NonLinearAggregate.class) || aggregate.is(MinMaxAggregate.class))
                return false;
            NonLinearAggregate nonLinear = aggregate.to(NonLinearAggregate.class);
            return nonLinear.postProcess != null && nonLinear.semigroup.name.equals("ConcatSemigroup") &&
                    aggregate.getType().is(DBSPTypeArray.class);
        }

        /** True if {@code g} is expensive, or if its result is a scalar of fixed size. */
        boolean worthFusing(DBSPExpression g) {
            return Expensive.isExpensive(this.compiler(), g) || g.getType().is(IsBoundedType.class);
        }

        /** Matches {@code reader} with its producer and the columns to fuse, and records the
         * match; true if matched */
        boolean check(Reader reader) {
            Producer producer = Producer.find(this.compiler(), reader.producerOutput(), this.getGraph());
            if (producer == null)
                return false;
            List<DBSPExpression> columns = DBSPBaseTupleExpression.columns(reader.function.body);
            if (columns == null)
                return false;

            List<ParameterUses> uses = new ArrayList<>();
            for (DBSPExpression column : columns) {
                ParameterUses use = new ParameterUses(this.compiler(), reader);
                use.apply(column);
                if (!use.fieldsOnly())
                    return false;
                uses.add(use);
            }

            // An ARRAY_AGG is fused when one column reads its array, and reads nothing else
            DBSPAggregateList list = producer.list();
            Map<Integer, DBSPExpression> g = new TreeMap<>();
            for (int i = 0; i < list.size(); i++) {
                if (!isArrayAgg(list.aggregates.get(i)))
                    continue;
                List<Integer> readingColumns = new ArrayList<>();
                for (int c = 0; c < columns.size(); c++)
                    if (uses.get(c).valueFields.contains(i))
                        readingColumns.add(c);
                if (readingColumns.size() != 1)
                    continue;
                int column = readingColumns.get(0);
                if (uses.get(column).onlyField(i) && this.worthFusing(columns.get(column)))
                    g.put(i, columns.get(column));
            }
            if (g.isEmpty())
                return false;
            Utilities.putNew(this.byReader, reader.operator, new Match(reader, producer, g));
            return true;
        }

        /** Matches a map or map_index */
        void checkMap(DBSPUnaryOperator map) {
            // A producer is an indexed Z-set
            if (map.getFunction().is(DBSPClosureExpression.class) &&
                    map.input().outputType().is(DBSPTypeIndexedZSet.class))
                this.check(new MapReader(this.compiler(), map));
        }

        /** Matches a join or star join on the first input that has a producer; the next
         * iteration of the pass sees the other inputs */
        void checkJoin(DBSPSimpleOperator join) {
            if (!join.getFunction().is(DBSPClosureExpression.class))
                return;
            for (int inputNo = 0; inputNo < join.inputs.size(); inputNo++)
                if (this.check(new JoinReader(this.compiler(), join, inputNo)))
                    return;
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            this.checkMap(operator);
        }

        @Override
        public void postorder(DBSPMapIndexOperator operator) {
            this.checkMap(operator);
        }

        @Override
        public void postorder(DBSPJoinOperator operator) {
            this.checkJoin(operator);
        }

        @Override
        public void postorder(DBSPStreamJoinOperator operator) {
            this.checkJoin(operator);
        }

        @Override
        public void postorder(DBSPStarJoinOperator operator) {
            this.checkJoin(operator);
        }
    }

    /** Rewrites each matched reader and its producer */
    static class Fuse extends CircuitCloneVisitor {
        /** The matches to rewrite */
        final FindFusable found;

        Fuse(DBSPCompiler compiler, FindFusable found) {
            super(compiler, false);
            this.found = found;
        }

        /** Builds the aggregate list with each fused {@code |acc| post(acc)} rewritten as
         * {@code |acc| g(post(acc))}, with the empty-set result {@code g(empty)} */
        DBSPAggregateList fusedList(Match match) {
            // Example: the list [SUM(c), ARRAY_AGG(c)] with g = CARDINALITY(ARRAY_DISTINCT(..)) at index 1
            //   post:      |acc: Vec<i32?>| to_array(acc)
            //   fusedPost: |acc: Vec<i32?>| CARDINALITY(ARRAY_DISTINCT(to_array(acc)))
            //   empty:     CARDINALITY(ARRAY_DISTINCT(ARRAY[]))
            DBSPAggregateList list = match.producer().list();
            List<IAggregate> aggregates = new ArrayList<>(list.aggregates);
            for (int i : match.g().keySet()) {
                NonLinearAggregate arrayAgg = aggregates.get(i).to(NonLinearAggregate.class);
                DBSPClosureExpression post = Objects.requireNonNull(arrayAgg.postProcess);
                DBSPClosureExpression fusedPost = match.g(this.compiler(), i, post.body).closure(post.parameters);
                DBSPExpression empty = match.g(this.compiler(), i, arrayAgg.emptySetResult);
                aggregates.set(i, new NonLinearAggregate(arrayAgg.getNode(),
                        arrayAgg.zero, arrayAgg.increment, fusedPost, empty, arrayAgg.semigroup));
            }
            return new DBSPAggregateList(list.getNode(), list.rowVar, aggregates);
        }

        /** Builds the fused producer, adds its operators to the circuit, and returns the port the
         * reader reads after the rewrite: the fused aggregate, or the map_index of the fused
         * window aggregate, which is added as well */
        OutputPort fusedProducer(Match match) {
            Producer producer = match.producer();
            DBSPAggregateOperatorBase aggregate = producer.aggregate();
            DBSPAggregateList list = this.fusedList(match);
            DBSPTypeIndexedZSet originalOutputType = aggregate.getOutputIndexedZSetType();
            DBSPTypeTuple fusedValueType = list.getEmptySetResultType();
            OutputPort input = this.mapped(aggregate.input());
            if (producer.index() == null) {
                // Example: the list [SUM(c), ARRAY_AGG(c)] fused at index 1
                //   before: IndexedZSet<Tup1<k>, Tup2<i32?, Array?>>
                //   after:  IndexedZSet<Tup1<k>, Tup2<i32?, i32?>>
                DBSPTypeIndexedZSet fusedOutputType = TypeCompiler.makeIndexedZSet(originalOutputType.keyType, fusedValueType);
                DBSPSimpleOperator fused;
                if (aggregate.is(DBSPStreamAggregateOperator.class))
                    fused = new DBSPStreamAggregateOperator(aggregate.getRelNode(), fusedOutputType, null, list, input);
                else
                    fused = new DBSPAggregateOperator(aggregate.getRelNode(), fusedOutputType, null, list, input);
                fused.copyAnnotations(aggregate);
                this.addOperator(fused);
                return fused.outputPort();
            }

            // The output of a window aggregate holds the order-by column and the nullable aggregate
            // tuple.  Example: CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(c))) OVER (PARTITION BY p ORDER BY ts ..)
            //   before: IndexedZSet<Tup1<p>, Tup2<u64, Tup1<Array>?>>
            //   after:  IndexedZSet<Tup1<p>, Tup2<u64, Tup1<i32?>?>>, the postprocessing computes the cardinality
            DBSPPartitionedRollingAggregateOperator window = aggregate.to(DBSPPartitionedRollingAggregateOperator.class);
            DBSPTypeTuple originalElement = originalOutputType.elementType.to(DBSPTypeTuple.class);
            DBSPTypeIndexedZSet fusedOutputType = TypeCompiler.makeIndexedZSet(originalOutputType.keyType,
                    new DBSPTypeTuple(originalElement.tupFields[0], fusedValueType.withMayBeNull(true)));
            DBSPSimpleOperator fused = new DBSPPartitionedRollingAggregateOperator(window.getRelNode(),
                    window.partitioningFunction, null, list, window.lower, window.upper, fusedOutputType, input)
                    .copyAnnotations(window);
            this.addOperator(fused);

            // The map_index keeps its key, which reads the partition and the order-by column:
            //   before: |q: (&Tup1<p>, &Tup2<u64, Tup1<Array>?>)| ((q.0.0, to_signed(q.1.0)), q.1.1.unwrap_or(empty))
            //   after:  |q: (&Tup1<p>, &Tup2<u64, Tup1<i32?>?>)| ((q.0.0, to_signed(q.1.0)), q.1.1.unwrap_or(fusedEmpty))
            // where fusedEmpty holds the results of the fused aggregates on an empty set
            DBSPMapIndexOperator index = producer.index();
            DBSPClosureExpression indexFunction = index.getClosureFunction();
            DBSPExpression[] body = Objects.requireNonNull(indexFunction.body.to(DBSPRawTupleExpression.class).fields);
            Utilities.enforce(body.length == 2 && indexFunction.parameters.length == 1 && originalElement.size() == 2,
                    () -> "Unexpected window index: " + index);
            // q: (&Tup1<Option<i32>>, &Tup2<u64, Option<Tup1<Option<i32>>>>)
            DBSPVariablePath q = fusedOutputType.getKVRefType().var();
            // &Tup2::new((*q.1).0, _)
            DBSPExpression orderOnly = new DBSPTupleExpression(
                    q.field(1).deref().field(0), new NoExpression(originalElement.tupFields[1])).borrow();
            // Tup2::new((*q.0).0.as_ref().cloned(), UnsignedWrappers::to_signed::<i32, i32, i64, u64>((*q.1).0, true, false))
            DBSPExpression key = body[0].closure(indexFunction.parameters)
                    .call(new DBSPRawTupleExpression(q.field(0), orderOnly))
                    .reduce(this.compiler());
            // (*q.1).1.as_ref().cloned().unwrap_or(Tup1::new(cardinalityVecN(array_distinctN(Some(Arc::new(Vec::<Option<i32>>::new()))))))
            DBSPExpression value = new DBSPApplyMethodExpression(index.getNode(), "unwrap_or", fusedValueType,
                    q.field(1).deref().field(1).applyCloneIfNeeded(), list.getEmptySetResult());
            DBSPSimpleOperator fusedIndex = new DBSPMapIndexOperator(index.getRelNode(),
                    new DBSPRawTupleExpression(key, value).closure(q), fused.outputPort())
                    .copyAnnotations(index);
            this.addOperator(fusedIndex);
            return fusedIndex.outputPort();
        }

        /** Rewrites the reader over its fused producer, adds it to the circuit, and maps the
         * original reader to it: {@code |p| (p.0, .., h(p.1.j), .., g(p.1.i), ..)} rewritten as
         * {@code |p'| (p'.0, .., h(p'.1.j), .., p'.1.i, ..)} */
        void fuse(Match match) {
            // Example: the map |p: (&Tup1<k>, &Tup2<i32?, Array?>)| Tup3::new((*p.0).0, (*p.1).0, cardinalityVecN(array_distinctN((*p.1).1)))
            // over the list [SUM(c), ARRAY_AGG(c)], fused at index 1
            Reader reader = match.reader();
            OutputPort producerOutput = this.fusedProducer(match);
            // [p': (&Tup1<k>, &Tup2<i32?, i32?>)]
            DBSPParameter[] parameters = reader.parameters(producerOutput.getOutputIndexedZSetType().elementType);
            DBSPExpression body = reader.function.body;
            // [(*p.0).0, (*p.1).0, (*p'.1).1]
            List<DBSPExpression> columns = new ArrayList<>();
            for (DBSPExpression column : Objects.requireNonNull(DBSPBaseTupleExpression.columns(body))) {
                int i = match.fusedIndex(column);
                columns.add(i < 0 ? column : reader.valueField(parameters, i).applyCloneIfNeeded());
            }
            // {0 -> (*p'.1).0}
            Map<Integer, DBSPExpression> unchanged = new HashMap<>();
            for (int j = 0; j < match.valueType().size(); j++)
                if (!match.g().containsKey(j))
                    unchanged.put(j, reader.valueField(parameters, j));
            // &Tup2::new((*p'.1).0, _)
            DBSPExpression value = match.value(unchanged);
            // Tup3::new((*p'.0).0, (*p'.1).0, (*p'.1).1): the columns called with (p'.0, value)
            DBSPExpression fusedBody = match.call(this.compiler(),
                    DBSPBaseTupleExpression.withColumns(body, columns), reader.arguments(parameters, value));
            // map(|p': (&Tup1<k>, &Tup2<i32?, i32?>)| Tup3::new((*p'.0).0, (*p'.1).0, (*p'.1).1))
            DBSPSimpleOperator result = reader.with(fusedBody.closure(parameters), producerOutput, this);
            this.map(reader.operator, result);
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            Match match = this.found.byReader.get(operator);
            if (match == null)
                super.postorder(operator);
            else
                this.fuse(match);
        }

        @Override
        public void postorder(DBSPMapIndexOperator operator) {
            Match match = this.found.byReader.get(operator);
            if (match == null)
                super.postorder(operator);
            else
                this.fuse(match);
        }

        @Override
        public void postorder(DBSPJoinOperator operator) {
            Match match = this.found.byReader.get(operator);
            if (match == null)
                super.postorder(operator);
            else
                this.fuse(match);
        }

        @Override
        public void postorder(DBSPStreamJoinOperator operator) {
            Match match = this.found.byReader.get(operator);
            if (match == null)
                super.postorder(operator);
            else
                this.fuse(match);
        }

        @Override
        public void postorder(DBSPStarJoinOperator operator) {
            Match match = this.found.byReader.get(operator);
            if (match == null)
                super.postorder(operator);
            else
                this.fuse(match);
        }
    }
}
