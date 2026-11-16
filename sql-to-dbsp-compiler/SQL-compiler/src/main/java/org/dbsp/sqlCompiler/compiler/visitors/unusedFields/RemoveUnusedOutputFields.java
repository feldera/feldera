package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
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
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.operator.IMultiOutput;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindUsedOutputFields.Kind;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Removes from the outputs of the operators the fields that {@link FindUsedOutputFields}
 * found unused, in one visit of the circuit.
 *
 * <ul>
 * <li>A Map, MapIndex or Join stops computing the unused fields of its output.</li>
 * <li>Filter, Integrate, Differentiate, Negate, Noop, Sum, Subtract and Delay emit their input
 *     rows; AntiJoin and Window emit the rows of their left input.  The source of these rows
 *     drops the unused fields.  If the source still emits one of these fields, a projection
 *     between the source and the operator drops the field.</li>
 * <li>A Deindex emits the value fields that its readers use.</li>
 * <li>A delta0 of a recursive circuit emits the fields that its source emits after the
 *     visit.</li>
 * <li>The closure of each operator with a narrowed input port is rewritten to read
 *     the fields that remain.</li>
 * </ul> */
public class RemoveUnusedOutputFields extends CircuitCloneVisitor {
    final FindUsedOutputFields used;
    /** Key: an output port of the visited circuit; value: the fields of the port that the
     * replacement of the port carries.  The replacement of a port that is not a key carries
     * all fields. */
    Map<OutputPort, FieldUseMap> kept;

    public RemoveUnusedOutputFields(DBSPCompiler compiler, FindUsedOutputFields used) {
        super(compiler, false);
        this.used = used;
        this.kept = new HashMap<>();
        this.preservesTypes = false;
    }

    /** The type of a collection of the rows that 'function' produces: a ZSet or an
     * IndexedZSet, like 'previous' */
    private DBSPType collectionType(DBSPClosureExpression function, DBSPType previous) {
        DBSPType row = function.getResultType();
        if (previous.is(DBSPTypeZSet.class))
            return new DBSPTypeZSet(row);
        return new DBSPTypeIndexedZSet(row.to(DBSPTypeRawTuple.class));
    }

    /** The function 'function' for parameters that have only the kept fields: the parameters
     * get the narrower types, and each field access in the body gets the new field index.
     * @param keptFields  For each parameter the fields that the new parameter has;
     *                    null if the parameter does not change.
     * @param depth       Depth of the fields removed from the parameters. */
    private DBSPClosureExpression withNarrowedParameters(
            DBSPClosureExpression function, List<FieldUseMap> keptFields, int depth) {
        function = function.ensureTree(this.compiler).to(DBSPClosureExpression.class);
        Utilities.enforce(function.parameters.length == keptFields.size());
        ParameterFieldUse use = new ParameterFieldUse();
        for (int i = 0; i < function.parameters.length; i++) {
            DBSPParameter parameter = function.parameters[i];
            FieldUseMap fields = keptFields.get(i);
            if (fields == null)
                fields = new FieldUseMap(parameter.getType(), true);
            use.add(parameter, fields);
        }
        RewriteFields rewriter = use.getFieldRewriter(this.compiler, depth);
        return rewriter.rewriteClosure(function);
    }

    /** Add a Map or MapIndex that reads 'source' and emits only the fields in 'target'.
     * @param reader        Operator that reads the new operator.
     * @param sourceFields  Fields that 'source' emits; null if it emits all fields.
     * @param target        Fields that the new operator emits.  Both field sets use the field
     *                      numbers of the port that 'source' replaces.
     * @param depth         Depth of the fields removed.
     * @return              The output port of the new operator. */
    private OutputPort addProjection(DBSPSimpleOperator reader, OutputPort source,
                                     @Nullable FieldUseMap sourceFields, FieldUseMap target, int depth) {
        FieldUseMap relative = sourceFields == null ? target : target.relativeTo(sourceFields, depth);
        DBSPClosureExpression projection = Objects.requireNonNull(relative.getProjection(depth));
        int size = projection.getResultType().getToplevelFieldCount();
        DBSPSimpleOperator result;
        if (source.outputType().is(DBSPTypeZSet.class))
            result = new DBSPMapOperator(reader.getRelNode(), projection, source);
        else
            result = new DBSPMapIndexOperator(reader.getRelNode(), projection, source);
        result.addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
        this.addOperator(result);
        return result.outputPort();
    }

    /** Make 'result' the replacement of 'operator' in the new circuit.
     * @param keptOutput  Fields of the output of 'operator' that 'result' emits;
     *                    null if it emits all fields. */
    private void setReplacement(
            DBSPSimpleOperator operator, DBSPSimpleOperator result, @Nullable FieldUseMap keptOutput) {
        this.map(operator, result);
        if (keptOutput != null)
            Utilities.putNew(this.kept, operator.outputPort(), keptOutput);
    }

    /** True if {@link FindUsedOutputFields} handles 'operator' as an operator of kind 'kind' */
    private boolean hasKind(DBSPSimpleOperator operator, Kind kind) {
        return this.used.kind(operator) == kind;
    }

    /** The fields of the output of a MAP or JOIN operator that the replacement computes;
     * null if it computes all fields */
    @Nullable
    private FieldUseMap usedOutput(DBSPSimpleOperator operator) {
        if (!this.used.canNarrowOutput(operator))
            return null;
        return this.used.getUsed(operator.outputPort());
    }

    /** Replace a MAP or JOIN operator.
     * @param keptFields  For each parameter of the function the fields that the parameter
     *                    has after the replacement; null if the parameter does not change.
     * @param depth       Depth of the fields removed from the parameters.
     * @return            False if the operator needs no change. */
    private boolean narrowFunction(DBSPSimpleOperator operator, List<FieldUseMap> keptFields, int depth) {
        FieldUseMap usedOutput = this.usedOutput(operator);
        boolean sameParameters = !Linq.any(keptFields, Objects::nonNull);
        if (sameParameters && usedOutput == null)
            return false;
        DBSPClosureExpression function = operator.getClosureFunction();
        DBSPType outputType = operator.outputType;
        // Drop the unused output fields first: the function reads some fields only to compute them
        if (usedOutput != null) {
            function = this.used.restrictToUsed(function, usedOutput);
            outputType = this.collectionType(function, operator.outputType);
        }
        if (!sameParameters)
            function = this.withNarrowedParameters(function, keptFields, depth);
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        DBSPSimpleOperator result = operator.with(function, outputType, sources, false)
                .to(DBSPSimpleOperator.class);
        this.setReplacement(operator, result, usedOutput);
        return true;
    }

    private boolean narrowMap(DBSPSimpleOperator operator) {
        if (!this.hasKind(operator, Kind.MAP))
            return false;
        OutputPort input = operator.inputs.get(0);
        List<FieldUseMap> keptFields = Arrays.asList(this.kept.get(input));
        return this.narrowFunction(operator, keptFields, FieldUseMap.depth(input));
    }

    /** The fields of the values of an IndexedZSet port that the replacement of the port carries;
     * null if it carries all fields.  Checks that the replacement carries all key fields. */
    @Nullable
    private FieldUseMap keptValueFields(OutputPort port) {
        FieldUseMap fields = this.kept.get(port);
        if (fields == null)
            return null;
        Utilities.enforce(!fields.field(0).hasUnusedFields(),
                () -> "Input " + port + " of a join or antijoin lost key fields: " + fields);
        return fields.field(1);
    }

    private boolean narrowJoin(DBSPSimpleOperator operator) {
        if (!this.hasKind(operator, Kind.JOIN))
            return false;
        // The parameters of the function are the key, the left value and the right value
        List<FieldUseMap> keptFields = Arrays.asList(
                null, this.keptValueFields(operator.inputs.get(0)), this.keptValueFields(operator.inputs.get(1)));
        return this.narrowFunction(operator, keptFields, 1);
    }

    /** The port that the replacement of 'operator' reads in place of 'input': a port of the
     * new circuit that emits exactly the fields in 'target'.  It is the replacement of 'input',
     * or a new projection of it if the replacement emits more fields.
     * @param target  Fields that 'operator' needs from 'input'; null if it needs all fields. */
    private OutputPort keepTargetFields(DBSPSimpleOperator operator, OutputPort input, @Nullable FieldUseMap target) {
        FieldUseMap keptInput = this.kept.get(input);
        OutputPort source = this.mapped(input);
        boolean sameFields = (keptInput == null || target == null) ?
                keptInput == target : keptInput.sameUse(target);
        if (sameFields)
            return source;
        // The source emits fields that 'operator' does not need
        Utilities.enforce(target != null, () -> "Input " + input + " of " + operator +
                " lost fields that the operator needs");
        return this.addProjection(operator, source, keptInput, target, FieldUseMap.depth(input));
    }

    /** Replace a filter: its input gets exactly the fields that the predicate and the readers
     * of the filter use */
    private boolean narrowFilter(DBSPSimpleOperator operator) {
        if (!this.hasKind(operator, Kind.FILTER))
            return false;
        OutputPort input = operator.inputs.get(0);
        FieldUseMap target = this.used.filterInputUse(operator);
        if (target == null && !this.kept.containsKey(input))
            return false;
        OutputPort source = this.keepTargetFields(operator, input, target);
        DBSPClosureExpression predicate = operator.getClosureFunction();
        if (target != null)
            predicate = this.withNarrowedParameters(predicate, Linq.list(target), FieldUseMap.depth(input));
        DBSPSimpleOperator result = operator.with(predicate, source.outputType(), Linq.list(source), false)
                .to(DBSPSimpleOperator.class);
        this.setReplacement(operator, result, target);
        return true;
    }

    /** Replace an Integrate, Differentiate, Negate, Noop, Sum, Subtract or Delay: each input
     * gets exactly the fields that the readers of the operator use */
    private boolean narrowIdentity(DBSPSimpleOperator operator) {
        if (!this.hasKind(operator, Kind.IDENTITY))
            return false;
        // The readers of the operator use these fields, so the operator needs them from each input
        FieldUseMap target = this.used.getUsed(operator.outputPort());
        boolean changed = target != null;
        List<OutputPort> sources = new ArrayList<>();
        for (OutputPort input: operator.inputs) {
            if (this.kept.containsKey(input))
                changed = true;
            sources.add(this.keepTargetFields(operator, input, target));
        }
        if (!changed)
            return false;
        DBSPSimpleOperator result = operator.with(operator.function, sources.get(0).outputType(), sources, true)
                .to(DBSPSimpleOperator.class);
        this.setReplacement(operator, result, target);
        return true;
    }

    /** Replace an antijoin: the output rows are the rows of the left input */
    private boolean narrowAntiJoin(DBSPSimpleOperator operator) {
        if (!this.hasKind(operator, Kind.ANTIJOIN))
            return false;
        OutputPort left = operator.inputs.get(0);
        OutputPort right = operator.inputs.get(1);
        FieldUseMap target = this.used.leftInputUse(operator);
        if (target == null && !this.kept.containsKey(left) && !this.kept.containsKey(right))
            return false;
        OutputPort leftSource = this.keepTargetFields(operator, left, target);
        // The antijoin reads only the keys of its right input, so any value fields are correct.
        // No projection removes them: the right input can share an index with a join.
        this.keptValueFields(right);
        OutputPort rightSource = this.mapped(right);
        DBSPSimpleOperator result = operator.with(
                operator.function, leftSource.outputType(), Linq.list(leftSource, rightSource), true)
                .to(DBSPSimpleOperator.class);
        this.setReplacement(operator, result, target);
        return true;
    }

    /** Replace a window: the output rows are the rows of the left input */
    private boolean narrowWindow(DBSPSimpleOperator operator) {
        if (!this.hasKind(operator, Kind.WINDOW))
            return false;
        OutputPort left = operator.inputs.get(0);
        FieldUseMap target = this.used.leftInputUse(operator);
        if (target == null && !this.kept.containsKey(left))
            return false;
        OutputPort leftSource = this.keepTargetFields(operator, left, target);
        OutputPort bounds = this.mapped(operator.inputs.get(1));
        DBSPSimpleOperator result = operator.with(
                operator.function, leftSource.outputType(), Linq.list(leftSource, bounds), true)
                .to(DBSPSimpleOperator.class);
        this.setReplacement(operator, result, target);
        return true;
    }

    /** Replace a Deindex: the output rows are the values of the input */
    private boolean narrowDeindex(DBSPSimpleOperator operator) {
        if (!this.hasKind(operator, Kind.DEINDEX))
            return false;
        OutputPort input = operator.inputs.get(0);
        FieldUseMap keptInput = this.kept.get(input);
        FieldUseMap target = this.used.getUsed(operator.outputPort());
        if (keptInput == null && target == null)
            return false;
        OutputPort source = this.mapped(input);
        FieldUseMap keptValueFields = keptInput == null ? null : keptInput.field(1);
        if (keptValueFields != null && !keptValueFields.hasUnusedFields())
            keptValueFields = null;
        boolean sameFields = (keptValueFields == null || target == null) ?
                keptValueFields == target : keptValueFields.sameUse(target);
        DBSPSimpleOperator result;
        if (sameFields) {
            // The values of the new input are the rows that the readers use
            result = operator.with(operator.function, operator.outputType, Linq.list(source), true)
                    .to(DBSPSimpleOperator.class);
        } else {
            // Other readers of the input use more value fields: a Map keeps the used ones
            Utilities.enforce(target != null, () -> "Input " + input + " of " + operator +
                    " lost fields that the readers of the operator use");
            FieldUseMap relative = keptValueFields == null ? target : target.relativeTo(keptValueFields, 1);
            DBSPClosureExpression projection = Objects.requireNonNull(relative.getProjection(1));
            DBSPVariablePath row = source.getOutputIndexedZSetType().getKVRefType().var();
            DBSPClosureExpression function = projection.call(row.field(1))
                    .reduce(this.compiler)
                    .closure(row);
            result = new DBSPMapOperator(operator.getRelNode(), function, source)
                    .copyAnnotations(operator);
        }
        this.setReplacement(operator, result, target);
        return true;
    }

    /** Replace a delta0: its output has the fields that its source in the enclosing circuit
     * has after the visit */
    private boolean narrowDelta(DBSPSimpleOperator operator) {
        if (!this.hasKind(operator, Kind.DELTA))
            return false;
        OutputPort input = operator.inputs.get(0);
        FieldUseMap keptInput = this.kept.get(input);
        if (keptInput == null)
            return false;
        OutputPort source = this.mapped(input);
        DBSPSimpleOperator result = operator.with(operator.function, source.outputType(), Linq.list(source), true)
                .to(DBSPSimpleOperator.class);
        this.setReplacement(operator, result, keptInput);
        return true;
    }

    /** The replacement of an input of 'operator' must carry all fields */
    private void checkInputs(DBSPOperator operator) {
        for (OutputPort input: operator.inputs)
            Utilities.enforce(!this.kept.containsKey(input),
                    () -> "Input " + input + " of " + operator + " lost fields that the operator may use");
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        if (!this.narrowMap(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        if (!this.narrowMap(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        if (!this.narrowJoin(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        if (!this.narrowJoin(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        if (!this.narrowJoin(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        if (!this.narrowJoin(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        if (!this.narrowJoin(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator operator) {
        if (!this.narrowJoin(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        if (!this.narrowFilter(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPAntiJoinOperator operator) {
        if (!this.narrowAntiJoin(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        if (!this.narrowAntiJoin(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPWindowOperator operator) {
        if (!this.narrowWindow(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        if (!this.narrowDeindex(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPIntegrateOperator operator) {
        if (!this.narrowIdentity(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDifferentiateOperator operator) {
        if (!this.narrowIdentity(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        if (!this.narrowIdentity(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPNoopOperator operator) {
        if (!this.narrowIdentity(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        if (!this.narrowIdentity(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        if (!this.narrowIdentity(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDelayOperator operator) {
        if (!this.narrowIdentity(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDeltaOperator operator) {
        if (!this.narrowDelta(operator))
            super.postorder(operator);
    }

    @Override
    public void replace(DBSPSimpleOperator operator) {
        this.checkInputs(operator);
        super.replace(operator);
    }

    @Override
    public void replaceMultiOutput(IMultiOutput operator) {
        this.checkInputs(operator.asOperator());
        super.replaceMultiOutput(operator);
    }

    @Override
    public Token startVisit(IDBSPOuterNode circuit) {
        this.kept = new HashMap<>();
        return super.startVisit(circuit);
    }
}
