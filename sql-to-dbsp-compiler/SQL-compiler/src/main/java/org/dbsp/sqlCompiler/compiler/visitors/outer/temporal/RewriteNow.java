package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.Documentation;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindSourcePositions;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.InsertLimiters;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalIncrementExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Replace:
 * (1) MAP operators that contain now() as an expression with
 * map operators that take an extra field and use that instead of the now() call.
 * Insert a join prior to such operators (which also requires a MapIndex operator).
 * Also inserts a MapIndex operator to index the 'NOW' built-in table.
 * (2) FILTER operators that involve now(); some are compiled into WINDOW operators,
 * ("temporal filters"), the others are compiled in a similar way with MAP operators.
 */
public class RewriteNow extends CircuitCloneVisitor {
    public static final Documentation.Link NOW_DOCUMENTATION = new Documentation.Link("sql/datetime", "now");
    // Holds the indexed version of the 'now' operator (indexed with an empty key).
    // (actually, it's the differentiator after the index)
    @Nullable
    DBSPSimpleOperator nowIndexed = null;
    @Nullable
    DBSPSimpleOperator now = null;

    /** Rewrites a closure of the form |t: &T| ... now() ...
     * to a closure of the form |t: &T1| ... (*t).last,
     * where T1 is T with an extra Timestamp field, and
     * last is the index of the last field of T1. */
    static class RewriteNowClosure extends InnerRewriteVisitor {
        /** Replacement for the now function */
        @Nullable
        DBSPExpression nowReplacement;
        /** Replacement for the parameter references */
        @Nullable
        DBSPExpression parameterReplacement;
        @Nullable
        ReferenceMap refMap;

        public RewriteNowClosure(DBSPCompiler compiler) {
            super(compiler, false);
            this.nowReplacement = null;
            this.parameterReplacement = null;
            this.refMap = null;
        }

        @Override
        protected boolean rewritesTypes() {
            return false;
        }

        @Override
        public VisitDecision preorder(DBSPApplyExpression expression) {
            // A function call that is `now()` is replaced with the `nowReplacement`
            if (ContainsNow.isNow(expression)) {
                this.map(expression, Objects.requireNonNull(this.nowReplacement));
                return VisitDecision.STOP;
            }
            return super.preorder(expression);
        }

        @Override
        public VisitDecision preorder(DBSPVariablePath var) {
            // A variable that references the parameter is replaced with the `parameterReplacement` expression
            IDBSPDeclaration decl = Objects.requireNonNull(this.refMap).getDeclaration(var);
            if (decl.is(DBSPParameter.class)) {
                this.map(var, Objects.requireNonNull(this.parameterReplacement));
                return VisitDecision.STOP;
            }
            return super.preorder(var);
        }

        @Override
        public VisitDecision preorder(DBSPClosureExpression closure) {
            if (!this.context.isEmpty()) {
                if (ContainsNow.find(this.compiler, closure.body))
                    throw new CompilationError(
                            "NOW() is not supported inside a lambda expression", closure.getNode());
                this.map(closure, closure);
                return VisitDecision.STOP;
            }
            Utilities.enforce(closure.parameters.length == 1);
            DBSPParameter param = closure.parameters[0];
            DBSPTypeTuple paramType = param.getType().to(DBSPTypeRef.class).deref().to(DBSPTypeTuple.class);
            List<DBSPType> fields = Linq.list(paramType.tupFields);
            fields.add(ContainsNow.timestampType());
            DBSPParameter newParam = new DBSPParameter(param.getName(), paramType.makeRelatedTupleType(fields).ref());
            this.parameterReplacement = newParam.asVariable();
            this.nowReplacement = newParam.asVariable().deref().field(paramType.size());
            ResolveReferences ref = new ResolveReferences(this.compiler, false);
            ref.apply(closure);
            this.refMap = ref.reference;

            this.push(closure);
            DBSPExpression body = this.transform(closure.body);
            this.pop(closure);
            DBSPExpression result = body.closure(newParam);
            this.map(closure, result);
            return VisitDecision.STOP;
        }
    }

    /** Rewrites every occurrence of now() replacing it with a specified expression. */
    static class RewriteNowExpression extends InnerRewriteVisitor {
        /** Replacement for the now() function */
        final DBSPExpression replacement;

        public RewriteNowExpression(DBSPCompiler compiler, DBSPExpression replacement) {
            super(compiler, false);
            this.replacement = replacement;
        }

        @Override
        public VisitDecision preorder(DBSPApplyExpression expression) {
            if (ContainsNow.isNow(expression)) {
                this.map(expression, Objects.requireNonNull(this.replacement));
                return VisitDecision.STOP;
            }
            return super.preorder(expression);
        }

        @Override
        protected boolean rewritesTypes() {
            return false;
        }
    }

    public RewriteNow(DBSPCompiler compiler) {
        super(compiler, false);
    }

    /** Join {@code input} with the NOW stream; {@code operator} is the operator being rewritten,
     * and {@code nowCall} the NOW() call that requires the join. */
    DBSPSimpleOperator createJoin(DBSPSimpleOperator input, DBSPUnaryOperator operator,
                                  DBSPExpression nowCall) {
        DBSPType inputType = input.getOutputZSetElementType();
        var functions = this.compiler.options.ioOptions.rustOptimizerPasses
                ? RustCircuitPass.clockJoinFunctions(this.compiler, inputType, ContainsNow.timestampType())
                : clockJoinFunctions(inputType);
        DBSPMapIndexOperator index = new DBSPMapIndexOperator(operator.getRelNode(), functions.left,
                TypeCompiler.makeIndexedZSet(DBSPTypeTuple.EMPTY, inputType), input.outputPort());
        this.addOperator(index);
        DBSPClosureExpression joinFunction = functions.right;
        DBSPTypeZSet joinType = TypeCompiler.makeZSet(
                new DBSPTypeTuple(joinFunction.getResultType().to(DBSPTypeTuple.class).tupFields));
        Utilities.enforce(nowIndexed != null);
        // The join has no SQL counterpart; use the NOW() source code position
        CalciteRelNode joinNode = operator.getRelNode().copy();
        joinNode.addSourcePositions(List.of(nowCall.getSourcePosition()));
        DBSPSimpleOperator result = new DBSPStreamJoinOperator(joinNode, joinType,
                joinFunction, index.outputPort(), nowIndexed.outputPort(), false);
        this.addOperator(result);
        return result;
    }

    private static Pair<DBSPClosureExpression, DBSPClosureExpression> clockJoinFunctions(DBSPType inputType) {
        DBSPVariablePath var = inputType.ref().var();
        DBSPClosureExpression index = new DBSPRawTupleExpression(
                new DBSPTupleExpression(), DBSPTupleExpression.flatten(var.deref())).closure(var);
        DBSPVariablePath key = DBSPTypeTuple.EMPTY.ref().var();
        DBSPVariablePath left = inputType.ref().var();
        DBSPVariablePath right = new DBSPTypeTuple(ContainsNow.timestampType()).ref().var();
        List<DBSPExpression> fields = left.deref().allFields();
        fields.add(right.deref().field(0));
        return Pair.of(index, new DBSPTupleExpression(fields, false).closure(key, left, right));
    }

    private static DBSPClosureExpression dropClock(DBSPType joinedRow) {
        DBSPVariablePath var = joinedRow.ref().var();
        List<DBSPExpression> fields = var.deref().allFields();
        Utilities.removeLast(fields);
        return new DBSPTupleExpression(fields, false).closure(var);
    }

    private DBSPClosureExpression rewriteClockClosure(DBSPClosureExpression closure) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.rewriteClockClosure(this.compiler, closure, ContainsNow.timestampType());
        return new RewriteNowClosure(this.compiler()).apply(closure).to(DBSPClosureExpression.class);
    }

    void warnExpensive(@Nullable DBSPExpression expression) {
        this.compiler.reportWarning(
                Objects.requireNonNull(expression).getSourcePosition(), "Inefficient pattern",
                "NOW() expression is used in a pattern that could require expensive computations\n"
                        + NOW_DOCUMENTATION.citation());
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        ContainsNow cn = new ContainsNow(this.compiler(), true);
        DBSPExpression function = operator.getFunction();
        cn.apply(function);
        if (cn.found()) {
            OutputPort input = this.mapped(operator.input());
            this.warnExpensive(cn.nowExpression);
            DBSPSimpleOperator join = this.createJoin(
                    input.simpleNode(), operator, Objects.requireNonNull(cn.nowExpression));
            function = this.rewriteClockClosure(function.to(DBSPClosureExpression.class));
            DBSPSimpleOperator result = new DBSPMapOperator(
                    operator.getRelNode(), function, operator.getOutputZSetType(), join.outputPort());
            this.map(operator, result);
        } else {
            super.postorder(operator);
        }
    }

    /**
     * True if a comparison includes "equality"
     */
    static boolean isInclusive(DBSPOpcode opcode) {
        return opcode == DBSPOpcode.GTE || opcode == DBSPOpcode.LTE;
    }

    /**
     * True if a comparison is "greater than (or equal)"
     */
    static boolean isGreater(DBSPOpcode opcode) {
        return opcode == DBSPOpcode.GTE || opcode == DBSPOpcode.GT;
    }

    /** The opcode comparing the swapped operands: 'a OP b' iff 'b swappedOpcode(OP) a'. */
    static DBSPOpcode swappedOpcode(DBSPOpcode opcode) {
        return switch (opcode) {
            case LT -> DBSPOpcode.GT;
            case GTE -> DBSPOpcode.LTE;
            case LTE -> DBSPOpcode.GTE;
            case GT -> DBSPOpcode.LT;
            case EQ -> DBSPOpcode.EQ;
            default -> throw new InternalCompilerError(opcode.toString());
        };
    }

    /**
     * Decompose the filter's function into a sequence of comparisons.
     *
     * @param operator A filter operator.
     * @param function The operator's function.
     */
    public List<BooleanExpression> findTemporalFilters(DBSPFilterOperator operator, DBSPClosureExpression function) {
        return FindComparisons.decomposeIntoTemporalFilters(this.compiler, operator, function);
    }

    public DBSPSimpleOperator getNow() {
        return Objects.requireNonNull(this.now);
    }

    /**
     * An operator that produces a scalar value of the now input
     * (not a ZSet, but a single scalar), including a differentiator.
     */
    DBSPSimpleOperator scalarNow() {
        DBSPSimpleOperator source = this.getNow();
        DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(source.getRelNode(), source.outputPort());
        this.addOperator(diff);

        DBSPType row = source.getOutputZSetElementType();
        ClockWaterline functions = this.compiler.options.ioOptions.rustOptimizerPasses
                ? RustCircuitPass.scalarClock(this.compiler, row, ContainsNow.timestampType().getMinValue())
                : scalarClock(source);
        DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(source.getRelNode(),
                functions.initial(), functions.extract(), functions.maximum(), diff.outputPort());
        this.addOperator(waterline);
        return waterline;
    }

    public record ClockWaterline(DBSPClosureExpression initial, DBSPClosureExpression extract,
                                 DBSPClosureExpression maximum) {}

    private static ClockWaterline scalarClock(DBSPSimpleOperator source) {
        DBSPVariablePath t = source.getOutputZSetElementType().ref().var();
        DBSPExpression timestamp = t.deref().field(0);
        DBSPTupleExpression min = new DBSPTupleExpression(ContainsNow.timestampType().getMinValue());
        DBSPTupleExpression timestampTuple = new DBSPTupleExpression(timestamp);
        DBSPClosureExpression maximum = InsertLimiters.timestampMax(source.getNode(), min.getTypeAsTupleBase());
        return new ClockWaterline(min.closure(), timestampTuple.closure(t, DBSPTypeRawTuple.EMPTY.ref().var()), maximum);
    }

    /** Combine consecutive compatible expressions in the decomposition */
    static List<BooleanExpression> combineExpressions(List<BooleanExpression> decomposition) {
        List<BooleanExpression> combined = new ArrayList<>();
        BooleanExpression current = null;
        for (BooleanExpression expression : decomposition) {
            if (current == null) {
                current = expression;
            } else {
                if (current.compatible(expression)) {
                    current = current.combine(expression);
                } else {
                    combined.add(current.seal());
                    current = expression;
                }
            }
        }
        if (current != null)
            combined.add(current.seal());
        return combined;
    }

    /**
     * Implement a temporal comparison described as a list of comparisons.
     *
     * @param operator    Original filter operator
     * @param source      Result of filtering from previous comparisons.
     * @param comparisons A list of compatible comparisons that can be compiled into a window.
     * @return An operator whose output is the result of the filtering.
     */
    DBSPSimpleOperator implementTemporalFilter(DBSPFilterOperator operator,
                                               DBSPSimpleOperator source,
                                               TemporalFilterList comparisons) {
        // Now input comes from here
        CalciteRelNode relNode = operator.getRelNode();
        DBSPSimpleOperator scalarNow = this.scalarNow();
        DBSPTypeTupleBase inputType = source.getOutputZSetElementType().to(DBSPTypeTupleBase.class);
        DBSPExpression common = comparisons.comparisons().get(0).noNow();
        // The index and the bounds must share this exact key type.
        DBSPType windowKeyType = common.getType().withMayBeNull(false);
        var functions = this.windowFunctions(comparisons, inputType, common, windowKeyType);
        DBSPClosureExpression makeWindow = functions.bounds();
        DBSPSimpleOperator windowBounds = new DBSPApplyOperator(relNode, makeWindow, scalarNow.outputPort(), null);
        this.addOperator(windowBounds);

        if (functions.nonnull() != null) {
            var filter = new DBSPFilterOperator(relNode, functions.nonnull(), source.outputPort());
            this.addOperator(filter);
            source = filter;
        }

        WindowProjection projection = functions.projection();
        DBSPClosureExpression indexFunction = projection.index();
        var valueType = indexFunction.getResultType().to(DBSPTypeRawTuple.class).getFieldType(1);
        DBSPTypeIndexedZSet ix = new DBSPTypeIndexedZSet(operator.getRelNode(), windowKeyType, valueType);
        DBSPMapIndexOperator index = new DBSPMapIndexOperator(operator.getRelNode(),
                indexFunction, ix, source.outputPort());
        this.addOperator(index);

        // Apply window function.  Operator is incremental, so add D & I around it
        DBSPDifferentiateOperator diffIndex = new DBSPDifferentiateOperator(relNode, index.outputPort());
        this.addOperator(diffIndex);
        CalciteRelNode windowNode = relNode.copy();
        FindSourcePositions finder = new FindSourcePositions(this.compiler, false);
        finder.apply(makeWindow);
        windowNode.addSourcePositions(finder.positions);
        DBSPSimpleOperator window = new DBSPWindowOperator(
                windowNode, functions.lowerInclusive(), functions.upperInclusive(),
                functions.lowerUnbounded(), diffIndex.outputPort(), windowBounds.outputPort());
        this.addOperator(window);
        DBSPSimpleOperator winInt = new DBSPIntegrateOperator(relNode, window.outputPort());
        this.addOperator(winInt);

        DBSPSimpleOperator deindex;
        if (projection.restore() == null)
            deindex = new DBSPDeindexOperator(relNode, window.getFunctionNode(), winInt.outputPort());
        else
            deindex = new DBSPMapOperator(relNode, projection.restore(), new DBSPTypeZSet(inputType), winInt.outputPort());
        this.addOperator(deindex);
        return deindex;
    }

    public record WindowFunctions(DBSPClosureExpression bounds, @Nullable DBSPClosureExpression nonnull,
                                  WindowProjection projection, boolean lowerInclusive, boolean upperInclusive,
                                  boolean lowerUnbounded) {}

    private WindowFunctions windowFunctions(TemporalFilterList comparisons, DBSPTypeTupleBase inputType,
                                            DBSPExpression common, DBSPType keyType) {
        DBSPParameter param = comparisons.getParameter();
        if (this.compiler.options.ioOptions.rustOptimizerPasses) {
            var terms = comparisons.comparisons();
            var domain = keyType.to(IsBoundedType.class);
            boolean hasUpperBound = terms.stream().anyMatch(t -> !isGreater(t.opcode()));
            return RustCircuitPass.temporalWindow(this.compiler, param, inputType, common, keyType,
                    ContainsNow.timestampType().getMinValue(), domain.getMinValue(),
                    hasUpperBound ? domain.getMinValue() : domain.getMaxValue(),
                    Linq.map(terms, TemporalFilter::opcode), Linq.map(terms, TemporalFilter::withNow));
        }
        WindowBounds bounds = comparisons.getWindowBounds(this.compiler);
        DBSPClosureExpression window = bounds.makeWindow(keyType);
        DBSPClosureExpression nonnull = common.getType().mayBeNull ? common.is_null().not().closure(param) : null;
        return new WindowFunctions(window, nonnull, windowProjection(bounds, param, inputType, keyType),
                bounds.lower() == null || bounds.lower().inclusive(), bounds.upper() == null || bounds.upper().inclusive(),
                bounds.lower() == null);
    }

    public record WindowProjection(DBSPClosureExpression index, @Nullable DBSPClosureExpression restore) {}

    private static WindowProjection windowProjection(WindowBounds bounds, DBSPParameter param,
            DBSPTypeTupleBase inputType, DBSPType windowKeyType) {
        DBSPExpression common = bounds.common();
        // Index input by the temporal key.  When the key is a column, the value omits
        // that column, and the map after the window puts it back from the key.
        int keyColumn = bounds.keyColumn(param);
        DBSPExpression value;
        if (keyColumn < 0) {
            value = param.asVariable().deref().applyClone();
        } else {
            List<DBSPExpression> valueFields = new ArrayList<>();
            for (int i = 0; i < inputType.size(); i++)
                if (i != keyColumn)
                    valueFields.add(param.asVariable().deref().field(i).applyCloneIfNeeded());
            value = new DBSPTupleExpression(valueFields, false);
        }
        DBSPClosureExpression indexFunction =
                new DBSPRawTupleExpression(
                        common.cast(common.getNode(), windowKeyType,
                                DBSPCastExpression.CastType.SqlUnsafe),
                        value).closure(param);
        if (keyColumn < 0)
            return new WindowProjection(indexFunction, null);
        DBSPVariablePath var = new DBSPTypeRawTuple(windowKeyType.ref(), value.getType().ref()).var();
        DBSPExpression[] fields = new DBSPExpression[inputType.size()];
        for (int i = 0; i < inputType.size(); i++) {
            if (i == keyColumn) {
                // The key has the column's type, except that it cannot be NULL
                fields[i] = var.field(0).deref().applyCloneIfNeeded()
                        .cast(common.getNode(), inputType.getFieldType(i),
                                DBSPCastExpression.CastType.SqlUnsafe);
            } else {
                int valueField = i < keyColumn ? i : i - 1;
                fields[i] = var.field(1).deref().field(valueField).applyCloneIfNeeded();
            }
        }
        DBSPClosureExpression rebuild = inputType.makeTuple(fields).closure(var);
        return new WindowProjection(indexFunction, rebuild);
    }

    /**
     * Implement a list of filters, some of which may be temporal filters
     *
     * @param operator Original filter operator.
     * @param filters  A decomposition of the filter's condition into a sequence
     *                 of Boolean expressions, some of which may be suitable for
     *                 temporal filters.
     * @return The replacement for the original filter operator.
     */
    DBSPSimpleOperator implementTemporalFilters(DBSPFilterOperator operator,
                                                List<BooleanExpression> filters) {
        int nonTemporal = Linq.where(filters, f -> f.is(NonTemporalFilter.class)).size();
        Utilities.enforce(nonTemporal <= 1);
        if (nonTemporal == 1) {
            // Only the last element may be a non-temporal filter
            Utilities.enforce(Utilities.last(filters).is(NonTemporalFilter.class));
        }
        OutputPort current = this.mapped(operator.input());
        DBSPParameter param = operator.getClosureFunction().parameters[0];
        for (BooleanExpression expression : filters) {
            if (expression.is(NonTemporalFilter.class)) {
                // must be the last in the list
                return current.simpleNode();
            } else if (expression.is(TemporalFilterList.class)) {
                current = this.implementTemporalFilter(
                        operator, current.simpleNode(), expression.to(TemporalFilterList.class)).outputPort();
            } else {
                // NowComparison expressions have been eliminated by combineExpressions
                DBSPExpression body = expression.to(NoNow.class).noNow().wrapBoolIfNeeded();
                DBSPClosureExpression closure = body.closure(param);
                DBSPFilterOperator filter = new DBSPFilterOperator(operator.getRelNode(), closure, current);
                this.addOperator(filter);
                current = filter.outputPort();
            }
        }
        return current.simpleNode();
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPClosureExpression function = operator.getClosureFunction();
        if (!ContainsNow.find(this.compiler, function)) {
            // Filter does not involve 'now'
            super.postorder(operator);
            return;
        }

        function = function.ensureTree(this.compiler).to(DBSPClosureExpression.class);
        List<BooleanExpression> filters = this.findTemporalFilters(operator, function);
        filters = combineExpressions(filters);
        Utilities.enforce(!filters.isEmpty());
        DBSPSimpleOperator result = this.implementTemporalFilters(operator, filters);
        // If the last value in the list is a NonTemporalFilter, implement that as well
        BooleanExpression leftOver = Utilities.last(filters);

        if (leftOver.is(NonTemporalFilter.class)) {
            // Implement leftover as a join
            DBSPExpression filterBody = leftOver.to(NonTemporalFilter.class).expression().wrapBoolIfNeeded();
            ContainsNow cn = new ContainsNow(this.compiler(), true);
            cn.apply(filterBody);
            DBSPSimpleOperator join = this.createJoin(
                    result, operator, Objects.requireNonNull(cn.nowExpression));
            this.warnExpensive(filterBody);
            function = this.rewriteClockClosure(filterBody.closure(function.parameters));
            DBSPSimpleOperator filter = new DBSPFilterOperator(operator.getRelNode(), function, join.outputPort());
            this.addOperator(filter);
            // Drop the extra field
            DBSPType joinedRow = filter.getOutputZSetElementType();
            DBSPExpression drop = this.compiler.options.ioOptions.rustOptimizerPasses
                    ? RustCircuitPass.dropClock(this.compiler, joinedRow) : dropClock(joinedRow);
            result = new DBSPMapOperator(operator.getRelNode(), drop, operator.getOutputZSetType(), filter.outputPort());
            this.addOperator(result);
        }
        this.map(operator.outputPort(), result.outputPort(), false);
    }

    private DBSPClosureExpression clockIndex(DBSPType timestamp) {
        if (this.compiler.options.ioOptions.rustOptimizerPasses)
            return RustCircuitPass.clockIndex(this.compiler, timestamp);
        DBSPVariablePath var = new DBSPTypeTuple(timestamp).ref().var();
        return new DBSPRawTupleExpression(new DBSPTupleExpression(),
                new DBSPTupleExpression(var.deref().field(0))).closure(var);
    }

    private static Pair<DBSPClosureExpression, DBSPClosureExpression> clockAggregate(
            CalciteRelNode node, DBSPType timestamp, DBSPVariablePath weight) {
        DBSPVariablePath row = new DBSPTypeTuple(timestamp).ref().var();
        DBSPClosureExpression init = new DBSPTupleExpression(row.deref().field(0)).closure(row, weight);
        DBSPVariablePath acc = new DBSPTypeTuple(timestamp).var();
        DBSPClosureExpression update = new DBSPTupleExpression(new DBSPConditionalIncrementExpression(
                node, DBSPOpcode.AGG_MAX, timestamp, acc.field(0), row.deref().field(0), null)).closure(acc, row, weight);
        return Pair.of(init, update);
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        // We are doing this in preorder because we want now to be first in topological order,
        // otherwise the now operator may be inserted in the topological order too late,
        // after nodes that should depend on it
        // (but don't yet have an explicit edge, because they think they are calling a function).
        super.preorder(circuit);
        DBSPType timestamp = ContainsNow.timestampType();
        CalciteRelNode node = CalciteEmptyRel.INSTANCE;

        boolean useSource = !this.compiler.compiler().options.ioOptions.nowStream;
        if (useSource) {
            this.now = new DBSPNowOperator(node);
        } else {
            // table -> map_index -> chain_aggregate(max) -> deindex
            ProgramIdentifier tableName = DBSPCompiler.NOW_TABLE_NAME;
            IInputOperator nowInput = circuit.getInput(tableName);
            if (nowInput == null) {
                throw new CompilationError("Declaration for table 'NOW' not found in program");
            }
            this.now = nowInput.asOperator().to(DBSPSimpleOperator.class);
            // Prevent processing it again by this visitor
            this.visited.add(this.now);
            this.addOperator(this.now);

            DBSPMapIndexOperator index = new DBSPMapIndexOperator(node, this.clockIndex(timestamp), this.now.outputPort());
            this.addOperator(index);

            var aggregateFunctions = this.compiler.options.ioOptions.rustOptimizerPasses
                    ? RustCircuitPass.clockAggregate(this.compiler, timestamp, this.compiler.weightVar)
                    : clockAggregate(node, timestamp, this.compiler.weightVar);
            DBSPChainAggregateOperator aggregate = new DBSPChainAggregateOperator(
                    node, aggregateFunctions.left, aggregateFunctions.right, index.outputType, index.outputPort());
            this.addOperator(aggregate);
            this.nowIndexed = aggregate;

            this.now = new DBSPDeindexOperator(node, aggregate.getFunctionNode(), aggregate.outputPort());
        }
        this.addOperator(this.now);
        this.map(this.now.outputPort(), this.now.outputPort(), false);

        if (this.nowIndexed == null) {
            this.nowIndexed = new DBSPMapIndexOperator(
                    node, this.clockIndex(timestamp),
                    TypeCompiler.makeIndexedZSet(DBSPTypeTuple.EMPTY, new DBSPTypeTuple(timestamp)), now.outputPort());
            this.addOperator(this.nowIndexed);
        }
        this.nowIndexed.addAnnotation(AlwaysMonotone.INSTANCE, DBSPSimpleOperator.class);
        return VisitDecision.CONTINUE;
    }
}
