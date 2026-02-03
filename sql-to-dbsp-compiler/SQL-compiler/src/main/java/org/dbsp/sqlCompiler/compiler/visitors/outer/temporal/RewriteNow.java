package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

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
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
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
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
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
            Utilities.enforce(this.context.isEmpty());
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
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
        }
    }

    public RewriteNow(DBSPCompiler compiler) {
        super(compiler, false);
    }

    DBSPSimpleOperator createJoin(DBSPSimpleOperator input, DBSPUnaryOperator operator) {
        DBSPType inputType = input.getOutputZSetElementType();
        DBSPVariablePath var = inputType.ref().var();
        DBSPExpression indexFunction = new DBSPRawTupleExpression(
                new DBSPTupleExpression(),
                DBSPTupleExpression.flatten(var.deref()));
        DBSPMapIndexOperator index = new DBSPMapIndexOperator(
                operator.getRelNode(), indexFunction.closure(var),
                TypeCompiler.makeIndexedZSet(DBSPTypeTuple.EMPTY, inputType), input.outputPort());
        this.addOperator(index);

        // Join with 'indexedNow'
        DBSPVariablePath key = DBSPTypeTuple.EMPTY.ref().var();
        DBSPVariablePath left = inputType.ref().var();
        DBSPVariablePath right = new DBSPTypeTuple(ContainsNow.timestampType()).ref().var();
        List<DBSPExpression> fields = left.deref().allFields();
        fields.add(right.deref().field(0));
        DBSPTypeZSet joinType = TypeCompiler.makeZSet(new DBSPTypeTuple(
                Linq.map(fields, DBSPExpression::getType)));
        DBSPExpression joinFunction = new DBSPTupleExpression(fields, false)
                .closure(key, left, right);
        Utilities.enforce(nowIndexed != null);
        DBSPSimpleOperator result = new DBSPStreamJoinOperator(operator.getRelNode(), joinType,
                joinFunction, operator.isMultiset, index.outputPort(), nowIndexed.outputPort(), false);
        this.addOperator(result);
        return result;
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        ContainsNow cn = new ContainsNow(this.compiler(), true);
        DBSPExpression function = operator.getFunction();
        cn.apply(function);
        if (cn.found()) {
            OutputPort input = this.mapped(operator.input());
            DBSPSimpleOperator join = this.createJoin(input.simpleNode(), operator);
            RewriteNowClosure rn = new RewriteNowClosure(this.compiler());
            function = rn.apply(function).to(DBSPExpression.class);
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

    static DBSPOpcode inverse(DBSPOpcode opcode) {
        return switch (opcode) {
            case LT -> DBSPOpcode.GTE;
            case GTE -> DBSPOpcode.LT;
            case LTE -> DBSPOpcode.GT;
            case GT -> DBSPOpcode.LTE;
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

        // We know that the output type of the source is Zset<Tup1<Timestamp>>
        DBSPVariablePath t = source.getOutputZSetElementType().ref().var();
        DBSPTypeTimestamp type = ContainsNow.timestampType();
        DBSPExpression timestamp = t.deref().field(0);

        DBSPTupleExpression min = new DBSPTupleExpression(Linq.list(type.getMinValue()), false);
        DBSPTupleExpression timestampTuple = new DBSPTupleExpression(Linq.list(timestamp), false);
        DBSPClosureExpression max = InsertLimiters.timestampMax(source.getNode(), min.getTypeAsTupleBase());
        DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                source.getRelNode(), min.closure(),
                timestampTuple.closure(t, DBSPTypeRawTuple.EMPTY.ref().var()),
                max, diff.outputPort());
        this.addOperator(waterline);
        return waterline;
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
        DBSPSimpleOperator scalarNow = this.scalarNow();
        WindowBounds bounds = comparisons.getWindowBounds(this.compiler());
        DBSPClosureExpression makeWindow = bounds.makeWindow();
        DBSPSimpleOperator windowBounds = new DBSPApplyOperator(operator.getRelNode(),
                makeWindow, scalarNow.outputPort(), null);
        this.addOperator(windowBounds);

        // Filter the null timestamps away, they won't be selected anyway,
        // but window needs non-nullable values
        DBSPTypeTupleBase inputType = source.getOutputZSetElementType().to(DBSPTypeTupleBase.class);
        DBSPType commonType = bounds.common().getType();
        DBSPParameter param = comparisons.getParameter();
        if (bounds.common().getType().mayBeNull) {
            DBSPClosureExpression nonNull =
                    bounds.common().is_null().not().closure(param);
            DBSPFilterOperator filter = new DBSPFilterOperator(operator.getRelNode(), nonNull, source.outputPort());
            this.addOperator(filter);
            source = filter;
        }

        // Index input by timestamp
        DBSPClosureExpression indexFunction =
                new DBSPRawTupleExpression(
                        bounds.common().cast(bounds.common().getNode(), commonType.withMayBeNull(false),
                                DBSPCastExpression.CastType.SqlUnsafe),
                        param.asVariable().deref().applyClone()).closure(param);
        DBSPTypeIndexedZSet ix = new DBSPTypeIndexedZSet(operator.getRelNode(),
                commonType.withMayBeNull(false),
                inputType);
        DBSPMapIndexOperator index = new DBSPMapIndexOperator(operator.getRelNode(),
                indexFunction, ix, operator.isMultiset, source.outputPort());
        this.addOperator(index);

        // Apply window function.  Operator is incremental, so add D & I around it
        DBSPDifferentiateOperator diffIndex = new DBSPDifferentiateOperator(operator.getRelNode(), index.outputPort());
        this.addOperator(diffIndex);
        boolean lowerInclusive = bounds.lower() == null || bounds.lower().inclusive();
        boolean upperInclusive = bounds.upper() == null || bounds.upper().inclusive();
        DBSPSimpleOperator window = new DBSPWindowOperator(
                operator.getRelNode(), makeWindow.getNode(),
                lowerInclusive, upperInclusive, diffIndex.outputPort(), windowBounds.outputPort());
        this.addOperator(window);
        DBSPSimpleOperator winInt = new DBSPIntegrateOperator(operator.getRelNode(), window.outputPort());
        this.addOperator(winInt);

        // Deindex result of window
        DBSPSimpleOperator deindex = new DBSPDeindexOperator(operator.getRelNode(), window.getFunctionNode(),
                winInt.outputPort());
        this.addOperator(deindex);
        return deindex;
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
        ContainsNow cn = new ContainsNow(this.compiler(), true);
        DBSPClosureExpression function = operator.getClosureFunction();
        cn.apply(function);
        if (!cn.found) {
            // Filter does not involve 'now'
            super.postorder(operator);
            return;
        }

        function = function.ensureTree(compiler).to(DBSPClosureExpression.class);
        List<BooleanExpression> filters = this.findTemporalFilters(operator, function);
        filters = combineExpressions(filters);
        Utilities.enforce(!filters.isEmpty());
        DBSPSimpleOperator result = this.implementTemporalFilters(operator, filters);
        // If the last value in the list is a NonTemporalFilter, implement that as well
        BooleanExpression leftOver = Utilities.last(filters);

        if (leftOver.is(NonTemporalFilter.class)) {
            // Implement leftover as a join
            DBSPSimpleOperator join = this.createJoin(result, operator);
            RewriteNowClosure rn = new RewriteNowClosure(this.compiler());
            DBSPExpression filterBody = leftOver.to(NonTemporalFilter.class).expression().wrapBoolIfNeeded();
            function = filterBody.closure(function.parameters);
            function = rn.apply(function).to(DBSPClosureExpression.class);
            DBSPSimpleOperator filter = new DBSPFilterOperator(operator.getRelNode(), function, join.outputPort());
            this.addOperator(filter);
            // Drop the extra field
            DBSPVariablePath var = filter.getOutputZSetElementType().ref().var();
            List<DBSPExpression> fields = var.deref().allFields();
            Utilities.removeLast(fields);
            DBSPExpression drop = new DBSPTupleExpression(fields, false).closure(var);
            result = new DBSPMapOperator(operator.getRelNode(), drop, operator.getOutputZSetType(), filter.outputPort());
            this.addOperator(result);
        }
        this.map(operator.outputPort(), result.outputPort(), false);
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

            DBSPVariablePath var = new DBSPTypeTuple(timestamp).ref().var();
            DBSPClosureExpression indexTuple = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(), new DBSPTupleExpression(var.deref().field(0))).closure(var);
            DBSPMapIndexOperator index = new DBSPMapIndexOperator(node, indexTuple, this.now.outputPort());
            this.addOperator(index);

            DBSPVariablePath weightVar = compiler.weightVar;
            DBSPVariablePath timestampVar = new DBSPTypeTuple(timestamp).ref().var();
            DBSPClosureExpression init = new DBSPTupleExpression(timestampVar.deref().field(0))
                    .closure(timestampVar, weightVar);

            DBSPType chainType = index.outputType;

            DBSPVariablePath acc = new DBSPTypeTuple(timestamp).var();
            DBSPClosureExpression function = new DBSPTupleExpression(
                    new DBSPConditionalIncrementExpression(
                            node, DBSPOpcode.AGG_MAX, timestamp,
                            acc.field(0), timestampVar.deref().field(0), null))
                    .closure(acc, timestampVar, weightVar);

            DBSPChainAggregateOperator aggregate = new DBSPChainAggregateOperator(
                    node, init, function, chainType, index.outputPort());
            this.addOperator(aggregate);
            this.nowIndexed = aggregate;

            this.now = new DBSPDeindexOperator(node, aggregate.getFunctionNode(), aggregate.outputPort());
        }
        this.addOperator(this.now);
        this.map(this.now.outputPort(), this.now.outputPort(), false);

        if (this.nowIndexed == null) {
            DBSPVariablePath var = new DBSPTypeTuple(timestamp).ref().var();
            DBSPExpression indexFunction = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(),
                    new DBSPTupleExpression(var.deref().field(0)));
            this.nowIndexed = new DBSPMapIndexOperator(
                    node, indexFunction.closure(var),
                    TypeCompiler.makeIndexedZSet(DBSPTypeTuple.EMPTY, new DBSPTypeTuple(timestamp)), now.outputPort());
            this.addOperator(this.nowIndexed);
        }
        this.nowIndexed.addAnnotation(AlwaysMonotone.INSTANCE, DBSPSimpleOperator.class);
        return VisitDecision.CONTINUE;
    }
}
