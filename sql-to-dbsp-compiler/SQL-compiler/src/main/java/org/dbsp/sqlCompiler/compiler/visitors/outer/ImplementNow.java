package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
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
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.InsertLimiters;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.circuit.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.circuit.annotation.NoInc;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Implements the "now" operator.
 * This requires:
 * - using the input stream called now
 * - rewriting map operators that have calls to "now()' into a join followed by a map
 * - rewriting the invocations to the now() function in the map function to references to the input variable */
public class ImplementNow extends Passes {
    /** Discovers whether an expression contains a call to the now() function */
    static class ContainsNow extends InnerVisitor {
        public boolean found;
        /** If true the 'found' is reset for each invocation */
        public final boolean perExpression;

        public ContainsNow(DBSPCompiler compiler, boolean perExpression) {
            super(compiler);
            this.found = false;
            this.perExpression = perExpression;
        }

        static boolean isNow(DBSPApplyExpression node) {
            String name = node.getFunctionName();
            return name != null && name.equalsIgnoreCase("now");
        }

        static DBSPTypeTimestamp timestampType() {
            return DBSPTypeTimestamp.INSTANCE;
        }

        @Override
        public VisitDecision preorder(DBSPApplyExpression node) {
            if (isNow(node)) {
                Logger.INSTANCE.belowLevel(this, 1)
                        .append("Found 'now' call")
                        .newline();
                found = true;
                return VisitDecision.STOP;
            }
            return super.preorder(node);
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            if (this.perExpression) {
                this.found = false;
            }
        }

        public Boolean found() {
            return this.found;
        }
    }

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
            assert this.context.isEmpty();
            assert closure.parameters.length == 1;
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
    }

    /** Replace map operators that contain now() as an expression with
     * map operators that take an extra field and use that instead of the now() call.
     * Insert a join prior to such operators (which also requires a MapIndex operator.
     * Also inserts a MapIndex operator to index the 'NOW' built-in table.
     * Same for filter operators. */
    static class RewriteNow extends CircuitCloneVisitor {
        // Holds the indexed version of the 'now' operator (indexed with an empty key).
        // (actually, it's the differentiator after the index)
        @Nullable
        DBSPMapIndexOperator nowIndexed = null;

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
                    TypeCompiler.makeIndexedZSet(new DBSPTypeTuple(), inputType), input.outputPort());
            this.addOperator(index);

            // Join with 'indexedNow'
            DBSPVariablePath key = new DBSPTypeTuple().ref().var();
            DBSPVariablePath left = inputType.ref().var();
            DBSPVariablePath right = new DBSPTypeTuple(ContainsNow.timestampType()).ref().var();
            List<DBSPExpression> fields = left.deref().allFields();
            fields.add(right.deref().field(0));
            DBSPTypeZSet joinType = TypeCompiler.makeZSet(new DBSPTypeTuple(
                    Linq.map(fields, DBSPExpression::getType)));
            DBSPExpression joinFunction = new DBSPTupleExpression(fields, false)
                    .closure(key, left, right);
            assert nowIndexed != null;
            DBSPSimpleOperator result = new DBSPStreamJoinOperator(operator.getRelNode(), joinType,
                        joinFunction, operator.isMultiset, index.outputPort(), nowIndexed.outputPort());
            this.addOperator(result);
            return result;
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            ContainsNow cn = new ContainsNow(this.compiler(), true);
            DBSPExpression function = operator.getFunction();
            cn.apply(function);
            if (cn.found()) {
                DBSPSimpleOperator join = this.createJoin(operator.input().simpleNode(), operator);
                RewriteNowClosure rn = new RewriteNowClosure(this.compiler());
                function = rn.apply(function).to(DBSPExpression.class);
                DBSPSimpleOperator result = new DBSPMapOperator(
                        operator.getRelNode(), function, operator.getOutputZSetType(), join.outputPort());
                this.map(operator, result);
            } else {
                super.postorder(operator);
            }
        }

        record WindowBound(boolean inclusive, DBSPExpression expression) {
            WindowBound combine(WindowBound with, boolean lower) {
                assert this.inclusive == with.inclusive;
                DBSPOpcode opcode = lower ? DBSPOpcode.MIN : DBSPOpcode.MAX;
                DBSPExpression expression = ExpressionCompiler.makeBinaryExpression(this.expression.getNode(),
                        this.expression.getType(), opcode, this.expression, with.expression);
                return new WindowBound(this.inclusive, expression);
            }

            public String toString() {
                return this.expression.toString();
            }
        }

        record WindowBounds(
                DBSPCompiler compiler,
                @Nullable WindowBound lower,
                @Nullable WindowBound upper,
                DBSPExpression common) {
            DBSPClosureExpression makeWindow() {
                DBSPType type = ContainsNow.timestampType();
                // The input has type Tup1<Timestamp>
                DBSPVariablePath var = new DBSPTypeTuple(type).ref().var();
                RewriteNowExpression rn = new RewriteNowExpression(this.compiler(), var.deref().field(0));
                DBSPExpression lowerBound, upperBound;
                if (this.lower != null)
                    lowerBound = rn.apply(this.lower.expression).to(DBSPExpression.class);
                else
                    lowerBound = type.to(IsBoundedType.class).getMinValue();
                if (this.upper != null)
                    upperBound = rn.apply(this.upper.expression).to(DBSPExpression.class);
                else
                    upperBound = type.to(IsBoundedType.class).getMaxValue();
                return new DBSPRawTupleExpression(
                        DBSPTypeTypedBox.wrapTypedBox(lowerBound, false),
                        DBSPTypeTypedBox.wrapTypedBox(upperBound, false))
                        .closure(var);
            }

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append(this.common).append(" in ");
                if (this.lower == null)
                    builder.append("(*");
                else
                    builder.append(this.lower.inclusive ? "[" : "(")
                            .append(this.lower);
                builder.append(", ");
                if (this.upper == null)
                    builder.append("*)");
                else
                    builder.append(this.upper)
                            .append(this.upper.inclusive ? "]" : ")");
                return builder.toString();
            }
        }

        /** True if a comparison includes "equality" */
        static boolean isInclusive(DBSPOpcode opcode) {
            return opcode == DBSPOpcode.GTE || opcode == DBSPOpcode.LTE;
        }

        /** True if a comparison is "greater than (or equal)" */
        static boolean isGreater(DBSPOpcode opcode) {
            return opcode == DBSPOpcode.GTE || opcode == DBSPOpcode.GT;
        }

        /** Boolean expression that appear as conjuncts in a filter expression */
        interface BooleanExpression extends ICastable {
            /** true if these two Boolean expressions can be evaluated together */
            boolean compatible(BooleanExpression other);
            /** Combine two compatible boolean expressions */
            BooleanExpression combine(BooleanExpression other);
            /** Returns the final form of this Boolean expression */
            BooleanExpression seal();
        }

        /**
         * A Boolean expression that is a comparison involving now() that can be implemented as a temporal filter.
         * @param parameter  Parameter of the original filter comparison function
         * @param noNow    left expression, which does not involve now.
         * @param withNow  right expression, which must ba a monotone expression of now(),
         *                 and which does not involve other fields
         * @param opcode   comparison
         */
        record TemporalFilter(DBSPParameter parameter, DBSPExpression noNow,
                              DBSPExpression withNow, DBSPOpcode opcode)
                implements BooleanExpression {
            @Override
            public boolean compatible(BooleanExpression other) {
                TemporalFilter o = other.as(TemporalFilter.class);
                if (o == null)
                    return false;

                // To be compatible the operations must be on different sides, or have the same inclusivity
                boolean thisGe = isGreater(this.opcode);
                boolean otherGe = isGreater(o.opcode);
                if (thisGe == otherGe) {
                    boolean thisInclusive = isInclusive(this.opcode);
                    boolean otherInclusive = isInclusive(o.opcode);
                    if (thisInclusive != otherInclusive)
                        return false;
                }

                EquivalenceContext context = new EquivalenceContext();
                context.leftDeclaration.newContext();
                context.rightDeclaration.newContext();
                context.leftDeclaration.substitute(this.parameter.name, this.parameter);
                context.rightDeclaration.substitute(this.parameter.name, this.parameter);
                context.leftToRight.put(this.parameter, this.parameter);
                return context.equivalent(this.noNow, o.noNow);
            }

            @Override
            public BooleanExpression combine(BooleanExpression other) {
                return new TemporalFilterList(Linq.list(this, other.to(TemporalFilter.class)));
            }

            public BooleanExpression seal() {
                // return a singleton list
                return new TemporalFilterList(Linq.list(this));
            }
        }

        /** A Boolean expression that does not involve now */
        record NoNow(DBSPExpression noNow) implements BooleanExpression {
            @Override
            public boolean compatible(BooleanExpression other) {
                return other.is(NoNow.class);
            }

            @Override
            public BooleanExpression combine(BooleanExpression other) {
                return new NoNow(
                        ExpressionCompiler.makeBinaryExpression(this.noNow().getNode(),
                                DBSPTypeBool.create(false), DBSPOpcode.AND,
                                this.noNow, other.to(NoNow.class).noNow));
            }

            @Override
            public BooleanExpression seal() {
                return this;
            }
        }

        /** A Boolean expression that involves now() but is not a temporal filter */
        record NonTemporalFilter(DBSPExpression expression) implements BooleanExpression {
            @Override
            public boolean compatible(BooleanExpression other) {
                // This is compatible with anything else
                return true;
            }

            @Override
            public BooleanExpression combine(BooleanExpression other) {
                return new NonTemporalFilter(
                        ExpressionCompiler.makeBinaryExpression(this.expression().getNode(),
                                DBSPTypeBool.create(false), DBSPOpcode.AND,
                                this.expression, other.to(NoNow.class).noNow));
            }

            @Override
            public BooleanExpression seal() {
                return this;
            }
        }

        /** A List of NowComparison objects that are "compatible" with each other.
         * These have the shape "(t.field > now() + constant)",
         * where the same field is used in all comparisons. */
        record TemporalFilterList(List<TemporalFilter> comparisons) implements BooleanExpression {
            @Override
            public boolean compatible(BooleanExpression other) {
                return Linq.all(this.comparisons, c -> c.compatible(other));
            }

            @Override
            public BooleanExpression combine(BooleanExpression other) {
                this.comparisons.add(other.to(TemporalFilter.class));
                return this;
            }

            /** Combine two window bounds.  At least one must be non-null */
            static WindowBound combine(@Nullable WindowBound left, @Nullable WindowBound right, boolean lower) {
                if (left == null)
                    return Objects.requireNonNull(right);
                if (right == null)
                    return left;
                return left.combine(right, lower);
            }

            public WindowBounds getWindowBounds(DBSPCompiler compiler) {
                WindowBound lower = null;
                WindowBound upper = null;
                assert !this.comparisons.isEmpty();
                DBSPExpression common = this.comparisons.get(0).noNow;
                for (TemporalFilter comp: this.comparisons) {
                    boolean inclusive = isInclusive(comp.opcode);
                    boolean toLower = isGreater(comp.opcode);
                    WindowBound result = new WindowBound(inclusive, comp.withNow);
                    if (toLower) {
                        lower = combine(lower, result, toLower);
                    } else {
                        upper = combine(upper, result, toLower);
                    }
                }
                return new WindowBounds(compiler, lower, upper, common);
            }

            @Override
            public BooleanExpression seal() {
                return this;
            }

            public DBSPParameter getParameter() {
                return this.comparisons.get(0).parameter;
            }
        }

        /** Break a Boolean expression into a series of conjunctions, some of which
         * may be implemented as temporal filters. */
        static class FindComparisons {
            final DBSPParameter parameter;
            /** List of Boolean expressions; a NonTemporalComparison may appear in the last position only */
            final List<BooleanExpression> comparisons;
            final MonotoneTransferFunctions mono;
            final ContainsNow containsNow;
            final Set<DBSPOpcode> compOpcodes;

            public FindComparisons(DBSPClosureExpression closure, MonotoneTransferFunctions mono) {
                this.mono = mono;
                this.comparisons = new ArrayList<>();
                this.containsNow = new ContainsNow(mono.compiler, true);
                this.compOpcodes = new HashSet<>() {{
                    add(DBSPOpcode.LT);
                    add(DBSPOpcode.LTE);
                    add(DBSPOpcode.GTE);
                    add(DBSPOpcode.GT);
                    add(DBSPOpcode.EQ);
                }};

                DBSPClosureExpression clo = closure.to(DBSPClosureExpression.class);
                assert clo.parameters.length == 1;
                this.parameter = clo.parameters[0];

                DBSPExpression expression = clo.body;
                if (expression.is(DBSPUnaryExpression.class)) {
                    DBSPUnaryExpression unary = expression.to(DBSPUnaryExpression.class);
                    // If the filter is wrap_bool(expression), analyze expression
                    if (unary.opcode == DBSPOpcode.WRAP_BOOL)
                        expression = unary.source;
                }
                this.analyzeConjunction(expression);
            }

            /** Analyze a conjunction; return 'true' if it was fully decomposed */
            boolean analyzeConjunction(DBSPExpression expression) {
                DBSPBinaryExpression binary = expression.as(DBSPBinaryExpression.class);
                if (binary == null) {
                    NonTemporalFilter ntf = new NonTemporalFilter(expression);
                    this.comparisons.add(ntf);
                    return false;
                }
                if (binary.opcode == DBSPOpcode.AND) {
                    boolean foundLeft = this.analyzeConjunction(binary.left);
                    if (!foundLeft)
                        return false;
                    return this.analyzeConjunction(binary.right);
                } else {
                    boolean decomposed = this.findComparison(binary);
                    if (!decomposed)
                        this.comparisons.add(new NonTemporalFilter(expression));
                    return decomposed;
                }
            }

            /** See if a binary expression can be implemented as a temporal filter.
             * Return 'false' if the expression contains now() but is not a temporal filter. */
            boolean findComparison(DBSPBinaryExpression binary) {
                this.containsNow.apply(binary);
                if (!this.containsNow.found) {
                    NoNow expression = new NoNow(binary);
                    this.comparisons.add(expression);
                    return true;
                }

                if (!this.compOpcodes.contains(binary.opcode))
                    return false;
                this.containsNow.apply(binary.left);
                boolean leftHasNow = this.containsNow.found;
                this.containsNow.apply(binary.right);
                boolean rightHasNow = this.containsNow.found;
                if (leftHasNow == rightHasNow) {
                    // Both true or both false
                    return false;
                }

                DBSPExpression withNow = leftHasNow ? binary.left : binary.right;
                DBSPExpression withoutNow = leftHasNow ? binary.right : binary.left;

                // The expression containing now() must be monotone
                MonotoneExpression me = this.mono.maybeGet(withNow);
                if (me == null || !me.mayBeMonotone())
                    return false;

                DBSPOpcode opcode = leftHasNow ? inverse(binary.opcode) : binary.opcode;
                TemporalFilter comp = new TemporalFilter(this.parameter, withoutNow, withNow, opcode);
                this.comparisons.add(comp);
                return true;
            }
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

        /** Decompose the filter's function into a sequence of comparisons.
         * @param operator A filter operator.
         * @param function  The operator's function.
         * @return A list of comparisons that can be implemented as simple filters or
         *         window operations.  Returns 'null' if the filter expression
         *         cannot be expressed as a list of such comparisons.
         */
        List<BooleanExpression> findTemporalFilters(DBSPFilterOperator operator, DBSPClosureExpression function) {
            assert function.parameters.length == 1;
            DBSPParameter param = function.parameters[0];
            IMaybeMonotoneType nonMonotone = NonMonotoneType.nonMonotone(param.getType().deref());
            MonotoneTransferFunctions mono = new MonotoneTransferFunctions(
                    this.compiler(), operator, MonotoneTransferFunctions.ArgumentKind.ZSet, nonMonotone);
            mono.apply(function);

            FindComparisons analyzer = new FindComparisons(function, mono);
            return analyzer.comparisons;
        }

        public DBSPSimpleOperator getNow() {
            assert this.nowIndexed != null;
            return this.nowIndexed.inputs.get(0).simpleNode();
        }

        public DBSPSimpleOperator flattenNow() {
            // An operator that produces a scalar value of the now input
            // (not a ZSet, but a single scalar).
            DBSPSimpleOperator source = this.getNow();
            DBSPVariablePath t = source.getOutputZSetElementType().ref().var();
            // We know that the output type of the source is Zset<Tup1<Timestamp>>
            DBSPTypeTimestamp type = ContainsNow.timestampType();
            DBSPExpression timestamp = t.deref().field(0);

            DBSPTupleExpression min = new DBSPTupleExpression(Linq.list(type.getMinValue()), false);
            DBSPTupleExpression timestampTuple = new DBSPTupleExpression(Linq.list(timestamp), false);
            DBSPParameter parameter = t.asParameter();
            DBSPClosureExpression max = InsertLimiters.timestampMax(source.getNode(), min.getTypeAsTupleBase());
            DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                    source.getRelNode(), min.closure(),
                    timestampTuple.closure(parameter, new DBSPTypeRawTuple().ref().var().asParameter()),
                    max, source.outputPort());
            this.addOperator(waterline);
            return waterline;
        }

        /** Combine consecutive compatible expressions in the decomposition */
        static List<BooleanExpression> combineExpressions(List<BooleanExpression> decomposition) {
            List<BooleanExpression> combined = new ArrayList<>();
            BooleanExpression current = null;
            for (BooleanExpression expression: decomposition) {
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

        /** Implement a temporal comparison described as a list of comparisons.
         *
         * @param operator    Original filter operator
         * @param source      Result of filtering from previous comparisons.
         * @param comparisons A list of compatible comparisons that can be compiled into a window.
         * @return            An operator whose output is the result of the filtering.
         */
        DBSPSimpleOperator implementTemporalFilter(DBSPFilterOperator operator,
                                                   DBSPSimpleOperator source,
                                                   TemporalFilterList comparisons) {
            // Now input comes from here
            DBSPSimpleOperator flattenNow = this.flattenNow();
            WindowBounds bounds = comparisons.getWindowBounds(this.compiler());
            DBSPClosureExpression makeWindow = bounds.makeWindow();
            DBSPSimpleOperator windowBounds = new DBSPApplyOperator(operator.getRelNode(),
                    makeWindow, flattenNow.outputPort(), null);
            this.addOperator(windowBounds);

            // Filter the null timestamps away, they won't be selected anyway,
            // but window needs non-nullable values
            DBSPTypeTupleBase inputType = source.getOutputZSetElementType().to(DBSPTypeTupleBase.class);
            DBSPType commonType = bounds.common.getType();
            DBSPParameter param = comparisons.getParameter();
            if (bounds.common.getType().mayBeNull) {
                DBSPClosureExpression nonNull =
                        bounds.common.is_null().not().closure(param);
                DBSPFilterOperator filter = new DBSPFilterOperator(operator.getRelNode(), nonNull, source.outputPort());
                this.addOperator(filter);
                source = filter;
            }

            // Index input by timestamp
            DBSPClosureExpression indexFunction =
                    new DBSPRawTupleExpression(
                            bounds.common.cast(commonType.withMayBeNull(false), false),
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
            boolean lowerInclusive = bounds.lower == null || bounds.lower.inclusive;
            boolean upperInclusive = bounds.upper == null || bounds.upper.inclusive;
            DBSPSimpleOperator window = new DBSPWindowOperator(
                    operator.getRelNode(), lowerInclusive, upperInclusive, diffIndex.outputPort(), windowBounds.outputPort());
            this.addOperator(window);
            DBSPSimpleOperator winInt = new DBSPIntegrateOperator(operator.getRelNode(), window.outputPort());
            this.addOperator(winInt);

            // Deindex result of window
            DBSPSimpleOperator deindex = new DBSPDeindexOperator(operator.getRelNode(), winInt.outputPort());
            this.addOperator(deindex);
            return deindex;
        }

        /** Implement a list of filters, some of which may be temporal filters
         *
         * @param operator Original filter operator.
         * @param filters  A decomposition of the filter's condition into a sequence
         *                 of Boolean expressions, some of which may be suitable for
         *                 temporal filters.
         * @return         The replacement for the original filter operator.
         */
        DBSPSimpleOperator implementTemporalFilters(DBSPFilterOperator operator,
                                                    List<BooleanExpression> filters) {
            int nonTemporal = Linq.where(filters, f -> f.is(NonTemporalFilter.class)).size();
            assert nonTemporal <= 1;
            if (nonTemporal == 1) {
                // Only the last element may be a non-temporal filter
                assert Utilities.last(filters).is(NonTemporalFilter.class);
            }
            OutputPort current = this.mapped(operator.input());
            DBSPParameter param = operator.getClosureFunction().parameters[0];
            for (BooleanExpression expression: filters) {
                if (expression.is(NonTemporalFilter.class)) {
                    // must be the last in the list
                    return current.simpleNode();
                } else if (expression.is(TemporalFilterList.class)) {
                    current = this.implementTemporalFilter(
                            operator, current.simpleNode(), expression.to(TemporalFilterList.class)).outputPort();
                } else {
                    // NowComparison expressions have been eliminated by combineExpressions
                    DBSPExpression body = expression.to(NoNow.class).noNow.wrapBoolIfNeeded();
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
            DBSPExpression function = operator.getFunction();
            cn.apply(function);
            if (!cn.found) {
                // Filter does not involve 'now'
                super.postorder(operator);
                return;
            }

            // This makes it easier to discover more monotone expressions
            Simplify simplify = new Simplify(this.compiler());
            DBSPClosureExpression closure = function.to(DBSPClosureExpression.class);
            closure = simplify.apply(closure).to(DBSPClosureExpression.class);
            closure = closure.ensureTree(compiler).to(DBSPClosureExpression.class);
            List<BooleanExpression> filters = this.findTemporalFilters(operator, closure);
            filters = combineExpressions(filters);
            assert !filters.isEmpty();
            DBSPSimpleOperator result = this.implementTemporalFilters(operator, filters);
            // If the last value in the list is a NonTemporalFilter, implement that as well
            BooleanExpression leftOver = Utilities.last(filters);

            if (leftOver.is(NonTemporalFilter.class)) {
                // Implement leftover as a join
                DBSPSimpleOperator join = this.createJoin(result, operator);
                RewriteNowClosure rn = new RewriteNowClosure(this.compiler());
                DBSPExpression filterBody = leftOver.to(NonTemporalFilter.class).expression.wrapBoolIfNeeded();
                function = filterBody.closure(closure.parameters);
                function = rn.apply(function).to(DBSPExpression.class);
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

            DBSPSimpleOperator now;
            boolean useSource = !this.compiler.compiler().options.ioOptions.nowStream;
            if (useSource) {
                now = new DBSPNowOperator(node);
            } else {
                // A table followed by a differentiator.
                ProgramIdentifier tableName = DBSPCompiler.NOW_TABLE_NAME;
                now = circuit.getInput(tableName);
                if (now == null) {
                    throw new CompilationError("Declaration for table 'NOW' not found in program");
                }
                // Prevent processing it again by this visitor
                this.visited.add(now);
                this.addOperator(now);

                DBSPDifferentiateOperator dNow = new DBSPDifferentiateOperator(node, now.outputPort());
                now = dNow;
                dNow.annotations.add(NoInc.INSTANCE);
            }
            this.addOperator(now);
            this.map(now.outputPort(), now.outputPort(), false);

            DBSPVariablePath var = new DBSPTypeTuple(timestamp).ref().var();
            DBSPExpression indexFunction = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(),
                    new DBSPTupleExpression(DBSPTypeTupleBase.flatten(var.deref()), false));
            this.nowIndexed = new DBSPMapIndexOperator(
                    node, indexFunction.closure(var),
                    TypeCompiler.makeIndexedZSet(new DBSPTypeTuple(), new DBSPTypeTuple(timestamp)), now.outputPort());
            this.nowIndexed.addAnnotation(AlwaysMonotone.INSTANCE, DBSPSimpleOperator.class);
            this.addOperator(this.nowIndexed);
            return VisitDecision.CONTINUE;
        }
    }

    static class OmitNow extends CircuitDispatcher {
        public OmitNow(DBSPCompiler compiler, InnerVisitor innerVisitor) {
            super(compiler, innerVisitor, false);
        }

        @Override
        public VisitDecision preorder(DBSPNowOperator node) {
            return VisitDecision.STOP;
        }
    }

    public ImplementNow(DBSPCompiler compiler) {
        super("ImplementNow", compiler);
        boolean removeTable = !compiler.compiler().options.ioOptions.nowStream;
        ContainsNow cn = new ContainsNow(compiler, false);
        RewriteNow rewriteNow = new RewriteNow(compiler);
        this.passes.add(new OmitNow(compiler, cn));
        this.passes.add(new Conditional(compiler, rewriteNow, cn::found));
        this.passes.add(new Conditional(compiler,
                new RemoveTable(compiler, DBSPCompiler.NOW_TABLE_NAME), () -> !cn.found || removeTable));
        ContainsNow cn0 = new ContainsNow(compiler, false);
        this.passes.add(new OmitNow(compiler, cn0));
        this.passes.add(new Conditional(compiler,
                new Fail(compiler, "Instances of 'now' have not been replaced"), cn0::found));
    }
}

