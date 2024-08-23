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
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ReferenceMap;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
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
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
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

        public ContainsNow(IErrorReporter reporter, boolean perExpression) {
            super(reporter);
            this.found = false;
            this.perExpression = perExpression;
        }

        static boolean isNow(DBSPApplyExpression node) {
            String name = node.getFunctionName();
            return name != null && name.equalsIgnoreCase("now");
        }

        static DBSPTypeTimestamp timestampType() {
            return new DBSPTypeTimestamp(CalciteObject.EMPTY, false);
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

    /** Remove the 'now' input table */
    public static class RemoveNow extends CircuitCloneVisitor {
        final ICompilerComponent compiler;

        RemoveNow(IErrorReporter errorReporter, ICompilerComponent compiler) {
            super(errorReporter, false);
            this.compiler = compiler;
        }

        @Override
        public void postorder(DBSPSourceMultisetOperator map) {
            if (map.tableName.equalsIgnoreCase("now")) {
                // Return without adding it to the circuit.
                Logger.INSTANCE.belowLevel(this, 1)
                        .append("Removing table 'now'")
                        .newline();
                this.compiler.compiler().removeNowTable();
                return;
            }
            super.postorder(map);
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

        public RewriteNowClosure(IErrorReporter reporter) {
            super(reporter);
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
            DBSPParameter newParam = new DBSPParameter(param.getName(), paramType.makeType(fields).ref());
            this.parameterReplacement = newParam.asVariable();
            this.nowReplacement = newParam.asVariable().deref().field(paramType.size());
            ResolveReferences ref = new ResolveReferences(this.errorReporter, false);
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

        public RewriteNowExpression(IErrorReporter reporter, DBSPExpression replacement) {
            super(reporter);
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
        final ICompilerComponent compiler;

        public RewriteNow(IErrorReporter reporter, ICompilerComponent compiler) {
            super(reporter, false);
            this.compiler = compiler;
        }

        DBSPOperator createJoin(DBSPUnaryOperator operator) {
            // Index the input
            DBSPType inputType = operator.input().getOutputZSetElementType();
            DBSPVariablePath var = inputType.ref().var();
            DBSPExpression indexFunction = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(),
                    var.deref().applyClone());
            DBSPMapIndexOperator index = new DBSPMapIndexOperator(
                    operator.getNode(), indexFunction.closure(var.asParameter()),
                    TypeCompiler.makeIndexedZSet(new DBSPTypeTuple(), inputType),
                    this.mapped(operator.input()));
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
                    .closure(key.asParameter(), left.asParameter(), right.asParameter());
            assert nowIndexed != null;
            DBSPOperator result = new DBSPStreamJoinOperator(operator.getNode(), joinType,
                        joinFunction, operator.isMultiset, index, nowIndexed);
            this.addOperator(result);
            return result;
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            ContainsNow cn = new ContainsNow(this.errorReporter, true);
            DBSPExpression function = operator.getFunction();
            cn.apply(function);
            if (cn.found()) {
                DBSPOperator join = this.createJoin(operator);
                RewriteNowClosure rn = new RewriteNowClosure(this.errorReporter);
                function = rn.apply(function).to(DBSPExpression.class);
                DBSPOperator result = new DBSPMapOperator(operator.getNode(), function, operator.getOutputZSetType(), join);
                this.map(operator, result);
            } else {
                super.postorder(operator);
            }
        }

        record WindowBound(boolean inclusive, DBSPExpression expression) {
            @Nullable WindowBound combine(WindowBound with, boolean lower) {
                if (this.inclusive != with.inclusive)
                    return null;
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
                IErrorReporter reporter,
                @Nullable WindowBound lower,
                @Nullable WindowBound upper,
                DBSPExpression common) {
            DBSPClosureExpression makeWindow() {
                DBSPType type = ContainsNow.timestampType();
                // The input has type Tup1<Timestamp>
                DBSPVariablePath var = new DBSPTypeTuple(type).ref().var();
                RewriteNowExpression rn = new RewriteNowExpression(this.reporter, var.deref().field(0));
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
                        .closure(var.asParameter());
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

        @Nullable
        static WindowBound combine(@Nullable WindowBound left, @Nullable WindowBound right, boolean lower) {
            if (left == null)
                return right;
            if (right == null)
                return left;
            return left.combine(right, lower);
        }

        static class FindComparisons {
            static class Comparison {
                /** Left expression does not involve now() */
                final DBSPExpression noNow;
                /** Right expression must ba a monotone expression of now(), which does
                 * not involve other fields */
                final DBSPExpression withNow;
                final DBSPOpcode opcode;

                Comparison(DBSPExpression noNow, DBSPExpression withNow, DBSPOpcode opcode) {
                    this.noNow = noNow;
                    this.withNow = withNow;
                    this.opcode = opcode;
                }
            }

            final boolean complete;
            final List<Comparison> comparisons;
            final MonotoneTransferFunctions mono;
            final ContainsNow containsNow;
            final Set<DBSPOpcode> compOpcodes;

            public FindComparisons(DBSPClosureExpression closure, MonotoneTransferFunctions mono) {
                this.mono = mono;
                this.comparisons = new ArrayList<>();
                this.containsNow = new ContainsNow(mono.errorReporter, true);
                this.compOpcodes = new HashSet<>() {{
                    add(DBSPOpcode.LT);
                    add(DBSPOpcode.LTE);
                    add(DBSPOpcode.GTE);
                    add(DBSPOpcode.GT);
                    add(DBSPOpcode.EQ);
                }};

                DBSPClosureExpression clo = closure.to(DBSPClosureExpression.class);

                DBSPExpression expression = clo.body;
                if (expression.is(DBSPUnaryExpression.class)) {
                    DBSPUnaryExpression unary = expression.to(DBSPUnaryExpression.class);
                    // If the filter is wrap_bool(expression), analyze expression
                    if (unary.operation == DBSPOpcode.WRAP_BOOL)
                        expression = unary.source;
                }
                this.complete = this.analyzeConjunction(expression);
            }

            boolean analyzeConjunction(DBSPExpression expression) {
                DBSPBinaryExpression binary = expression.as(DBSPBinaryExpression.class);
                if (binary == null)
                    return false;
                if (binary.operation == DBSPOpcode.AND) {
                    boolean foundLeft = this.analyzeConjunction(binary.left);
                    boolean foundRight = this.analyzeConjunction(binary.right);
                    return foundLeft && foundRight;
                } else {
                    return this.findComparison(binary);
                }
            }

            boolean findComparison(DBSPBinaryExpression binary) {
                if (!this.compOpcodes.contains(binary.operation))
                    return false;
                this.containsNow.apply(binary.left);
                boolean leftHasNow = this.containsNow.found;
                this.containsNow.apply(binary.right);
                boolean rightHasNow = this.containsNow.found;
                if (leftHasNow == rightHasNow)
                    // Both true or both false
                    return false;

                DBSPExpression withNow = leftHasNow ? binary.left : binary.right;
                DBSPExpression withoutNow = leftHasNow ? binary.right : binary.left;

                // The expression containing now() must be monotone
                MonotoneExpression me = this.mono.maybeGet(withNow);
                if (me == null)
                    return false;

                DBSPOpcode opcode = leftHasNow ? inverse(binary.operation) : binary.operation;
                Comparison comp = new Comparison(withoutNow, withNow, opcode);
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
                default -> throw new InternalCompilerError(opcode.toString());
            };
        }

        @Nullable WindowBounds findWindowBounds(DBSPOperator operator) {
            DBSPClosureExpression closure = operator.getFunction().to(DBSPClosureExpression.class);
            assert closure.parameters.length == 1;
            DBSPParameter param = closure.parameters[0];
            IMaybeMonotoneType nonMonotone = NonMonotoneType.nonMonotone(param.getType().deref());
            MonotoneTransferFunctions mono = new MonotoneTransferFunctions(
                    this.errorReporter, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, nonMonotone);
            mono.apply(closure);

            FindComparisons analyzer = new FindComparisons(closure, mono);
            if (!analyzer.complete || analyzer.comparisons.isEmpty())
                return null;

            // If not all comparisons compare the same expression, give up
            DBSPExpression common = null;
            EquivalenceContext context = new EquivalenceContext();
            context.leftDeclaration.newContext();
            context.rightDeclaration.newContext();
            context.leftDeclaration.substitute(param.name, param);
            context.rightDeclaration.substitute(param.name, param);
            context.leftToRight.put(param, param);

            for (FindComparisons.Comparison comp: analyzer.comparisons) {
                if (common == null)
                    common = comp.noNow;
                else if (!context.equivalent(comp.noNow, common))
                    return null;
            }
            if (common == null)
                // This shouldn't really happen
                return null;

            WindowBound lower = null;
            WindowBound upper = null;
            for (FindComparisons.Comparison comp: analyzer.comparisons) {
                boolean inclusive = comp.opcode == DBSPOpcode.GTE || comp.opcode == DBSPOpcode.LTE;
                boolean toLower = comp.opcode == DBSPOpcode.GTE || comp.opcode == DBSPOpcode.GT;
                WindowBound result = new WindowBound(inclusive, comp.withNow);
                if (toLower) {
                    lower = combine(lower, result, toLower);
                    if (lower == null)
                        return null;
                } else {
                    upper = combine(upper, result, toLower);
                    if (upper == null)
                        return null;
                }
            }
            return new WindowBounds(this.errorReporter, lower, upper, common);
        }

        public DBSPOperator getNow() {
            assert this.nowIndexed != null;
            return this.nowIndexed.inputs.get(0);
        }

        public DBSPOperator flattenNow() {
            // An operator that produces a scalar value of the now input
            // (not a ZSet, but a single scalar).
            DBSPOperator source = this.getNow();
            DBSPVariablePath t = source.getOutputZSetElementType().ref().var();
            // We know that the output type of the source is Zset<Tup1<Timestamp>>
            DBSPTypeTimestamp type = ContainsNow.timestampType();
            DBSPExpression timestamp = t.deref().field(0);

            DBSPTupleExpression min = new DBSPTupleExpression(Linq.list(type.getMinValue()), false);
            DBSPTupleExpression timestampTuple = new DBSPTupleExpression(Linq.list(timestamp), false);
            DBSPParameter parameter = t.asParameter();
            DBSPClosureExpression max = InsertLimiters.timestampMax(source.getNode(), min.getTupleType());
            DBSPWaterlineOperator waterline = new DBSPWaterlineOperator(
                    source.getNode(), min.closure(),
                    timestampTuple.closure(parameter, new DBSPTypeRawTuple().ref().var().asParameter()),
                    max, source);
            this.addOperator(waterline);
            return waterline;
        }

        @Override
        public void postorder(DBSPFilterOperator operator) {
            ContainsNow cn = new ContainsNow(this.errorReporter, true);
            DBSPExpression function = operator.getFunction();
            DBSPClosureExpression closure = function.to(DBSPClosureExpression.class);
            assert closure.parameters.length == 1;
            DBSPParameter param = closure.parameters[0];
            cn.apply(function);
            if (!cn.found) {
                super.postorder(operator);
                return;
            }

            // Look for "temporal filters".
            // These have the shape "&& (t.field > now() + constant)",
            // where the same field is used in all comparisons
            WindowBounds bounds = this.findWindowBounds(operator);
            if (bounds != null) {
                DBSPOperator flattenNow = this.flattenNow();
                DBSPClosureExpression makeWindow = bounds.makeWindow();
                DBSPOperator windowBounds = new DBSPApplyOperator(operator.getNode(),
                        makeWindow, flattenNow, null);
                this.addOperator(windowBounds);

                // Filter the null timestamps away, they won't be selected anyway,
                // but window needs non-nullable values
                DBSPTypeTupleBase inputType = operator.input().getOutputZSetElementType().to(DBSPTypeTupleBase.class);
                DBSPOperator source = this.mapped(operator.input());
                DBSPType commonType = bounds.common.getType();
                if (bounds.common.getType().mayBeNull) {
                    DBSPClosureExpression nonNull =
                            new DBSPUnaryExpression(
                                    operator.getNode(),
                                    DBSPTypeBool.create(false),
                                    DBSPOpcode.NOT,
                                    bounds.common.is_null()).closure(param);
                    DBSPFilterOperator filter = new DBSPFilterOperator(operator.getNode(), nonNull, source);
                    this.addOperator(filter);
                    source = filter;
                }

                // Index input by timestamp
                DBSPClosureExpression indexFunction =
                        new DBSPRawTupleExpression(
                                bounds.common.cast(commonType.setMayBeNull(false)),
                                param.asVariable().deref().applyClone()).closure(param);
                DBSPTypeIndexedZSet ix = new DBSPTypeIndexedZSet(operator.getNode(),
                        commonType.setMayBeNull(false),
                        inputType);
                DBSPMapIndexOperator index = new DBSPMapIndexOperator(operator.getNode(),
                        indexFunction, ix, operator.isMultiset, source);
                this.addOperator(index);

                // Apply window function.  Operator is incremental, so add D & I around it
                DBSPDifferentiateOperator diffIndex = new DBSPDifferentiateOperator(operator.getNode(), index);
                this.addOperator(diffIndex);
                DBSPOperator window = new DBSPWindowOperator(operator.getNode(), diffIndex, windowBounds);
                this.addOperator(window);
                DBSPOperator winInt = new DBSPIntegrateOperator(operator.getNode(), window);
                this.addOperator(winInt);

                // Deindex result of window
                DBSPOperator deindex = new DBSPDeindexOperator(operator.getNode(), winInt);
                this.map(operator, deindex);
                return;
            }

            DBSPOperator join = this.createJoin(operator);
            RewriteNowClosure rn = new RewriteNowClosure(this.errorReporter);
            function = rn.apply(function).to(DBSPExpression.class);
            DBSPOperator filter = new DBSPFilterOperator(operator.getNode(), function, join);
            this.addOperator(filter);
            // Drop the extra field
            DBSPVariablePath var = filter.getOutputZSetElementType().ref().var();
            List<DBSPExpression> fields = var.deref().allFields();
            Utilities.removeLast(fields);
            DBSPExpression drop = new DBSPTupleExpression(fields, false).closure(var.asParameter());
            DBSPMapOperator result = new DBSPMapOperator(operator.getNode(), drop, operator.getOutputZSetType(), filter);
            this.map(operator, result);
        }

        @Override
        public void startVisit(IDBSPOuterNode circuit) {
            super.startVisit(circuit);

            DBSPType timestamp = ContainsNow.timestampType();
            CalciteObject node = circuit.getNode();

            // We are doing this in startVisitor because we want now to be first in topological order,
            // otherwise it may be traversed too late, after nodes that should depend on it
            // (but don't yet, because they think they are calling a function).
            DBSPOperator now;
            boolean useSource = !this.compiler.compiler().options.ioOptions.nowStream;
            if (useSource) {
                now = new DBSPNowOperator(circuit.getNode());
            } else {
                // A table followed by a differentiator.
                String tableName = this.compiler.compiler().toCase("NOW");
                now = circuit.to(DBSPCircuit.class).circuit.getInput(tableName);
                if (now == null) {
                    throw new CompilationError("Declaration for table 'NOW' not found in program");
                }
                // Prevent processing it again by this visitor
                this.visited.add(now);
                this.addOperator(now);

                DBSPDifferentiateOperator dNow = new DBSPDifferentiateOperator(circuit.getNode(), now);
                now = dNow;
                dNow.annotations.add(new NoInc());
            }
            this.addOperator(now);
            this.map(now, now, false);

            DBSPVariablePath var = new DBSPTypeTuple(timestamp).ref().var();
            DBSPExpression indexFunction = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(),
                    var.deref().applyClone());
            this.nowIndexed = new DBSPMapIndexOperator(
                    node, indexFunction.closure(var.asParameter()),
                    TypeCompiler.makeIndexedZSet(new DBSPTypeTuple(), new DBSPTypeTuple(timestamp)), now);
            this.nowIndexed.addAnnotation(new AlwaysMonotone());
            this.addOperator(this.nowIndexed);
        }
    }

    public ImplementNow(IErrorReporter reporter, ICompilerComponent compiler) {
        super(reporter);
        boolean removeTable = !compiler.compiler().options.ioOptions.nowStream;
        ContainsNow cn = new ContainsNow(reporter, false);
        RewriteNow rewriteNow = new RewriteNow(reporter, compiler);
        this.passes.add(cn.getCircuitVisitor());
        this.passes.add(new Conditional(reporter, rewriteNow, cn::found));
        this.passes.add(new Conditional(reporter, new RemoveNow(reporter, compiler), () -> !cn.found || removeTable));
        ContainsNow cn0 = new ContainsNow(reporter, false);
        this.passes.add(cn0.getCircuitVisitor());
        this.passes.add(new Conditional(reporter,
                new Fail(reporter, "Instances of 'now' have not been replaced"), cn0::found));
    }
}
