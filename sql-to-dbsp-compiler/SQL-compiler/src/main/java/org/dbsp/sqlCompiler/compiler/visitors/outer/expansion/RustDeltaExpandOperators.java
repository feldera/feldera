package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPositiveOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Bind the Rust delta graph to live operators and give the limiter visitor
 * the exact operators that produced each term of the derivative. */
public final class RustDeltaExpandOperators extends DeltaExpandOperators {
    public enum JoinForm { INNER, FILTERED_INNER, LEFT_OUTER, FILTERED_LEFT_OUTER }
    public enum Term {
        LEFT, RIGHT, LEFT_INTEGRAL, RIGHT_INTEGRAL, LEFT_DELTA, RIGHT_DELTA, BOTH,
        ANTI_JOIN, UNMATCHED, LEFT_FILTER, RIGHT_FILTER, BOTH_FILTER, UNMATCHED_FILTER, SUM
    }
    public record Step(Term output, List<Term> inputs) {}

    // The edge lists depend only on the join form. Indexed outputs use the
    // same formula as ZSet outputs and bind different live operator classes.
    private static final Map<JoinForm, List<Step>> FORMULAS = RustCircuitPass.deltaJoinFormulas();

    public RustDeltaExpandOperators(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public String getName() {
        return "DeltaExpandOperators";
    }

    private void expandStar(DBSPStarJoinBaseOperator original) {
        var inputs = original.inputs.stream().map(this::mapped).toList();
        var expanded = RustCircuitPass.starDelta(this.compiler, original, inputs);
        expanded.operators().forEach(this::addOperator);
        this.map(original, expanded.output(), false);
        this.addExpansion(original, expanded.correspondence());
    }

    @Override
    public void postorder(DBSPStarJoinOperator operator) {
        this.expandStar(operator);
    }

    @Override
    public void postorder(DBSPStarJoinIndexOperator operator) {
        this.expandStar(operator);
    }

    @Override
    public void postorder(DBSPStarJoinFilterMapOperator operator) {
        this.expandStar(operator);
    }

    private void expandUnary(DBSPUnaryOperator original) {
        var expanded = RustCircuitPass.unaryDelta(this.compiler, original, this.mapped(original.input()));
        expanded.operators().forEach(this::addOperator);
        this.map(original, expanded.output(), false);
        this.addExpansion(original, expanded.correspondence());
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        this.expandUnary(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        this.expandUnary(operator);
    }

    @Override
    public void postorder(DBSPPositiveOperator operator) {
        this.expandUnary(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        this.expand(operator, JoinForm.INNER, null);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        this.expand(operator, JoinForm.INNER, null);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        Utilities.enforce(operator.map == null,
                () -> "JoinFilterMap carries a map before the monotone analysis: " + operator);
        this.expand(operator, JoinForm.FILTERED_INNER, operator.getFilter());
    }

    @Override
    void expandLeftJoin(DBSPJoinBaseOperator operator, @Nullable DBSPExpression filter) {
        this.expand(operator, filter == null ? JoinForm.LEFT_OUTER : JoinForm.FILTERED_LEFT_OUTER, filter);
    }

    private void expand(DBSPJoinBaseOperator original, JoinForm form, @Nullable DBSPExpression filter) {
        var terms = new Terms();
        terms.put(Term.LEFT, this.mapped(original.left()));
        terms.put(Term.RIGHT, this.mapped(original.right()));
        for (var step : FORMULAS.get(form)) {
            var inputs = step.inputs.stream().map(terms::port).toList();
            var node = original.getRelNode();
            DBSPSimpleOperator op = switch (step.output) {
                case LEFT_INTEGRAL, RIGHT_INTEGRAL -> {
                    var integral = new DBSPDelayedIntegralOperator(node, inputs.get(0));
                    integral.copyAnnotations((step.output == Term.LEFT_INTEGRAL ? original.left() : original.right()).node());
                    yield integral;
                }
                case LEFT_DELTA, RIGHT_DELTA, BOTH -> streamJoin(original, inputs.get(0), inputs.get(1));
                case ANTI_JOIN -> new DBSPAntiJoinOperator(node, inputs.get(0), inputs.get(1));
                case UNMATCHED -> {
                    var pad = RustCircuitPass.unmatchedRows(this.compiler, original);
                    yield original.outputType.is(DBSPTypeIndexedZSet.class)
                            ? new DBSPMapIndexOperator(node, pad, original.getOutputIndexedZSetType(), inputs.get(0))
                            : new DBSPMapOperator(node, pad, inputs.get(0));
                }
                case LEFT_FILTER, RIGHT_FILTER, BOTH_FILTER, UNMATCHED_FILTER ->
                        new DBSPFilterOperator(node, Objects.requireNonNull(filter), inputs.get(0));
                case SUM -> new DBSPSumOperator(node, inputs);
                case LEFT, RIGHT -> throw new IllegalStateException("Cannot replace a join input");
            };
            this.addOperator(op);
            terms.put(step.output, op.outputPort());
        }
        this.map(original, terms.get(Term.SUM, DBSPSumOperator.class), false);
        this.addExpansion(original, terms.correspondence(form));
    }

    private static final class Terms {
        private final EnumMap<Term, OutputPort> outputs = new EnumMap<>(Term.class);

        void put(Term term, OutputPort port) {
            Utilities.putNew(this.outputs, term, port);
        }

        OutputPort port(Term term) {
            return Utilities.getExists(this.outputs, term);
        }

        <T extends DBSPSimpleOperator> T get(Term term, Class<T> type) {
            return this.port(term).node().to(type);
        }

        OperatorDeltaExpansion correspondence(JoinForm form) {
            var left = this.get(Term.LEFT_INTEGRAL, DBSPDelayedIntegralOperator.class);
            var right = this.get(Term.RIGHT_INTEGRAL, DBSPDelayedIntegralOperator.class);
            var sum = this.get(Term.SUM, DBSPSumOperator.class);
            var leftDelta = this.get(Term.LEFT_DELTA, DBSPJoinBaseOperator.class);
            var rightDelta = this.get(Term.RIGHT_DELTA, DBSPJoinBaseOperator.class);
            var both = this.get(Term.BOTH, DBSPJoinBaseOperator.class);
            return switch (form) {
                case INNER -> leftDelta instanceof DBSPStreamJoinIndexOperator indexed
                        ? new JoinIndexDeltaExpansion(left, right, indexed, rightDelta.to(DBSPStreamJoinIndexOperator.class),
                                both.to(DBSPStreamJoinIndexOperator.class), sum)
                        : new JoinDeltaExpansion(left, right, leftDelta.to(DBSPStreamJoinOperator.class),
                                rightDelta.to(DBSPStreamJoinOperator.class), both.to(DBSPStreamJoinOperator.class), sum);
                case FILTERED_INNER -> new JoinFilterMapExpansion(left, right, leftDelta.to(DBSPStreamJoinOperator.class),
                        rightDelta.to(DBSPStreamJoinOperator.class), both.to(DBSPStreamJoinOperator.class),
                        this.filter(Term.LEFT_FILTER), this.filter(Term.RIGHT_FILTER), this.filter(Term.BOTH_FILTER), sum);
                case LEFT_OUTER -> new LeftJoinDeltaExpansion(left, right, leftDelta, rightDelta, both,
                        this.get(Term.ANTI_JOIN, DBSPAntiJoinOperator.class), this.port(Term.UNMATCHED).simpleNode(), sum);
                case FILTERED_LEFT_OUTER -> new LeftJoinFilterMapExpansion(left, right, leftDelta, rightDelta, both,
                        this.get(Term.ANTI_JOIN, DBSPAntiJoinOperator.class), this.port(Term.UNMATCHED).simpleNode(),
                        this.filter(Term.LEFT_FILTER), this.filter(Term.RIGHT_FILTER), this.filter(Term.BOTH_FILTER),
                        this.filter(Term.UNMATCHED_FILTER), sum);
            };
        }

        private DBSPFilterOperator filter(Term term) {
            return this.get(term, DBSPFilterOperator.class);
        }
    }
}
