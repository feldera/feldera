package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;
import java.util.function.Predicate;

/** Recognizes EXCEPT written using a LEFT JOIN and reconstructs it.
 *
 * <p>Plan before:                                          should denote the same field
 * LogicalProject(EXPR$0=[true])                /--------------------------------------\
 *   LogicalFilter(condition=[NOT(IS NOT NULL($1))])                                   |
 *     LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])   |
 *       LogicalTableScan(table=[[schema, g]])                                         |
 *       LogicalAggregate(group=[{}], agg#0=[MIN($0)])                                 |
 *         LogicalProject($f0=[true])              /-----------------------------------/
 *           LogicalFilter(condition=[=($0, $cor0.arg1)])
 *             LogicalTableScan(table=[[schema, f]])
 *
 * <p>Plan after:
 * LogicalProject(EXPR$0=[true])
 *   LogicalMinus(all=[false])
 *     LogicalProject(arg1=[$0])
 *       LogicalTableScan(table=[[schema, g]])
 *     LogicalProject(arg1=[$0])
 *       LogicalTableScan(table=[[schema, f]])
 */
public class ExceptOptimizerRule
        extends RelRule<ExceptOptimizerRule.Config>
        implements TransformationRule {

    protected ExceptOptimizerRule(Config config) {
        super(config);
    }

    //~ Methods ----------------------------------------------------------------

    @Override public void onMatch(RelOptRuleCall call) {
        config.matchHandler().accept(this, call);
    }

    private void match(RelOptRuleCall call) {
        Project outer = call.rel(0);
        Filter outerFilter = call.rel(1);
        Correlate cor = call.rel(2);
        RelNode leftInput = call.rel(3).stripped();
        Filter innerFilter = call.rel(6);
        RelNode rightInput = innerFilter.stripped().getInput(0);

        // Additional validation
        // outerFilter condition is IS NULL($1) or NOT(IS NOT NULL($1))
        RexNode outerCondition = outerFilter.getCondition();
        int outerFieldIndex;
        if (outerCondition.isA(SqlKind.NOT)) {
            RexCall not = (RexCall) outerCondition;
            RexNode op0 = not.getOperands().get(0);
            if (op0.isA(SqlKind.IS_NOT_NULL)) {
                RexCall notNull = (RexCall) op0;
                RexNode opIn = notNull.getOperands().get(0);
                if (opIn instanceof RexInputRef ref) {
                    outerFieldIndex = ref.getIndex() - cor.stripped().getInput(0).getRowType().getFieldCount();
                } else {
                    return;
                }
            } else {
                // No match.
                return;
            }
        } else if (outerCondition.isA(SqlKind.IS_NULL)) {
            RexCall isNull = (RexCall) outerCondition;
            RexNode opIn = isNull.getOperands().get(0);
            if (opIn instanceof RexInputRef ref) {
                outerFieldIndex = ref.getIndex() - cor.stripped().getInput(0).getRowType().getFieldCount();
            } else {
                return;
            }
        } else {
            return;
        }

        // innerFilter condition is =($0, $cor0.$1)
        CorrelationId correlVar = cor.getCorrelationId();
        int innerFieldIndex;
        RexNode innerCondition = innerFilter.getCondition();
        if (innerCondition.isA(SqlKind.EQUALS) || innerCondition.isA(SqlKind.IS_NOT_DISTINCT_FROM)) {
            RexNode left = ((RexCall) innerCondition).getOperands().get(0);
            RexNode right = ((RexCall) innerCondition).getOperands().get(1);
            if (left.getType().isNullable() || right.getType().isNullable())
                return;
            if (left instanceof RexInputRef ref) {
                innerFieldIndex = ref.getIndex();
                if (right instanceof RexFieldAccess fa) {
                    RexNode data = fa.getReferenceExpr();
                    RelDataTypeField field = fa.getField();
                    if (field.getIndex() != outerFieldIndex)
                        return;
                    if (data instanceof RexCorrelVariable innerCor) {
                        if (!innerCor.id.equals(correlVar))
                            return;
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            } else if (right instanceof RexInputRef ref) {
                innerFieldIndex = ref.getIndex();
                if (left instanceof RexFieldAccess fa) {
                    RexNode data = fa.getReferenceExpr();
                    RelDataTypeField field = fa.getField();
                    if (field.getIndex() != outerFieldIndex)
                        return;
                    if (data instanceof RexCorrelVariable innerCor) {
                        if (!innerCor.id.equals(correlVar))
                            return;
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            } else {
                return;
            }
        } else {
            return;
        }

        final RelBuilder builder = call.builder();
        builder.push(leftInput);
        builder.project(builder.field(outerFieldIndex));
        builder.push(rightInput);
        builder.project(builder.field(innerFieldIndex));
        builder.minus(false);
        builder.project(outer.getProjects().get(0));
        call.transformTo(builder.build());
    }

    @SuppressWarnings("unused")
    public static final class ExceptOptimizerRuleConfig implements ExceptOptimizerRule.Config {
        private final GenericRuleConfigState<ExceptOptimizerRule> state;

        private ExceptOptimizerRuleConfig(Builder builder) {
            this.state = new GenericRuleConfigState<>(
                    ExceptOptimizerRule.Config.super.relBuilderFactory(),
                    builder.description,
                    ExceptOptimizerRule.Config.super.operandSupplier(),
                    builder.matchHandler);
        }

        private ExceptOptimizerRuleConfig(GenericRuleConfigState<ExceptOptimizerRule> state) {
            this.state = state;
        }

        @Override @javax.annotation.Nullable
        public RelBuilderFactory relBuilderFactory() {
            return this.state.relBuilderFactory();
        }

        @Override
        public @javax.annotation.Nullable @Nullable String description() {
            return this.state.description();
        }

        @Override
        public OperandTransform operandSupplier() {
            return this.state.operandSupplier();
        }

        @Override
        public MatchHandler<ExceptOptimizerRule> matchHandler() {
            return this.state.matchHandler();
        }

        public ExceptOptimizerRuleConfig withRelBuilderFactory(RelBuilderFactory value) {
            return new ExceptOptimizerRuleConfig(this.state.withRelBuilderFactory(value));
        }

        public ExceptOptimizerRuleConfig withDescription(@javax.annotation.Nullable @Nullable String value) {
            return new ExceptOptimizerRuleConfig(this.state.withDescription(value));
        }

        public ExceptOptimizerRuleConfig withOperandSupplier(OperandTransform value) {
            return new ExceptOptimizerRuleConfig(this.state.withOperandSupplier(value));
        }

        public ExceptOptimizerRuleConfig withMatchHandler(MatchHandler<ExceptOptimizerRule> matchHandler) {
            return new ExceptOptimizerRuleConfig(this.state.withMatchHandler(matchHandler));
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private final @javax.annotation.Nullable
            @Nullable String description = null;

            private @javax.annotation.Nullable MatchHandler<ExceptOptimizerRule> matchHandler;

            private Builder() {}

            public Builder withMatchHandler(MatchHandler<ExceptOptimizerRule> matchHandler) {
                this.matchHandler = Objects.requireNonNull(matchHandler, "matchHandler");
                return this;
            }

            public ExceptOptimizerRuleConfig build() {
                return new ExceptOptimizerRuleConfig(this);
            }
        }
    }

    public static final Predicate<Project> PROJECTS_TO_TRUE = p -> p.getProjects().size() == 1 &&
            p.getProjects().get(0).isAlwaysTrue();

    public interface Config extends RelRule.Config {
        Config DEFAULT = ExceptOptimizerRuleConfig.builder()
                .withMatchHandler(ExceptOptimizerRule::match)
                .build()
                .withOperandSupplier(
                        b0 -> b0.operand(Project.class)
                                .predicate(PROJECTS_TO_TRUE).oneInput(
                                        b1 -> b1.operand(Filter.class)
                                                .oneInput(b2 -> b2.operand(Correlate.class)
                                                        .predicate(c -> c.getJoinType() == JoinRelType.LEFT)
                                                        .inputs(b3 -> b3.operand(RelNode.class).anyInputs(),
                                                                b4 -> b4.operand(Aggregate.class)
                                                                        .predicate(a -> a.getGroupCount() == 0 &&
                                                                                a.getAggCallList().size() == 1 &&
                                                                                a.getAggCallList().get(0).getAggregation().getKind() == SqlKind.MIN)
                                                                        .oneInput(b5 -> b5.operand(Project.class)
                                                                                .predicate(PROJECTS_TO_TRUE)
                                                                                .oneInput(b6 -> b6.operand(Filter.class)
                                                                                        .anyInputs()))))))
                .as(Config.class);

        @Override default ExceptOptimizerRule toRule() {
            return new ExceptOptimizerRule(this);
        }

        MatchHandler<ExceptOptimizerRule> matchHandler();
    }
}
