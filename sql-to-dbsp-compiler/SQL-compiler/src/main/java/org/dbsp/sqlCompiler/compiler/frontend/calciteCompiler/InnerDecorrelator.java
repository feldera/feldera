package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Var;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/** The Calcite decorrelator is all or nothing: if it fails to decorrelate a query, it does nothing
 * for subqueries.  This optimization pass finds trees rooted at a Correlate node without any
 * Correlate children and tries to decorrelate them independently. */
public class InnerDecorrelator
        extends RelRule<InnerDecorrelator.Config>
        implements TransformationRule {

    protected InnerDecorrelator(Config config) {
        super(config);
    }

    //~ Methods ----------------------------------------------------------------

    @Override public void onMatch(RelOptRuleCall call) {
        config.matchHandler().accept(this, call);
    }

    private void match(RelOptRuleCall call) {
        Correlate cor = call.rel(0);
        RelNode stripped = CalciteOptimizer.stripRecursively(cor);
        for (RelNode input: stripped.getInputs()) {
            if (CalciteOptimizer.containsCorrelate(input))
                // Give up
                return;
        }

        RelNode rel = RelDecorrelator.decorrelateQuery(stripped, call.builder());
        if (rel != null
                && !CalciteOptimizer.containsCorrelate(rel)
                // This is necessary due to https://issues.apache.org/jira/browse/CALCITE-7024
                && cor.getRowType().equals(rel.getRowType())) {
            call.transformTo(rel);
        }
    }

    @SuppressWarnings({"DataFlowIssue", "unused"})
    public static final class InnerDecorrelatorConfig implements InnerDecorrelator.Config {
        @javax.annotation.Nullable @Nullable private final RelBuilderFactory relBuilderFactory;
        private final @Nullable String description;
        private final OperandTransform operandSupplier;
        @javax.annotation.Nullable private final MatchHandler<InnerDecorrelator> matchHandler;

        private InnerDecorrelatorConfig(Builder builder) {
            this.description = null;
            this.matchHandler = builder.matchHandler;
            this.relBuilderFactory = Objects.requireNonNull(relBuilderFactoryInitialize(), "relBuilderFactory");
            this.operandSupplier = Objects.requireNonNull(operandSupplierInitialize(), "operandSupplier");
        }

        private InnerDecorrelatorConfig(
                @javax.annotation.Nullable RelBuilderFactory relBuilderFactory,
                @javax.annotation.Nullable @Nullable String description,
                OperandTransform operandSupplier,
                MatchHandler<InnerDecorrelator> matchHandler) {
            this.relBuilderFactory = relBuilderFactory;
            this.description = description;
            this.operandSupplier = operandSupplier;
            this.matchHandler = matchHandler;
        }

        private RelBuilderFactory relBuilderFactoryInitialize() {
            return InnerDecorrelator.Config.super.relBuilderFactory();
        }

        private OperandTransform operandSupplierInitialize() {
            return InnerDecorrelator.Config.super.operandSupplier();
        }

        @Override @javax.annotation.Nullable
        public RelBuilderFactory relBuilderFactory() {
            return this.relBuilderFactory;
        }

        @Override
        public @javax.annotation.Nullable @Nullable String description() {
            return description;
        }

        @Override
        public OperandTransform operandSupplier() {
            return this.operandSupplier;
        }

        @Override
        public MatchHandler<InnerDecorrelator> matchHandler() {
            return matchHandler;
        }

        public InnerDecorrelatorConfig withRelBuilderFactory(RelBuilderFactory value) {
            if (this.relBuilderFactory == value) return this;
            RelBuilderFactory newValue = Objects.requireNonNull(value, "relBuilderFactory");
            return new InnerDecorrelatorConfig(newValue, this.description, this.operandSupplier, this.matchHandler);
        }

        public InnerDecorrelatorConfig withDescription(@javax.annotation.Nullable @Nullable String value) {
            if (Objects.equals(this.description, value)) return this;
            return new InnerDecorrelatorConfig(this.relBuilderFactory, value, this.operandSupplier, this.matchHandler);
        }

        public InnerDecorrelatorConfig withOperandSupplier(OperandTransform value) {
            if (this.operandSupplier == value) return this;
            OperandTransform newValue = Objects.requireNonNull(value, "operandSupplier");
            return new InnerDecorrelatorConfig(this.relBuilderFactory, this.description, newValue, this.matchHandler);
        }

        public InnerDecorrelatorConfig withMatchHandler(MatchHandler<InnerDecorrelator> value) {
            if (this.matchHandler == value) return this;
            MatchHandler<InnerDecorrelator> newValue = Objects.requireNonNull(value, "matchHandler");
            return new InnerDecorrelatorConfig(this.relBuilderFactory, this.description, this.operandSupplier, newValue);
        }

        @Override
        public boolean equals(@javax.annotation.Nullable Object another) {
            if (this == another) return true;
            return another instanceof InnerDecorrelatorConfig
                    && equalTo((InnerDecorrelatorConfig) another);
        }

        private boolean equalTo(InnerDecorrelatorConfig another) {
            return relBuilderFactory.equals(another.relBuilderFactory)
                    && Objects.equals(description, another.description)
                    && operandSupplier.equals(another.operandSupplier)
                    && matchHandler.equals(another.matchHandler);
        }

        @Override
        public int hashCode() {
            @Var int h = 5381;
            h += (h << 5) + relBuilderFactory.hashCode();
            h += (h << 5) + Objects.hashCode(description);
            h += (h << 5) + operandSupplier.hashCode();
            h += (h << 5) + matchHandler.hashCode();
            return h;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper("Config")
                    .omitNullValues()
                    .add("relBuilderFactory", relBuilderFactory)
                    .add("description", description)
                    .add("operandSupplier", operandSupplier)
                    .add("matchHandler", matchHandler)
                    .toString();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private @javax.annotation.Nullable MatchHandler<InnerDecorrelator> matchHandler;

            private Builder() {}

            public Builder withMatchHandler(MatchHandler<InnerDecorrelator> matchHandler) {
                this.matchHandler = Objects.requireNonNull(matchHandler, "matchHandler");
                return this;
            }

            public InnerDecorrelatorConfig build() {
                return new InnerDecorrelatorConfig(this);
            }
        }
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = InnerDecorrelatorConfig.builder()
                .withMatchHandler(InnerDecorrelator::match)
                .build()
                .withOperandSupplier(b0 -> b0.operand(Correlate.class)
                        .anyInputs())
                .as(Config.class);

        @Override default InnerDecorrelator toRule() {
            return new InnerDecorrelator(this);
        }

        MatchHandler<InnerDecorrelator> matchHandler();
    }
}
