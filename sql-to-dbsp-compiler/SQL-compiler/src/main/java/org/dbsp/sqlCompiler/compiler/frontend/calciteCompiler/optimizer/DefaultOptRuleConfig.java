package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Var;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;

import java.util.Objects;

public class DefaultOptRuleConfig<R extends RelOptRule> implements RelRule.Config {
    @javax.annotation.Nullable @Nullable
    private final RelBuilderFactory relBuilderFactory;
    private final @Nullable String description;
    private final RelRule.OperandTransform operandSupplier;
    @javax.annotation.Nullable private final RelRule.MatchHandler<R> matchHandler;

    private DefaultOptRuleConfig(DefaultOptRuleConfig.Builder<R> builder) {
        this.description = null;
        this.matchHandler = builder.matchHandler;
        this.relBuilderFactory = Objects.requireNonNull(relBuilderFactoryInitialize(), "relBuilderFactory");
        this.operandSupplier = Objects.requireNonNull(operandSupplierInitialize(), "operandSupplier");
    }

    private DefaultOptRuleConfig(
            @javax.annotation.Nullable RelBuilderFactory relBuilderFactory,
            @javax.annotation.Nullable @Nullable String description,
            RelRule.OperandTransform operandSupplier,
            RelRule.MatchHandler<R> matchHandler) {
        this.relBuilderFactory = relBuilderFactory;
        this.description = description;
        this.operandSupplier = operandSupplier;
        this.matchHandler = matchHandler;
    }

    private RelBuilderFactory relBuilderFactoryInitialize() {
        return RelRule.Config.super.relBuilderFactory();
    }

    private RelRule.OperandTransform operandSupplierInitialize() {
        return RelRule.Config.super.operandSupplier();
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
    public RelRule.OperandTransform operandSupplier() {
        return this.operandSupplier;
    }

    public RelRule.MatchHandler<R> matchHandler() {
        return matchHandler;
    }

    public DefaultOptRuleConfig<R> withRelBuilderFactory(RelBuilderFactory value) {
        if (this.relBuilderFactory == value) return this;
        RelBuilderFactory newValue = Objects.requireNonNull(value, "relBuilderFactory");
        return new DefaultOptRuleConfig<R>(newValue, this.description, this.operandSupplier, this.matchHandler);
    }

    public DefaultOptRuleConfig<R> withDescription(@javax.annotation.Nullable @Nullable String value) {
        if (Objects.equals(this.description, value)) return this;
        return new DefaultOptRuleConfig<R>(this.relBuilderFactory, value, this.operandSupplier, this.matchHandler);
    }

    public DefaultOptRuleConfig<R> withOperandSupplier(RelRule.OperandTransform value) {
        if (this.operandSupplier == value) return this;
        RelRule.OperandTransform newValue = Objects.requireNonNull(value, "operandSupplier");
        return new DefaultOptRuleConfig<>(this.relBuilderFactory, this.description, newValue, this.matchHandler);
    }

    public DefaultOptRuleConfig<R> withMatchHandler(RelRule.MatchHandler<R> value) {
        if (this.matchHandler == value) return this;
        RelRule.MatchHandler<R> newValue = Objects.requireNonNull(value, "matchHandler");
        return new DefaultOptRuleConfig<>(this.relBuilderFactory, this.description, this.operandSupplier, newValue);
    }

    @Override
    public boolean equals(@javax.annotation.Nullable Object another) {
        if (this == another) return true;
        return another instanceof DefaultOptRuleConfig
                && equalTo((DefaultOptRuleConfig<R>) another);
    }

    private boolean equalTo(DefaultOptRuleConfig<R> another) {
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

    public static <R extends RelOptRule> DefaultOptRuleConfig.Builder<R> builder() {
        return new DefaultOptRuleConfig.Builder<R>();
    }

    public static final class Builder<C extends RelOptRule> {
        private @javax.annotation.Nullable RelRule.MatchHandler<C> matchHandler;

        public Builder() {}

        public DefaultOptRuleConfig.Builder<C> withMatchHandler(RelRule.MatchHandler<C> matchHandler) {
            this.matchHandler = Objects.requireNonNull(matchHandler, "matchHandler");
            return this;
        }

        public DefaultOptRuleConfig<C> build() {
            return new DefaultOptRuleConfig<C>(this);
        }
    }

    public static <C extends RelOptRule> DefaultOptRuleConfig<C> create() {
        return new DefaultOptRuleConfig<C>(new Builder<C>());
    }

    @Override
    public RelOptRule toRule() {
        throw new UnimplementedException();
    }
}
