package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Var;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/** State that is common to Calcite optimization rules configurations. */
public class GenericRuleConfigState<T extends RelOptRule> {
    @javax.annotation.Nullable private RelBuilderFactory relBuilderFactory;
    @javax.annotation.Nullable private @Nullable String description;
    @javax.annotation.Nullable private RelRule.OperandTransform operandSupplier;
    @javax.annotation.Nullable private RelRule.MatchHandler<T> matchHandler;

    protected GenericRuleConfigState(
            @javax.annotation.Nullable RelBuilderFactory relBuilderFactory,
            @javax.annotation.Nullable @Nullable String description,
            @javax.annotation.Nullable RelRule.OperandTransform operandSupplier,
            @javax.annotation.Nullable RelRule.MatchHandler<T> matchHandler) {
        this.relBuilderFactory = relBuilderFactory;
        this.description = description;
        this.operandSupplier = operandSupplier;
        this.matchHandler = matchHandler;
    }

    protected void set(@javax.annotation.Nullable RelBuilderFactory relBuilderFactory,
                       @javax.annotation.Nullable @Nullable String description,
                       RelRule.OperandTransform operandSupplier,
                       @javax.annotation.Nullable RelRule.MatchHandler<T> matchHandler) {
        this.relBuilderFactory = relBuilderFactory;
        this.description = description;
        this.operandSupplier = operandSupplier;
        this.matchHandler = matchHandler;
    }

    @javax.annotation.Nullable
    public RelBuilderFactory relBuilderFactory() {
        return this.relBuilderFactory;
    }

    public @javax.annotation.Nullable @Nullable String description() {
        return description;
    }

    public RelRule.OperandTransform operandSupplier() {
        return Objects.requireNonNull(this.operandSupplier);
    }

    public RelRule.MatchHandler<T> matchHandler() {
        return Objects.requireNonNull(this.matchHandler);
    }

    public GenericRuleConfigState<T> withRelBuilderFactory(RelBuilderFactory value) {
        if (this.relBuilderFactory == value) return this;
        RelBuilderFactory newValue = Objects.requireNonNull(value, "relBuilderFactory");
        return new GenericRuleConfigState<>(newValue, this.description, this.operandSupplier, this.matchHandler);
    }

    public GenericRuleConfigState<T> withDescription(@javax.annotation.Nullable @Nullable String value) {
        if (Objects.equals(this.description, value)) return this;
        return new GenericRuleConfigState<>(this.relBuilderFactory, value, this.operandSupplier, this.matchHandler);
    }

    public GenericRuleConfigState<T> withOperandSupplier(RelRule.OperandTransform value) {
        if (this.operandSupplier == value) return this;
        RelRule.OperandTransform newValue = Objects.requireNonNull(value, "operandSupplier");
        return new GenericRuleConfigState<>(this.relBuilderFactory, this.description, newValue, this.matchHandler);
    }

    public GenericRuleConfigState<T> withMatchHandler(RelRule.MatchHandler<T> value) {
        if (this.matchHandler == value) return this;
        RelRule.MatchHandler<T> newValue = Objects.requireNonNull(value, "matchHandler");
        return new GenericRuleConfigState<>(this.relBuilderFactory, this.description, this.operandSupplier, newValue);
    }

    @Override
    public boolean equals(@javax.annotation.Nullable Object another) {
        if (this == another) return true;
        //noinspection unchecked
        return another instanceof GenericRuleConfigState
                && equalTo((GenericRuleConfigState<T>) another);
    }

    private boolean equalTo(GenericRuleConfigState<T> another) {
        return Objects.equals(relBuilderFactory, another.relBuilderFactory)
                && Objects.equals(description, another.description)
                && Objects.equals(operandSupplier, another.operandSupplier)
                && Objects.equals(matchHandler, another.matchHandler);
    }

    @Override
    public int hashCode() {
        @Var int h = 5381;
        h += (h << 5) + Objects.hashCode(relBuilderFactory);
        h += (h << 5) + Objects.hashCode(description);
        h += (h << 5) + Objects.hashCode(operandSupplier);
        h += (h << 5) + Objects.hashCode(matchHandler);
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
}
