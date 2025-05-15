package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Var;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// In Calcite this kind of class is generated automatically, but here this was
// produced by hand-editing the ImmutableSetOpToFilterRule class.
final class ImmutableSetopOptimizerRule {
    private ImmutableSetopOptimizerRule() {}

    static final class Config implements SetopOptimizerRule.Config {
        private final RelBuilderFactory relBuilderFactory;
        private final @Nullable java.lang.@org.checkerframework.checker.nullness.qual.Nullable String description;
        private final RelRule.OperandTransform operandSupplier;
        private final RelRule.MatchHandler<SetopOptimizerRule> matchHandler;

        private Config(ImmutableSetopOptimizerRule.Config.Builder builder) {
            this.description = builder.description;
            this.matchHandler = builder.matchHandler;
            if (builder.relBuilderFactory != null) {
                initShim.withRelBuilderFactory(builder.relBuilderFactory);
            }
            if (builder.operandSupplier != null) {
                initShim.withOperandSupplier(builder.operandSupplier);
            }
            this.relBuilderFactory = initShim.relBuilderFactory();
            this.operandSupplier = initShim.operandSupplier();
            this.initShim = null;
        }

        private Config(
                RelBuilderFactory relBuilderFactory,
                @Nullable java.lang.@org.checkerframework.checker.nullness.qual.Nullable String description,
                RelRule.OperandTransform operandSupplier,
                RelRule.MatchHandler<SetopOptimizerRule> matchHandler) {
            this.relBuilderFactory = relBuilderFactory;
            this.description = description;
            this.operandSupplier = operandSupplier;
            this.matchHandler = matchHandler;
            this.initShim = null;
        }

        private static final byte STAGE_INITIALIZING = -1;
        private static final byte STAGE_UNINITIALIZED = 0;
        private static final byte STAGE_INITIALIZED = 1;
        private transient volatile ImmutableSetopOptimizerRule.Config.InitShim initShim = new ImmutableSetopOptimizerRule.Config.InitShim();

        private final class InitShim {
            private byte relBuilderFactoryBuildStage = STAGE_UNINITIALIZED;
            private RelBuilderFactory relBuilderFactory;

            RelBuilderFactory relBuilderFactory() {
                if (relBuilderFactoryBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
                if (relBuilderFactoryBuildStage == STAGE_UNINITIALIZED) {
                    relBuilderFactoryBuildStage = STAGE_INITIALIZING;
                    this.relBuilderFactory = Objects.requireNonNull(relBuilderFactoryInitialize(), "relBuilderFactory");
                    relBuilderFactoryBuildStage = STAGE_INITIALIZED;
                }
                return this.relBuilderFactory;
            }

            void withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
                this.relBuilderFactory = relBuilderFactory;
                relBuilderFactoryBuildStage = STAGE_INITIALIZED;
            }

            private byte operandSupplierBuildStage = STAGE_UNINITIALIZED;
            private RelRule.OperandTransform operandSupplier;

            RelRule.OperandTransform operandSupplier() {
                if (operandSupplierBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
                if (operandSupplierBuildStage == STAGE_UNINITIALIZED) {
                    operandSupplierBuildStage = STAGE_INITIALIZING;
                    this.operandSupplier = Objects.requireNonNull(operandSupplierInitialize(), "operandSupplier");
                    operandSupplierBuildStage = STAGE_INITIALIZED;
                }
                return this.operandSupplier;
            }

            void withOperandSupplier(RelRule.OperandTransform operandSupplier) {
                this.operandSupplier = operandSupplier;
                operandSupplierBuildStage = STAGE_INITIALIZED;
            }

            private String formatInitCycleMessage() {
                List<String> attributes = new ArrayList<>();
                if (relBuilderFactoryBuildStage == STAGE_INITIALIZING) attributes.add("relBuilderFactory");
                if (operandSupplierBuildStage == STAGE_INITIALIZING) attributes.add("operandSupplier");
                return "Cannot build Config, attribute initializers form cycle " + attributes;
            }
        }

        private RelBuilderFactory relBuilderFactoryInitialize() {
            return SetopOptimizerRule.Config.super.relBuilderFactory();
        }

        private RelRule.OperandTransform operandSupplierInitialize() {
            return SetopOptimizerRule.Config.super.operandSupplier();
        }

        @Override
        public RelBuilderFactory relBuilderFactory() {
            ImmutableSetopOptimizerRule.Config.InitShim shim = this.initShim;
            return shim != null
                    ? shim.relBuilderFactory()
                    : this.relBuilderFactory;
        }

        @Override
        public @Nullable java.lang.@org.checkerframework.checker.nullness.qual.Nullable String description() {
            return description;
        }

        @Override
        public RelRule.OperandTransform operandSupplier() {
            ImmutableSetopOptimizerRule.Config.InitShim shim = this.initShim;
            return shim != null
                    ? shim.operandSupplier()
                    : this.operandSupplier;
        }

        @Override
        public RelRule.MatchHandler<SetopOptimizerRule> matchHandler() {
            return matchHandler;
        }

        public final ImmutableSetopOptimizerRule.Config withRelBuilderFactory(RelBuilderFactory value) {
            if (this.relBuilderFactory == value) return this;
            RelBuilderFactory newValue = Objects.requireNonNull(value, "relBuilderFactory");
            return new ImmutableSetopOptimizerRule.Config(newValue, this.description, this.operandSupplier, this.matchHandler);
        }

        /**
         * Copy the current immutable object by setting a value for the {@link SetopOptimizerRule.Config#description() description} attribute.
         * An equals check used to prevent copying of the same value by returning {@code this}.
         * @param value A new value for description (can be {@code null})
         * @return A modified copy of the {@code this} object
         */
        public final ImmutableSetopOptimizerRule.Config withDescription(@Nullable java.lang.@org.checkerframework.checker.nullness.qual.Nullable String value) {
            if (Objects.equals(this.description, value)) return this;
            return new ImmutableSetopOptimizerRule.Config(this.relBuilderFactory, value, this.operandSupplier, this.matchHandler);
        }

        /**
         * Copy the current immutable object by setting a value for the {@link SetopOptimizerRule.Config#operandSupplier() operandSupplier} attribute.
         * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
         * @param value A new value for operandSupplier
         * @return A modified copy of the {@code this} object
         */
        public final ImmutableSetopOptimizerRule.Config withOperandSupplier(RelRule.OperandTransform value) {
            if (this.operandSupplier == value) return this;
            RelRule.OperandTransform newValue = Objects.requireNonNull(value, "operandSupplier");
            return new ImmutableSetopOptimizerRule.Config(this.relBuilderFactory, this.description, newValue, this.matchHandler);
        }

        /**
         * Copy the current immutable object by setting a value for the {@link SetopOptimizerRule.Config#matchHandler() matchHandler} attribute.
         * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
         * @param value A new value for matchHandler
         * @return A modified copy of the {@code this} object
         */
        public final ImmutableSetopOptimizerRule.Config withMatchHandler(RelRule.MatchHandler<SetopOptimizerRule> value) {
            if (this.matchHandler == value) return this;
            RelRule.MatchHandler<SetopOptimizerRule> newValue = Objects.requireNonNull(value, "matchHandler");
            return new ImmutableSetopOptimizerRule.Config(this.relBuilderFactory, this.description, this.operandSupplier, newValue);
        }

        /**
         * This instance is equal to all instances of {@code Config} that have equal attribute values.
         * @return {@code true} if {@code this} is equal to {@code another} instance
         */
        @Override
        public boolean equals(@Nullable Object another) {
            if (this == another) return true;
            return another instanceof ImmutableSetopOptimizerRule.Config
                    && equalTo((ImmutableSetopOptimizerRule.Config) another);
        }

        private boolean equalTo(ImmutableSetopOptimizerRule.Config another) {
            return relBuilderFactory.equals(another.relBuilderFactory)
                    && Objects.equals(description, another.description)
                    && operandSupplier.equals(another.operandSupplier)
                    && matchHandler.equals(another.matchHandler);
        }

        /**
         * Computes a hash code from attributes: {@code relBuilderFactory}, {@code description}, {@code operandSupplier}, {@code matchHandler}.
         * @return hashCode value
         */
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

        public static ImmutableSetopOptimizerRule.Config copyOf(SetopOptimizerRule.Config instance) {
            if (instance instanceof ImmutableSetopOptimizerRule.Config) {
                return (ImmutableSetopOptimizerRule.Config) instance;
            }
            return ImmutableSetopOptimizerRule.Config.builder()
                    .from(instance)
                    .build();
        }

        public static ImmutableSetopOptimizerRule.Config.Builder builder() {
            return new ImmutableSetopOptimizerRule.Config.Builder();
        }

        public static final class Builder {
            private static final long INIT_BIT_MATCH_HANDLER = 0x1L;
            private long initBits = 0x1L;

            private @Nullable RelBuilderFactory relBuilderFactory;
            private @Nullable java.lang.@org.checkerframework.checker.nullness.qual.Nullable String description;
            private @Nullable RelRule.OperandTransform operandSupplier;
            private @Nullable RelRule.MatchHandler<SetopOptimizerRule> matchHandler;

            private Builder() {
            }

            public final ImmutableSetopOptimizerRule.Config.Builder from(RelRule.Config instance) {
                Objects.requireNonNull(instance, "instance");
                from((Object) instance);
                return this;
            }

            public final ImmutableSetopOptimizerRule.Config.Builder from(SetopOptimizerRule.Config instance) {
                Objects.requireNonNull(instance, "instance");
                from((Object) instance);
                return this;
            }

            private void from(Object object) {
                if (object instanceof RelRule.Config) {
                    RelRule.Config instance = (RelRule.Config) object;
                    withRelBuilderFactory(instance.relBuilderFactory());
                    withOperandSupplier(instance.operandSupplier());
                    @Nullable java.lang.@org.checkerframework.checker.nullness.qual.Nullable String descriptionValue = instance.description();
                    if (descriptionValue != null) {
                        withDescription(descriptionValue);
                    }
                }
                if (object instanceof SetopOptimizerRule.Config) {
                    SetopOptimizerRule.Config instance = (SetopOptimizerRule.Config) object;
                    withMatchHandler(instance.matchHandler());
                }
            }

            public final ImmutableSetopOptimizerRule.Config.Builder withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
                this.relBuilderFactory = Objects.requireNonNull(relBuilderFactory, "relBuilderFactory");
                return this;
            }

            public ImmutableSetopOptimizerRule.Config.Builder withDescription(@Nullable java.lang.@org.checkerframework.checker.nullness.qual.Nullable String description) {
                this.description = description;
                return this;
            }

            public final ImmutableSetopOptimizerRule.Config.Builder withOperandSupplier(RelRule.OperandTransform operandSupplier) {
                this.operandSupplier = Objects.requireNonNull(operandSupplier, "operandSupplier");
                return this;
            }

            public final ImmutableSetopOptimizerRule.Config.Builder withMatchHandler(RelRule.MatchHandler<SetopOptimizerRule> matchHandler) {
                this.matchHandler = Objects.requireNonNull(matchHandler, "matchHandler");
                initBits &= ~INIT_BIT_MATCH_HANDLER;
                return this;
            }

            public ImmutableSetopOptimizerRule.Config build() {
                if (initBits != 0) {
                    throw new IllegalStateException(formatRequiredAttributesMessage());
                }
                return new ImmutableSetopOptimizerRule.Config(this);
            }

            private String formatRequiredAttributesMessage() {
                List<String> attributes = new ArrayList<>();
                if ((initBits & INIT_BIT_MATCH_HANDLER) != 0) attributes.add("matchHandler");
                return "Cannot build Config, some of required attributes are not set " + attributes;
            }
        }
    }
}

