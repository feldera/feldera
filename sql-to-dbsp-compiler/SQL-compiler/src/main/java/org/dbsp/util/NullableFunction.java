package org.dbsp.util;

import javax.annotation.Nullable;

/** Variant of {@link java.util.function.Function} interface with nullable result */
@FunctionalInterface
public interface NullableFunction<S, R> {
    @Nullable R apply(S source);
}
