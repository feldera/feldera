package org.dbsp.util;

import javax.annotation.Nullable;

/** Variant of {@link java.util.function.Predicate} interface with nullable result */
@FunctionalInterface
public interface NullablePredicate<T> {
    @Nullable Boolean test(T value);
}
