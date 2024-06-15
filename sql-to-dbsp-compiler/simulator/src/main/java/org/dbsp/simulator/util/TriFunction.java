package org.dbsp.simulator.util;

import javax.annotation.Nullable;

@FunctionalInterface
public interface TriFunction<T, U, V, R> {
    @Nullable R apply(@Nullable T var1, @Nullable U var2, @Nullable V var3);
}