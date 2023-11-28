package org.dbsp.util;

@FunctionalInterface
public interface TriFunction<T, U, V, R> {
    R apply(T var1, U var2, V var3);
}