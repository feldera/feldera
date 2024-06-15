package org.dbsp.simulator.types;

/**
 * Interface implemented by weights.
 */
public interface WeightType<T> {
    T add(T left, T right);
    T negate(T value);
    T zero();
    T one();
    boolean isZero(T left);
    boolean greaterThanZero(T value);
    T multiply(T left, T right);
}
