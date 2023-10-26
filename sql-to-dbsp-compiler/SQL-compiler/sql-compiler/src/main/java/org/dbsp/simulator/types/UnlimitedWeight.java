package org.dbsp.simulator.types;

import java.math.BigInteger;

/**
 * Weights represented as unlimited precision integers.
 */
public class UnlimitedWeight implements WeightType<BigInteger> {
    private UnlimitedWeight() {}

    public static UnlimitedWeight INSTANCE = new UnlimitedWeight();

    @Override
    public BigInteger add(BigInteger left, BigInteger right) {
        return left.add(right);
    }

    @Override
    public BigInteger negate(BigInteger value) {
        return value.negate();
    }

    @Override
    public BigInteger zero() {
        return BigInteger.ZERO;
    }

    @Override
    public BigInteger one() { return BigInteger.ONE; }

    public boolean isZero(BigInteger value) {
        return value.equals(BigInteger.ZERO);
    }

    @Override
    public boolean greaterThanZero(BigInteger value) {
        return value.compareTo(BigInteger.ZERO) > 0;
    }

    @Override
    public BigInteger multiply(BigInteger left, BigInteger right) {
        return left.multiply(right);
    }
}
