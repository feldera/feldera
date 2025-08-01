package org.dbsp.simulator.types;

import java.math.BigInteger;

/**
 * Weights represented as unlimited precision integers.
 */
public class UnlimitedWeightType implements WeightType {
    static record UnlimitedWeight(BigInteger weight) implements Weight {}

    static BigInteger get(Weight w) {
        return w.to(UnlimitedWeight.class).weight();
    }

    private UnlimitedWeightType() {}

    public static UnlimitedWeightType INSTANCE = new UnlimitedWeightType();

    @Override
    public Weight add(Weight left, Weight right) {
        return new UnlimitedWeight(get(left).add(get(right)));
    }

    @Override
    public Weight negate(Weight value) {
        return new UnlimitedWeight(get(value).negate());
    }

    @Override
    public Weight zero() {
        return new UnlimitedWeight(BigInteger.ZERO);
    }

    @Override
    public Weight one() { return new UnlimitedWeight(BigInteger.ONE); }

    public boolean isZero(Weight value) {
        return get(value).equals(BigInteger.ZERO);
    }

    @Override
    public boolean greaterThanZero(Weight value) {
        return get(value).compareTo(BigInteger.ZERO) > 0;
    }

    @Override
    public Weight multiply(Weight left, Weight right) {
        return new UnlimitedWeight(get(left).multiply(get(right)));
    }
}
