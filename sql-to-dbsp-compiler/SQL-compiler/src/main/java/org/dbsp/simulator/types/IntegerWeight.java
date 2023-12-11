package org.dbsp.simulator.types;

/**
 * Weights represented as integers.
 * Throws on overflow.
 */
public class IntegerWeight implements WeightType<Integer> {
    private IntegerWeight() {}

    public static final IntegerWeight INSTANCE = new IntegerWeight();

    @Override
    public Integer add(Integer left, Integer right) {
        return Math.addExact(left, right);
    }

    @Override
    public Integer negate(Integer value) {
        return Math.negateExact(value);
    }

    @Override
    public Integer zero() {
        return 0;
    }

    @Override
    public Integer one() { return 1; }

    public boolean isZero(Integer value) {
        return value == 0;
    }

    @Override
    public boolean greaterThanZero(Integer value) {
        return value > 0;
    }

    @Override
    public Integer multiply(Integer left, Integer right) {
        return Math.multiplyExact(left, right);
    }
}
