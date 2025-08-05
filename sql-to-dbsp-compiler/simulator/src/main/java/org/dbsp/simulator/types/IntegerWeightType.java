package org.dbsp.simulator.types;

import org.dbsp.simulator.util.ICastable;

/**
 * Weights represented as integers.
 * Throws on overflow.
 */
public class IntegerWeightType implements WeightType {
    public record IntegerWeight(int weight) implements Weight {
        @Override
        public int asInteger() {
            return this.weight;
        }

        @Override
        public String toString() {
            return Integer.toString(this.weight);
        }
    }

    private IntegerWeightType() {}

    public static final IntegerWeightType INSTANCE = new IntegerWeightType();

    static int get(Weight w) {
        return w.to(IntegerWeight.class).weight();
    }

    public static IntegerWeight create(int w) {
        return new IntegerWeight(w);
    }

    @Override
    public Weight add(Weight left, Weight right) {
        return new IntegerWeight(Math.addExact(get(left), get(right)));
    }

    @Override
    public Weight negate(Weight value) {
        return new IntegerWeight(Math.negateExact(get(value)));
    }

    @Override
    public Weight zero() {
        return new IntegerWeight(0);
    }

    @Override
    public Weight one() { return new IntegerWeight(1); }

    public boolean isZero(Weight value) {
        return get(value) == 0;
    }

    @Override
    public boolean greaterThanZero(Weight value) {
        return get(value) > 0;
    }

    @Override
    public Weight multiply(Weight left, Weight right) {
        return new IntegerWeight(Math.multiplyExact(get(left), get(right)));
    }
}
