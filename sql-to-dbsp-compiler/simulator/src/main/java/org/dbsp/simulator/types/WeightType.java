package org.dbsp.simulator.types;

public interface WeightType {
    Weight add(Weight left, Weight right);
    Weight negate(Weight value);
    Weight zero();
    Weight one();
    boolean isZero(Weight left);
    boolean greaterThanZero(Weight value);
    Weight multiply(Weight left, Weight right);
}
