package org.dbsp.util;

import java.util.HashMap;
import java.util.Map;

/** A Bijection is a pair of one-to-one maps */
public class Bijection<L, R> {
    Map<L, R> left;
    Map<R, L> right;

    public Bijection() {
        this.left = new HashMap<>();
        this.right = new HashMap<>();
    }

    public void map(L left, R right) {
        Utilities.putNew(this.left, left, right);
        Utilities.putNew(this.right, right, left);
    }

    public boolean has(L left) { return this.left.containsKey(left); }

    public boolean hasRight(R right) { return this.right.containsKey(right); }

    public R getRight(L left) {
        return Utilities.getExists(this.left, left);
    }

    public L getLeft(R right) {
        return Utilities.getExists(this.right, right);
    }
}
