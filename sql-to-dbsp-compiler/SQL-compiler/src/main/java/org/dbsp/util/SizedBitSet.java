package org.dbsp.util;

import java.util.BitSet;

/** A BitSet and the number of bits it represents */
public record SizedBitSet(int size, BitSet bits) {
    public SizedBitSet(int size) {
        this(size, new BitSet());
    }

    public boolean hasUnusedBits() {
        return this.size > this.bits.cardinality();
    }

    public int cardinality() {
        return this.bits.cardinality();
    }

    public void set(int fieldNo) {
        this.bits.set(fieldNo);
    }

    public void set(int start, int end, boolean b) {
        this.bits.set(start, end, b);
    }

    public boolean get(int i) {
        return this.bits.get(i);
    }

    public void set(int start, int end) {
        this.bits.set(start, end);
    }
}
