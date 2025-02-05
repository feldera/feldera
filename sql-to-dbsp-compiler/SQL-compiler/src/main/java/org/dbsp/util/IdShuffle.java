package org.dbsp.util;

import java.util.List;

/** Identity shuffle */
public class IdShuffle implements Shuffle {
    final int inputLength;

    public IdShuffle(int inputLength) {
        this.inputLength = inputLength;
    }

    @Override
    public int inputLength() {
        return this.inputLength;
    }

    @Override
    public <T> List<T> shuffle(List<T> data) {
        assert data.size() <= this.inputLength :
            "Shuffling " + data.size() + " more than expected " + this.inputLength;
        return data;
    }

    @Override
    public Shuffle after(Shuffle shuffle) {
        return shuffle;
    }

    @Override
    public boolean emitsIndex(int index) {
        return true;
    }
}
