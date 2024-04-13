package org.dbsp.util;

import java.util.ArrayList;
import java.util.List;

public class ExplicitShuffle implements Shuffle {
    final List<Integer> indexes;

    /** Create an empty shuffle: no values produced in output */
    public ExplicitShuffle() {
        this.indexes = new ArrayList<>();
    }

    public ExplicitShuffle(List<Integer> elements) {
        this.indexes = elements;
    }

    public void add(int index) {
        this.indexes.add(index);
    }

    @Override
    public <T> List<T> shuffle(List<T> data) {
        List<T> result = new ArrayList<>(this.indexes.size());
        for (int index: this.indexes)
            result.add(data.get(index));
        return result;
    }
}
