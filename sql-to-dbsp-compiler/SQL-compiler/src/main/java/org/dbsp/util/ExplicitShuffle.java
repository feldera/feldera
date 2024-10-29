package org.dbsp.util;

import java.util.ArrayList;
import java.util.List;

public class ExplicitShuffle implements Shuffle {
    final List<Integer> indexes;

    public ExplicitShuffle(List<Integer> elements) {
        this.indexes = elements;
    }

    @Override
    public <T> List<T> shuffle(List<T> data) {
        List<T> result = new ArrayList<>(this.indexes.size());
        for (int index: this.indexes)
            result.add(data.get(index));
        return result;
    }
}
