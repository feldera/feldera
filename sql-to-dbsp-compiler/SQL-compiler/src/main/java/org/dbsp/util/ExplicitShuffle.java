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

    public static <T> Shuffle computePermutation(List<T> input, List<T> output) {
        List<Integer> shuffle = new ArrayList<>();
        for (int i = 0; i < input.size(); i++) {
            T in = input.get(i);
            int index = output.indexOf(in);
            assert index >= 0 : "Input " + in + " not found in output";
            assert !shuffle.contains(index) : "Input " + in + " appears twice";
            shuffle.add(index);
        }
        return new ExplicitShuffle(shuffle);
    }
}
