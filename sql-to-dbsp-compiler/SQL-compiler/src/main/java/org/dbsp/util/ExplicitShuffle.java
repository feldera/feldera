package org.dbsp.util;

import java.util.ArrayList;
import java.util.List;

public class ExplicitShuffle implements Shuffle {
    final int inputLength;
    final List<Integer> indexes;

    public ExplicitShuffle(int inputLength, List<Integer> elements) {
        this.indexes = elements;
        this.inputLength = inputLength;
    }

    @Override
    public int inputLength() {
        return this.inputLength;
    }

    @Override
    public <T> List<T> shuffle(List<T> data) {
        List<T> result = new ArrayList<>(this.indexes.size());
        for (int index: this.indexes)
            result.add(data.get(index));
        return result;
    }

    @Override
    public Shuffle after(Shuffle shuffle) {
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < shuffle.inputLength(); i++)
            results.add(i);
        results = shuffle.shuffle(results);
        results = this.shuffle(results);
        return new ExplicitShuffle(shuffle.inputLength(), results);
    }

    @Override
    public boolean emitsIndex(int index) {
        return this.indexes.contains(index);
    }

    @Override
    public Shuffle invert() {
        // This implementation is correct only if the shuffle is a permutation,
        // something we don't check.
        assert this.inputLength == this.indexes.size();
        List<Integer> inverse = Linq.fill(this.inputLength, 0);
        for (int i = 0; i < this.inputLength; i++)
            inverse.set(this.indexes.get(i), i);
        return new ExplicitShuffle(this.inputLength, inverse);
    }

    @Override
    public boolean isIdentityPermutation() {
        for (int i = 0; i < this.inputLength; i++)
            if (this.indexes.get(i) != i) return false;
        return true;
    }

    public static <T> Shuffle computePermutation(List<T> input, List<T> output) {
        List<Integer> shuffle = new ArrayList<>();
        for (T in : input) {
            int index = output.indexOf(in);
            assert index >= 0 : "Input " + in + " not found in output";
            assert !shuffle.contains(index) : "Input " + in + " appears twice";
            shuffle.add(index);
        }
        return new ExplicitShuffle(input.size(), shuffle);
    }

    @Override
    public String toString() {
        return this.indexes.toString();
    }
}
