package org.dbsp.util;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** A shuftle described explicitly using a list of input indexes in the order they appear in the eoutput */
public class ExplicitShuffle implements Shuffle {
    /** The length of the input sequence */
    final int inputLength;
    /** List of input indexes in the order they appear in the output */
    final List<Integer> indexes;

    public ExplicitShuffle(int inputLength, List<Integer> elements) {
        this.indexes = elements;
        this.inputLength = inputLength;
        for (Integer i: elements)
            Utilities.enforce(i >= 0 && i < inputLength);
    }

    @Override
    public int inputLength() {
        return this.inputLength;
    }

    @Override
    public int outputLength() {
        return this.indexes.size();
    }

    /** Apply the shuffle to a list of values; returns the shuffled values. */
    @Override
    public <T> List<T> shuffle(List<T> data) {
        Utilities.enforce(data.size() == this.inputLength);
        List<T> result = new ArrayList<>(this.indexes.size());
        for (int index: this.indexes)
            result.add(data.get(index));
        return result;
    }

    /** Compose two shuffles; the output of 'shuffle' must have the right size for the input of this */
    @Override
    public Shuffle after(Shuffle shuffle) {
        Utilities.enforce(this.inputLength() == shuffle.outputLength());
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
        Utilities.enforce(this.inputLength == this.indexes.size());
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
            Utilities.enforce(index >= 0, () -> "Input " + in + " not found in output");
            Utilities.enforce(!shuffle.contains(index), () -> "Input " + in + " appears twice");
            shuffle.add(index);
        }
        return new ExplicitShuffle(input.size(), shuffle);
    }

    @Override
    public String toString() {
        return this.indexes.toString();
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject()
                .appendClass(this)
                .label("inputLength")
                .append(this.inputLength)
                .label("indexes")
                .beginArray();
        for (int index: this.indexes)
            stream.append(index);
        stream.endArray()
                .endObject();
    }

    public static ExplicitShuffle fromJson(JsonNode node) {
        int inputLength = Utilities.getIntProperty(node, "inputLength");
        List<Integer> indexes = Linq.list(Linq.map(
                Utilities.getProperty(node, "indexes").elements(),
                JsonNode::asInt));
        return new ExplicitShuffle(inputLength, indexes);
    }

    /** Compress a shuffle by removing from the input list the specified indexes.
     * None of these indexes can be in the output list.
     * @param removeFromShuffle List of collection indexes to remove.  None of these indexes
     *                          can be in the list of emitted indexes.
     *
     * <p>For example, consider the shuffle [3, 0] where we remove the element with index 1.
     * The result is the shuffle [2, 0].
     */
    @Override
    public Shuffle compress(Set<Integer> removeFromShuffle) {
        // Maps each possible index to its new value
        List<Integer> newIndex = new ArrayList<>();
        int current = 0;
        for (int i = 0; i < this.inputLength; i++) {
            newIndex.add(current);
            if (!removeFromShuffle.contains(i))
                current++;
        }

        List<Integer> explicit = new ArrayList<>();
        for (Integer index: this.indexes) {
            Utilities.enforce(!removeFromShuffle.contains(index));
            explicit.add(newIndex.get(index));
        }
        return new ExplicitShuffle(this.inputLength() - removeFromShuffle.size(), explicit);
    }
}
