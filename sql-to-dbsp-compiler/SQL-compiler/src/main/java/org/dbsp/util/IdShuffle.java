package org.dbsp.util;

import com.fasterxml.jackson.databind.JsonNode;

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
        Utilities.enforce(data.size() <= this.inputLength,
                () -> "Shuffling " + data.size() + " more than expected " + this.inputLength);
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

    @Override
    public Shuffle invert() {
        return this;
    }

    @Override
    public boolean isIdentityPermutation() {
        return true;
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject()
                .appendClass(this)
                .label("inputLength")
                .append(this.inputLength)
                .endObject();
    }

    public static IdShuffle fromJson(JsonNode node) {
        int inputLength = Utilities.getIntProperty(node, "inputLength");
        return new IdShuffle(inputLength);
    }
}
