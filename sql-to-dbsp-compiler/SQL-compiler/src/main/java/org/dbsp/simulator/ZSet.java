package org.dbsp.simulator;

import org.dbsp.simulator.types.WeightType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZSet<Data, Weight> {
    /** Maps values to weight.  Invariant: weights are never zero */
    final Map<Data, Weight> data;
    final WeightType<Weight> weightType;

    /** Create a Z-set by cloning the data from the specified map. */
    public ZSet(Map<Data, Weight> data, WeightType<Weight> weightType) {
        this.data = new HashMap<>();
        this.weightType = weightType;
        for (Map.Entry<Data, Weight> datum: data.entrySet()) {
            if (!this.weightType.isZero(datum.getValue()))
                this.data.put(datum.getKey(), datum.getValue());
        }
    }

    public int entryCount() {
        return this.data.size();
    }

    /** Create an empty Z-set */
    public ZSet(WeightType<Weight> weightType) {
        this.data = new HashMap<>();
        this.weightType = weightType;
    }

    public ZSet(Collection<Data> data, WeightType<Weight> weightType) {
        this.data = new HashMap<>();
        this.weightType = weightType;
        for (Data datum: data) {
            this.data.merge(datum, this.weightType.one(), this::merger);
        }
    }

    public ZSet<Data, Weight> negate() {
        Map<Data, Weight> result = new HashMap<>();
        for (Map.Entry<Data, Weight> entry: this.data.entrySet()) {
            result.put(entry.getKey(), this.weightType.negate(entry.getValue()));
        }
        return new ZSet<>(result, this.weightType);
    }

    public static <Data, Weight> ZSet<Data, Weight> zero(WeightType<Weight> weightType) {
        return new ZSet<>(weightType);
    }

    @Nullable
    Weight merger(Weight oldWeight, Weight newWeight) {
        Weight w = this.weightType.add(oldWeight, newWeight);
        if (this.weightType.isZero(w))
            return null;
        return w;
    }

    public ZSet<Data, Weight> add(ZSet<Data, Weight> other) {
        Map<Data, Weight> result = new HashMap<>(this.data);
        for (Map.Entry<Data, Weight> entry: other.data.entrySet()) {
            result.merge(entry.getKey(), entry.getValue(), this::merger);
        }
        return new ZSet<>(result, this.weightType);
    }

    public ZSet<Data, Weight> subtract(ZSet<Data, Weight> other) {
        Map<Data, Weight> result = new HashMap<>(this.data);
        for (Map.Entry<Data, Weight> entry: other.data.entrySet()) {
            result.merge(entry.getKey(), this.weightType.negate(entry.getValue()), this::merger);
        }
        return new ZSet<>(result, this.weightType);
    }

    public ZSet<Data, Weight> append(Data data, Weight weight) {
        this.data.merge(data, weight, this::merger);
        return this;
    }

    public ZSet<Data, Weight> append(Data data) {
        this.append(data, this.weightType.one());
        return this;
    }

    public ZSet<Data, Weight> distinct(boolean set) {
        Map<Data, Weight> result = new HashMap<>();
        for (Map.Entry<Data, Weight> entry: this.data.entrySet()) {
            Weight weight = entry.getValue();
            if (!this.weightType.greaterThanZero(weight))
                continue;
            if (set)
                weight = this.weightType.one();
            result.put(entry.getKey(), weight);
        }
        return new ZSet<>(result, this.weightType);
    }

    /** Returns a collection of all data items.
     * If an item has a negative weight, this throws an exception.
     * If an item has a larger weight, multiple copies are emitted. */
    public Collection<Data> toCollection() {
        List<Data> result = new ArrayList<>();
        for (Map.Entry<Data, Weight> entry : this.data.entrySet()) {
            Weight weight = entry.getValue();
            if (!this.weightType.greaterThanZero(weight))
                throw new RuntimeException("Entry with negative weight: " + entry);
            Weight minusOne = this.weightType.negate(this.weightType.one());
            while (this.weightType.greaterThanZero(weight)) {
                result.add(entry.getKey());
                weight = this.weightType.add(weight, minusOne);
            }
        }
        return result;
    }

    public boolean isEmpty() {
        return this.data.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("{\n");
        for (Map.Entry<Data, Weight> entry: this.data.entrySet()) {
            result.append(entry.getKey());
            result.append(" => ");
            result.append(entry.getValue());
            result.append("\n");
        }
        result.append("}");
        return result.toString();
    }
}
