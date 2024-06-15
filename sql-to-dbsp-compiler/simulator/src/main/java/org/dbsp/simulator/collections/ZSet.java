package org.dbsp.simulator.collections;

import org.dbsp.simulator.AggregateDescription;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.util.IIndentStream;
import org.dbsp.simulator.util.ToIndentableString;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class ZSet<Data, Weight> extends BaseCollection<Weight> implements ToIndentableString {
    /** Maps values to weights.  Invariant: weights are never zero */
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

    public Weight getWeight(Data data) {
        if (this.data.containsKey(data))
            return this.data.get(data);
        return this.weightType.zero();
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

    public <OtherData, Result> ZSet<Result, Weight> multiply(
            ZSet<OtherData, Weight> other,
            BiFunction<Data, OtherData, Result> combiner) {
        ZSet<Result, Weight> result = new ZSet<>(this.weightType);
        for (Map.Entry<Data, Weight> entry: this.data.entrySet()) {
            for (Map.Entry<OtherData, Weight> otherEntry: other.data.entrySet()) {
                Result data = combiner.apply(entry.getKey(), otherEntry.getKey());
                Weight weight = this.weightType.multiply(entry.getValue(), otherEntry.getValue());
                result.append(data, weight);
            }
        }
        return result;
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

    public boolean equals(ZSet<Data, Weight> other) {
        return this.subtract(other).isEmpty();
    }

    public ZSet<Data, Weight> append(Data data) {
        this.append(data, this.weightType.one());
        return this;
    }

    public ZSet<Data, Weight> append(ZSet<Data, Weight> other) {
        for (Map.Entry<Data, Weight> entry: other.data.entrySet()) {
            this.data.merge(entry.getKey(), entry.getValue(), this::merger);
        }
        return this;
    }

    public ZSet<Data, Weight> positive(boolean set) {
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

    public ZSet<Data, Weight> distinct() {
        return this.positive(true);
    }

    public <OData> ZSet<OData, Weight> map(Function<Data, OData> tupleTransform) {
        Map<OData, Weight> result = new HashMap<>();
        for (Map.Entry<Data, Weight> entry: this.data.entrySet()) {
            Weight weight = entry.getValue();
            OData out = tupleTransform.apply(entry.getKey());
            result.merge(out, weight, this::merger);
        }
        return new ZSet<>(result, this.weightType);
    }

    public ZSet<Data, Weight> filter(Predicate<Data> keep) {
        Map<Data, Weight> result = new HashMap<>();
        for (Map.Entry<Data, Weight> entry: this.data.entrySet()) {
            Weight weight = entry.getValue();
            if (keep.test(entry.getKey()))
                result.put(entry.getKey(), weight);
        }
        return new ZSet<>(result, this.weightType);
    }

    public <Key> IndexedZSet<Key, Data, Weight> index(Function<Data, Key> key) {
        IndexedZSet<Key, Data, Weight> result = new IndexedZSet<>(this.weightType);
        for (Map.Entry<Data, Weight> entry: this.data.entrySet()) {
            Weight weight = entry.getValue();
            Key keyValue = key.apply(entry.getKey());
            result.append(keyValue, entry.getKey(), weight);
        }
        return result;
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

    public ZSet<Data, Weight> union(ZSet<Data, Weight> other) {
        return this.add(other).distinct();
    }

    public ZSet<Data, Weight> union_all(ZSet<Data, Weight> other) {
        return this.add(other);
    }

    public ZSet<Data, Weight> except(ZSet<Data, Weight> other) {
        return this.distinct().subtract(other.distinct()).distinct();
    }

    public <Result, IntermediateResult> Result aggregate(
            AggregateDescription<Result, IntermediateResult, Data, Weight> aggregate) {
        IntermediateResult result = aggregate.initialValue;
        for (Map.Entry<Data, Weight> entry : this.data.entrySet()) {
            Weight weight = entry.getValue();
            result = aggregate.update.apply(result, entry.getKey(), weight);
        }
        return aggregate.finalize.apply(result);
    }

    public boolean isEmpty() {
        return this.data.isEmpty();
    }

    public IIndentStream toString(IIndentStream stream) {
        stream.append("{").increase();
        boolean first = true;
        for (Map.Entry<Data, Weight> entry: this.data.entrySet()) {
            if (!first)
                stream.append(",").newline();
            first = false;
            stream.append(entry.getKey().toString())
                    .append(" => ")
                    .append(entry.getValue().toString());
        }
        return stream.decrease()
                .newline()
                .append("}");
    }
}
