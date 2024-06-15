package org.dbsp.simulator.collections;

import org.dbsp.simulator.AggregateDescription;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.util.IIndentStream;
import org.dbsp.simulator.util.ToIndentableString;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class IndexedZSet<Key, Value, Weight> extends BaseCollection<Weight> implements ToIndentableString {
    final WeightType<Weight> weightType;
    final Map<Key, ZSet<Value, Weight>> index;

    public IndexedZSet(WeightType<Weight> weightType) {
        index = new HashMap<>();
        this.weightType = weightType;
    }

    public void append(Key key, Value value, Weight weight) {
        ZSet<Value, Weight> zset = this.index.getOrDefault(
                key, new ZSet<>(this.weightType));
        if (zset.isEmpty())
            // This is a new key
            this.index.put(key, zset);
        zset.append(value, weight);
        if (zset.isEmpty())
            // The group has become empty
            this.index.remove(key);
    }

    public <Result, OtherValue> IndexedZSet<Key, Result, Weight> join(
            IndexedZSet<Key, OtherValue, Weight> other,
            BiFunction<Value, OtherValue, Result> combiner) {
        IndexedZSet<Key, Result, Weight> result = new IndexedZSet<>(this.weightType);
        for (Key key: this.index.keySet()) {
            if (other.index.containsKey(key)) {
                ZSet<Value, Weight> left = this.index.get(key);
                ZSet<OtherValue, Weight> right = other.index.get(key);
                ZSet<Result, Weight> product = left.multiply(right, combiner);
                result.index.put(key, product);
            }
        }
        return result;
    }

    public <Result, IntermediateResult> IndexedZSet<Key, Result, Weight>
    aggregate(AggregateDescription<Result, IntermediateResult, Value, Weight> aggregate) {
        IndexedZSet<Key, Result, Weight> result = new IndexedZSet<>(this.weightType);
        for (Key key: this.index.keySet()) {
            ZSet<Value, Weight> set = this.index.get(key);
            Result agg = set.aggregate(aggregate);
            result.append(key, agg, this.weightType.one());
        }
        return result;
    }

    public ZSet<Value, Weight> deindex() {
        return this.flatten((k, v) -> v);
    }

    public <Result> ZSet<Result, Weight> flatten(BiFunction<Key, Value, Result> combine) {
        ZSet<Result, Weight> result = new ZSet<>(this.weightType);
        for (Key key: this.index.keySet()) {
            ZSet<Value, Weight> set = this.index.get(key);
            ZSet<Result, Weight> map = set.map(v -> combine.apply(key, v));
            result.append(map);
        }
        return result;
    }

    public int groupCount() {
        return this.index.size();
    }

    public IIndentStream toString(IIndentStream stream) {
        stream.append("{").increase();
        boolean first = true;
        for (Key key: this.index.keySet()) {
            ZSet<Value, Weight> data = this.index.get(key);
            if (!first)
                stream.append(",\n");
            first = false;
            stream.append(key.toString())
                .append("=>")
                .append(data);
        }
        return stream.decrease()
                .newline()
                .append("}");
    }
}
