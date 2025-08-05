package org.dbsp.simulator.collections;

import org.dbsp.simulator.AggregateDescription;
import org.dbsp.simulator.types.Weight;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.util.IIndentStream;
import org.dbsp.simulator.util.ToIndentableString;
import org.dbsp.simulator.values.DynamicSqlValue;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

@SuppressWarnings("unchecked")
public class IndexedZSet<Key extends DynamicSqlValue, Value extends DynamicSqlValue>
        extends BaseCollection
        implements ToIndentableString {
    final Map<Key, ZSet<Value>> index;

    public IndexedZSet(WeightType weightType) {
        super(weightType);
        index = new HashMap<>();
    }

    @Override
    public void append(BaseCollection other) {
        this.append((IndexedZSet<Key, Value>) other);
    }

    public void append(Key key, Value value, Weight weight) {
        ZSet<Value> zset = this.index.getOrDefault(
                key, new ZSet<>(this.weightType));
        if (zset.isEmpty())
            // This is a new key
            this.index.put(key, zset);
        zset.append(value, weight);
        if (zset.isEmpty())
            // The group has become empty
            this.index.remove(key);
    }

    public void append(Key key, ZSet<Value> value) {
        if (!this.index.containsKey(key))
            this.index.put(key, value);
        else {
            ZSet<Value> existing = this.index.get(key);
            existing.append(value);
            if (existing.isEmpty()) {
                this.index.remove(key);
            }
        }
    }

    public void append(IndexedZSet<Key, Value> other) {
        for (var entry: other.index.entrySet()) {
            this.append(entry.getKey(), entry.getValue());
        }
    }

    public <Result extends DynamicSqlValue, OtherValue extends DynamicSqlValue> IndexedZSet<Key, Result> join(
            IndexedZSet<Key, OtherValue> other,
            BiFunction<Value, OtherValue, Result> combiner) {
        IndexedZSet<Key, Result> result = new IndexedZSet<>(this.weightType);
        for (Key key: this.index.keySet()) {
            if (other.index.containsKey(key)) {
                ZSet<Value> left = this.index.get(key);
                ZSet<OtherValue> right = other.index.get(key);
                ZSet<Result> product = left.multiply(right, combiner);
                result.index.put(key, product);
            }
        }
        return result;
    }

    public <Result extends DynamicSqlValue, IntermediateResult> IndexedZSet<Key, Result>
    aggregate(AggregateDescription<Result, IntermediateResult, Value> aggregate) {
        IndexedZSet<Key, Result> result = new IndexedZSet<>(this.weightType);
        for (Key key: this.index.keySet()) {
            ZSet<Value> set = this.index.get(key);
            Result agg = set.aggregate(aggregate);
            result.append(key, agg, this.weightType.one());
        }
        return result;
    }

    public ZSet<Value> deindex() {
        return this.flatten((k, v) -> v);
    }

    public <Result extends DynamicSqlValue> ZSet<Result> flatten(BiFunction<Key, Value, Result> combine) {
        ZSet<Result> result = new ZSet<>(this.weightType);
        for (Key key: this.index.keySet()) {
            ZSet<Value> set = this.index.get(key);
            ZSet<Result> map = set.map(v -> combine.apply(key, v));
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
            ZSet<Value> data = this.index.get(key);
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
