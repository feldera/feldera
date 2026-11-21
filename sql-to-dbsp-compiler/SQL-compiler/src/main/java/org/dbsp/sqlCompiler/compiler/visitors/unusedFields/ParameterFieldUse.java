package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Substitution;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Maps each parameter to a "bitmap" of the fields that are used. */
public class ParameterFieldUse {
    final Map<DBSPParameter, FieldUseMap> paramMap;

    public ParameterFieldUse() {
        this.paramMap = new HashMap<>();
    }

    /** Restore the field-use map for a single closure parameter from Rust's input paths. */
    public static ParameterFieldUse fromPaths(DBSPParameter parameter, List<List<Integer>> paths) {
        var uses = new ParameterFieldUse();
        if (paths.isEmpty())
            uses.set(parameter, new FieldUseMap(parameter.getType(), false));
        for (var path : paths)
            new ParameterField(parameter, path).markParameterUse(uses);
        return uses;
    }

    public Iterable<DBSPParameter> getParameters() {
        return this.paramMap.keySet();
    }

    public void add(DBSPParameter param, FieldUseMap map) {
        Utilities.putNew(this.paramMap, param, map);
        Utilities.enforce(param.getType().sameType(map.getType()));
    }

    public FieldUseMap get(DBSPParameter param) {
        return Utilities.getExists(this.paramMap, param);
    }

    /** The use map of 'param', created by 'create' if there is none */
    public FieldUseMap getOrAdd(DBSPParameter param, Function<DBSPParameter, FieldUseMap> create) {
        return this.paramMap.computeIfAbsent(param, create);
    }

    public void set(DBSPParameter parameter, FieldUseMap map) {
        Utilities.putNew(this.paramMap, parameter, map);
    }

    public void updateParameterValue(DBSPParameter parameter, FieldUseMap map) {
        Utilities.enforce(parameter.getType().sameType(map.getType()));
        this.paramMap.put(parameter, map);
    }

    public FieldUseMap getUse() {
        Utilities.enforce(this.paramMap.size() == 1);
        return this.paramMap.values().iterator().next();
    }

    public void clear() {
        this.paramMap.clear();
    }

    @Override
    public String toString() {
        return this.paramMap.toString();
    }

    public boolean hasUnusedFields(int depth) {
        for (FieldUseMap map: this.paramMap.values())
            if (map.hasUnusedFields(depth))
                return true;
        return false;
    }

    /** This = this union other */
    public void union(ParameterFieldUse other) {
        for (DBSPParameter param: this.paramMap.keySet()) {
            FieldUseMap fum = this.paramMap.get(param);
            if (other.paramMap.containsKey(param)) {
                FieldUseMap ou = other.get(param);
                this.paramMap.put(param, fum.reduce(ou));
            }
        }
        for (DBSPParameter param: other.paramMap.keySet()) {
            if (!this.paramMap.containsKey(param))
                this.paramMap.put(param, other.get(param));
        }
    }

    /** Create a visitor which will rewrite parameters to only contain the used fields.
     * @param depth Depth up to which unused fields are eliminated.
     * Note: you cannot mutate the finder; it and the rewrite share state. */
    public RewriteFields getFieldRewriter(DBSPCompiler compiler, int depth) {
        Substitution<DBSPParameter, DBSPParameter> newParam = new Substitution<>();
        Iterable<DBSPParameter> parameters = this.getParameters();
        if (compiler.usesCircuitSeam())
            parameters = this.paramMap.keySet().stream()
                    .sorted(java.util.Comparator.comparingLong(DBSPParameter::getId)).toList();
        for (DBSPParameter param: parameters) {
            FieldUseMap map = this.get(param);
            DBSPType newType = Objects.requireNonNull(map.compressedType(depth));
            newParam.substitute(param, new DBSPParameter(param.getNode(), param.name, newType));
        }
        return new RewriteFields(compiler, newParam, this, depth);
    }
}
