package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** Describes how the fields of each parameter are remapped */
public class ParameterFieldRemap {
    final Map<DBSPParameter, FieldUseMap> remap;

    public ParameterFieldRemap() {
        this.remap = new HashMap<>();
    }

    public Iterable<DBSPParameter> getParameters() {
        return this.remap.keySet();
    }

    public void add(DBSPParameter param, FieldUseMap map) {
        Utilities.putNew(this.remap, param, map);
        Utilities.enforce(param.getType().sameType(map.getType()));
    }

    @Nullable
    public FieldUseMap maybeGet(DBSPParameter param) {
        return this.remap.get(param);
    }

    public FieldUseMap get(DBSPParameter param) {
        return Utilities.getExists(this.remap, param);
    }

    public void set(DBSPParameter parameter, FieldUseMap map) {
        Utilities.putNew(this.remap, parameter, map);
    }

    public void changeMap(DBSPParameter parameter, FieldUseMap map) {
        Utilities.enforce(parameter.getType().sameType(map.getType()));
        this.remap.put(parameter, map);
    }

    public void clear() {
        this.remap.clear();
    }

    @Override
    public String toString() {
        return this.remap.toString();
    }

    public boolean hasUnusedFields(int depth) {
        for (FieldUseMap map: this.remap.values())
            if (map.hasUnusedFields(depth))
                return true;
        return false;
    }
}
