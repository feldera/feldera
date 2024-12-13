package org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields;

import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** Describes how the fields of each parameter are remapped */
public class ParameterFieldRemap {
    final Map<DBSPParameter, FieldMap> remap;

    public ParameterFieldRemap() {
        this.remap = new HashMap<>();
    }

    @Nullable
    public FieldMap get(DBSPParameter param) {
        return this.remap.get(param);
    }

    public void setMap(DBSPParameter parameter, FieldMap map) {
        Utilities.putNew(this.remap, parameter, map);
    }

    public void changeMap(DBSPParameter parameter, FieldMap map) {
        this.remap.put(parameter, map);
    }

    @Override
    public String toString() {
        return this.remap.toString();
    }
}
