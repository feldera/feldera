package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.util.NameGen;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Holds information about the user-defined struct types */
class GlobalTypes {
    final NameGen structNameGen = new NameGen("struct_");
    final Map<String, DBSPTypeStruct> declarations = new HashMap<>();

    public String generateSaneName(String name) {
        if (this.declarations.containsKey(name))
            return this.declarations.get(name).sanitizedName;
        // After a sane name is generated, we expect that the structure will be shortly registered.
        return structNameGen.nextName();
    }

    public void register(DBSPTypeStruct struct) {
        Utilities.putNew(this.declarations, struct.name, struct);
    }

    public DBSPTypeStruct getStructByName(String name) {
        return Utilities.getExists(this.declarations, name);
    }

    public boolean containsStruct(String name) {
        return this.declarations.containsKey(name);
    }
}
