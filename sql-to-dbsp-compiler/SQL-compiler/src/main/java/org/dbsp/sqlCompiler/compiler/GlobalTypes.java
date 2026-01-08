package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** Holds information about the user-defined struct types */
class GlobalTypes {
    final Map<ProgramIdentifier, DBSPTypeStruct> declarations = new HashMap<>();

    public void register(ProgramIdentifier name, DBSPTypeStruct struct) {
        Utilities.putNew(this.declarations, name, struct);
    }

    @Nullable
    public DBSPTypeStruct getStructByName(ProgramIdentifier name) {
        return this.declarations.get(name);
    }

    public boolean containsStruct(ProgramIdentifier name) {
        return this.declarations.containsKey(name);
    }
}
