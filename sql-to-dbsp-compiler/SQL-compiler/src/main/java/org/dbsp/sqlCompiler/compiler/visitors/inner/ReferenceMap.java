package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** Maps (variable) names to their declarations. */
public class ReferenceMap {
    final Map<DBSPVariablePath, IDBSPDeclaration> declarations;

    public ReferenceMap() {
        this.declarations = new HashMap<>();
    }

    public void declare(DBSPVariablePath var, IDBSPDeclaration declaration) {
        if (this.declarations.containsKey(var)) {
            IDBSPDeclaration decl = this.declarations.get(var);
            if (decl != declaration)
                throw new InternalCompilerError("Changing declaration of " + var + " from\n" +
                        decl + " to\n" + declaration, var);
            return;
        }
        Utilities.putNew(this.declarations, var, declaration);
    }

    public IDBSPDeclaration getDeclaration(DBSPVariablePath var) {
        return Utilities.getExists(this.declarations, var);
    }

    @Nullable
    public IDBSPDeclaration get(DBSPVariablePath var) {
        return this.declarations.get(var);
    }

    public void clear() {
        this.declarations.clear();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (var kv: this.declarations.entrySet()) {
            builder.append(kv.getKey().toString())
                    .append("(")
                    .append(kv.getKey().id)
                    .append(")=>")
                    .append(kv.getValue().getClass().getSimpleName())
                    .append("(")
                    .append(kv.getValue().getId())
                    .append(")")
                    .append(System.lineSeparator());
        }
        return builder.toString();
    }
}
