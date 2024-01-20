package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.util.FreshName;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/** Generates new names for structs and struct fields that
 * are not legal Rust identifiers.  Notice that some Rust identifiers
 * cannot be used as names, e.g., 'None'.  So we always prefix the names
 * with a string that makes them legal identifiers. */
public class SanitizeStructNames extends InnerRewriteVisitor {
    protected FreshName nameGenerator;
    protected final Map<String, String> remapped;

    public SanitizeStructNames(IErrorReporter reporter, FreshName nameGenerator) {
        super(reporter);
        this.nameGenerator = nameGenerator;
        this.remapped = new HashMap<>();
    }

    String sanitizeName(String name, String prefix, boolean reuse) {
        if (reuse && this.remapped.containsKey(name))
            return this.remapped.get(name);
        String result = nameGenerator.freshName(prefix);
        if (reuse)
            this.remapped.put(name, result);
        return result;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct.Field field) {
        this.push(field);
        DBSPType type = this.transform(field.type);
        String sanitizedName = this.sanitizeName(field.name, "field", false);
        DBSPTypeStruct.Field result = new DBSPTypeStruct.Field(
                field.getNode(), field.name, sanitizedName, type, field.nameIsQuoted);
        this.pop(field);
        this.map(field, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct type) {
        this.push(type);
        List<DBSPTypeStruct.Field> fields = new ArrayList<>();
        FreshName save = this.nameGenerator;
        this.nameGenerator = new FreshName(new HashSet<>());  // local names for fields
        for (DBSPTypeStruct.Field f: type.fields.values()) {
            f.accept(this);
            DBSPTypeStruct.Field field = this.getResult().to(DBSPTypeStruct.Field.class);
            fields.add(field);
        }
        this.pop(type);
        this.nameGenerator = save;
        String saneName = type.name;
        if (!Utilities.isLegalRustIdentifier(saneName))
            saneName = "TABLE";
        String sanitizedName = this.sanitizeName(type.name, saneName, true);
        DBSPType result = new DBSPTypeStruct(type.getNode(), type.name, sanitizedName, fields);
        this.map(type, result);
        return VisitDecision.STOP;
    }
}
