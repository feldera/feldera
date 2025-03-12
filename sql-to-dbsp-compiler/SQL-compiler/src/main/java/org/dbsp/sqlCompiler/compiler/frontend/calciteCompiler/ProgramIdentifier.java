package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.util.IJson;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import java.util.Objects;

public final class ProgramIdentifier implements IJson {
    private final String name;
    private final boolean isQuoted;

    public ProgramIdentifier(String name, boolean isQuoted) {
        this.name = name;
        this.isQuoted = isQuoted;
    }

    public static ProgramIdentifier EMPTY = new ProgramIdentifier("", false);

    public static ProgramIdentifier fromJson(JsonNode node) {
        String name = Utilities.getStringProperty(node, "name");
        boolean isQuoted = Utilities.getBooleanProperty(node, "isQuoted");
        return new ProgramIdentifier(name, isQuoted);
    }

    @Override
    public void asJson(ToJsonInnerVisitor visitor) {
        visitor.stream.beginObject()
                .label("name").append(this.name)
                .label("isQuoted").append(this.isQuoted)
                .endObject();
    }

    @Override
    public String toString() {
        return this.name;
    }

    public String singleQuote() {
        return Utilities.singleQuote(this.name);
    }

    public String name() {
        return this.name;
    }

    public boolean isQuoted() {
        return this.isQuoted;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ProgramIdentifier) obj;
        return Objects.equals(this.name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
