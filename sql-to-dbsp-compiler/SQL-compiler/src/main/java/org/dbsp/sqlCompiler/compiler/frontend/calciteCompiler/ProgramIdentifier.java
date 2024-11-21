package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.dbsp.util.Utilities;

import java.util.Objects;

public final class ProgramIdentifier {
    private final String name;
    private final boolean isQuoted;

    public ProgramIdentifier(String name, boolean isQuoted) {
        this.name = name;
        this.isQuoted = isQuoted;
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
