package org.dbsp.sqlCompiler.compiler.visitors;

public enum VisitDecision {
    STOP,
    CONTINUE;

    public boolean stop() {
        return this.equals(STOP);
    }

    public boolean cont() {
        return this.equals(CONTINUE);
    }
}
