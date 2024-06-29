package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

/** Base class for monotone type information */
public abstract class BaseMonotoneType implements IMaybeMonotoneType {
    static long nextId = 0;
    final long id;

    protected BaseMonotoneType() {
        this.id = nextId++;
    }

    public long getId() {
        return this.id;
    }
}
