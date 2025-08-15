package org.dbsp.sqlCompiler.compiler;

public interface IHasColumnsMetadata {
    Iterable<? extends IColumnMetadata> getColumnsMetadata();
}
