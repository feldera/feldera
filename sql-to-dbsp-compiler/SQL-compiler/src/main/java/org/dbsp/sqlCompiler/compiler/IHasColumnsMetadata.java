package org.dbsp.sqlCompiler.compiler;

public interface IHasColumnsMetadata {
    Iterable<? extends IHasLateness> getLateness();
    Iterable<? extends IHasWatermark> getWatermarks();
}
