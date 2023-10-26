package org.dbsp.sqlCompiler.compiler.backend.jit;

/**
 * Represents a file that is passed as input or output to
 * a JIT executable and its serialization format.
 */
public class JitFileAndSerialization {
    public final String path;
    public final JitSerializationKind kind;

    public JitFileAndSerialization(String path, JitSerializationKind kind) {
        this.path = path;
        this.kind = kind;
    }
}
