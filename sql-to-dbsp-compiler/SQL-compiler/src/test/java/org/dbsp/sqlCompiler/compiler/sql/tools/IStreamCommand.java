package org.dbsp.sqlCompiler.compiler.sql.tools;

/** A command part of a stream of commands that a test supplies to a circuit */
public interface IStreamCommand {
    /** True if the two stream commands can be applied to the same circuit */
    boolean compatible(IStreamCommand other);
}
