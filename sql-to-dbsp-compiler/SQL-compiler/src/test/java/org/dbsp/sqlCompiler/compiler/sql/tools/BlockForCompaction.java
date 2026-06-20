package org.dbsp.sqlCompiler.compiler.sql.tools;

/** Emit a command to block for compaction */
public class BlockForCompaction implements IStreamCommand {
    @Override
    public boolean compatible(IStreamCommand other) {
        return true;
    }
}
