package org.dbsp.sqlCompiler.compiler.backend.jit.ir.types;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.util.ICastable;

/**
 * Interface implemented by types that can be elements of ZSets:
 * either JITRowType (for ZSets) orJITKVType (for IndexedZSets).
 */
public interface IJitKvOrRowType extends ICastable {
    /**
     * Add the description of this type to the specified
     * JSON parent node.
     * @param parent  JSON parent node.
     * @param label   Label to use for this type in the parent.
     */
    void addDescriptionTo(ObjectNode parent, String label);
}
