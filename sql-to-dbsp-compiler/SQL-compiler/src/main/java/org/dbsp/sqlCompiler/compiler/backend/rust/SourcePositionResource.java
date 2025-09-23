package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;
import org.dbsp.util.HashString;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** Used to implement a resource file for each Rust circuit generated, which
 * allows source position information to be encoded in string keys.
 * We do this because we hope that keys do not change as much as source positions
 * when the program is edited.
 *
 * <p>At compile-time we map each source position that may surface in an error message
 * to a key.  We then emit the key->position map in the generated Rust code as a SourceMap
 * Rust data structure.  This class is used to build the key->position map.
 */
public class SourcePositionResource {
    // The global Merkle hash of an operator and the index of an error handling operation within that operator
    record HashAndIndex(String hash, int index) {}
    // Hash string used for expressions evaluated outside any operator
    public static final String NO_OPERATOR_HASH = "global";

    /** Maps each key to the actual position represented */
    final Map<HashAndIndex, SourcePosition> keyToPosition = new HashMap<>();

    String getOperatorName(@Nullable DBSPOperator operator) {
        if (operator == null)
            return NO_OPERATOR_HASH;
        HashString hash = OperatorHash.getHash(operator, true);
        Utilities.enforce(hash != null);
        return hash.toString();
    }

    /** Allocate a resource key for the specified expression in the specified operator.
     * The operator may be null for expressions that do not belong to an operator. */
    public void allocateKey(@Nullable DBSPOperator operator, DBSPHandleErrorExpression expression) {
        SourcePositionRange range = expression.getNode().getPositionRange();
        if (range.isValid()) {
            HashAndIndex key = this.getKey(operator, expression);
            // This may overwrite if the expression processed is not a tree, but that should be benign.
            // This can occur due to CSE, for example
            this.keyToPosition.put(key, range.start);
        }
    }

    /* Retrieve a previously-allocated resource key for the specified expression in the specified operator */
    HashAndIndex getKey(@Nullable DBSPOperator operator, DBSPHandleErrorExpression expression) {
        String name = this.getOperatorName(operator);
        return new HashAndIndex(name, expression.index);
    }

    public static final String STATIC_NAME = "SOURCE_MAP_STATIC";

    public boolean isEmpty() {
        return this.keyToPosition.isEmpty();
    }

    public static void generateDeclaration(IIndentStream builder) {
        builder.append("pub static ")
                .append(STATIC_NAME)
                .append(": ")
                .append("OnceLock<SourceMap> = OnceLock::new();")
                .newline();
    }

    public void generateInitializer(IIndentStream builder) {
        builder.append(STATIC_NAME)
                .append(".get_or_init(|| {")
                .increase()
                .append("let mut m = SourceMap::new();")
                .newline();
        for (var entry : this.keyToPosition.entrySet()) {
            builder.append("m.insert(\"")
                    .append(entry.getKey().hash)
                    .append("\", ")
                    .append(entry.getKey().index)
                    .append(", ")
                    .append(entry.getValue().toRustConstant())
                    .append(");")
                    .newline();
        }
        builder.append("m")
                .newline()
                .decrease().append("});")
                .newline();
    }

    public static void generateReference(IIndentStream builder, String sourceMapName) {
        builder.append("let ")
                .append(sourceMapName)
                .append(" = ")
                .append(STATIC_NAME)
                .append(".get().unwrap();")
                .newline();
    }
}
