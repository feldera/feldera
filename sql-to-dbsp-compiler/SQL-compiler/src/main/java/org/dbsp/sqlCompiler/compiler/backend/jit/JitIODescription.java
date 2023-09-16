package org.dbsp.sqlCompiler.compiler.backend.jit;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.io.IOException;

/**
 * Base class for various kinds of JIT I/Os.
 */
public abstract class JitIODescription {
    public final String path;
    public final String relation;

    protected JitIODescription(String relation, String path) {
        this.relation = relation;
        this.path = path;
    }

    String getPath() {
        return this.path;
    }

    /**
     * A Json representation of this input or output.
     */
    public abstract JsonNode asJson();

    /**
     * Parse the contents of the file and expect to find a ZSet
     * with elements of the specified type.
     */
    public DBSPZSetLiteral.Contents parse(DBSPType elementType) throws IOException {
        throw new UnsupportedException(CalciteObject.EMPTY);
    }
}
