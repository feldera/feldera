package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.util.ICastable;
import org.dbsp.util.IHasId;

/** Base interface for representing fields of parameters that are used by a specific expression.
 * This is used as a symbolic value in a {@link org.dbsp.sqlCompiler.compiler.visitors.inner.SymbolicInterpreter}
 * that detects fields in the parameters of a function which do not influence the function result. */
public abstract class IUsedFields implements ICastable, IHasId {
    public final long id;
    static long crtId = 0;

    protected IUsedFields() {
        this.id = crtId++;
    }

    @Override
    public long getId() {
        return this.id;
    }

    /** Convert the used fields into a {@link ParameterFieldUse} representation.
     * The current representation is a set of fields, whereas the result
     * is more like a bitmap for each parameter. */
    public abstract ParameterFieldUse getParameterUse();
}
