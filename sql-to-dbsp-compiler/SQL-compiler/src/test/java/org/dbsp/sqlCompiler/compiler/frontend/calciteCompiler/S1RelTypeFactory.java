package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataTypeFactory;

/** Use the compiler's record-kind and nullability rules during relation import. */
public final class S1RelTypeFactory {
    private S1RelTypeFactory() { }
    public static RelDataTypeFactory create() { return new SqlToRelCompiler.CustomTypeFactory(); }
}
