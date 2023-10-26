package org.dbsp.sqlCompiler.compiler.backend.jit.ir.types;

import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;

public class JITTimeType extends JITScalarType {
    public static final JITTimeType INSTANCE = new JITTimeType();

    protected JITTimeType() {
        super(DBSPTypeCode.TIME);
    }
}
