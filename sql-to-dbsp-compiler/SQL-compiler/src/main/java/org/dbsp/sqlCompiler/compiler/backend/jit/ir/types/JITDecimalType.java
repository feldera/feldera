package org.dbsp.sqlCompiler.compiler.backend.jit.ir.types;

import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;

public class JITDecimalType extends JITScalarType {
    public static final JITDecimalType INSTANCE = new JITDecimalType();

    protected JITDecimalType() {
        super(DBSPTypeCode.DECIMAL);
    }
}
