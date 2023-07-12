package org.dbsp.sqlCompiler.compiler.backend.jit.ir.types;

public class JITDecimalType extends JITScalarType {
    public static final JITDecimalType INSTANCE = new JITDecimalType();

    protected JITDecimalType() {
        super("Decimal");
    }
}
