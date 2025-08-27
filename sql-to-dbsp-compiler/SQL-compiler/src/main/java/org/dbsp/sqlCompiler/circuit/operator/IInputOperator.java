package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;

public interface IInputOperator {
    TableMetadata getMetadata();
    DBSPOperator asOperator();
    ProgramIdentifier getTableName();
    SourcePositionRange getSourcePosition();
    DBSPType getDataOutputType();
    DBSPTypeStruct getOriginalRowType();
}
