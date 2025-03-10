package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.IDBSPNode;

import java.io.IOException;
import java.io.PrintStream;

public interface ICodeGenerator {
    void setPrintStream(PrintStream stream);
    void addDependency(String crate);
    void add(IDBSPNode node);
    void write(DBSPCompiler compiler) throws IOException;
}
