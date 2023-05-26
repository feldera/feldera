package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;

public interface ICompilerComponent {
    DBSPCompiler getCompiler();
}
