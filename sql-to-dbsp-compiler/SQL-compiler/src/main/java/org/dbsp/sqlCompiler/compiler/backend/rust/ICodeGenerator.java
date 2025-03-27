package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.ICastable;
import org.dbsp.util.IIndentStream;

import java.io.IOException;

/** Interface implemented by classes which write Rust code */
public interface ICodeGenerator extends ICastable {
    /** Set the stream where the output is generated */
    void setOutputBuilder(IIndentStream stream);
    /** The specified crate is a dependency */
    void addDependency(String crate);
    /** The specified node will produce code */
    void add(IDBSPNode node);
    /** Generate code for the nodes added in the current output stream */
    void write(DBSPCompiler compiler) throws IOException;
}
