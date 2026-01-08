package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CustomFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.util.Utilities;

/** Check that the function ConnectorMetadata is not invoked anywhere in the circuit.
 * (It is only allowed in the Default values for columns, which shows up in the metadata of input tables).*/
public class NoConnectorMetadata extends InnerVisitor {
    public NoConnectorMetadata(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        String name = expression.getFunctionName();
        if (name != null && name.equalsIgnoreCase(CustomFunctions.ConnectorMetadataFunction.NAME)) {
            throw new CompilationError("Calls to function " + Utilities.singleQuote(name) +
                    " are only allowed in DEFAULT column initializers",
                    expression.getNode());
        }
    }
}
