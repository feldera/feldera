package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CustomFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

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
