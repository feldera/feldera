package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.Logger;

/** Remove a specified input table */
public class RemoveTable extends CircuitCloneVisitor {
    final ProgramIdentifier tableName;

    public RemoveTable(DBSPCompiler compiler, ProgramIdentifier tableName) {
        super(compiler, false);
        this.tableName = tableName;
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator input) {
        if (this.tableName.equals(input.tableName)) {
            // Return without adding it to the circuit.
            Logger.INSTANCE.belowLevel(this, 1)
                    .append("Removing table ")
                    .appendSupplier(this.tableName::name)
                    .newline();
            this.compiler.compiler().removeTable(this.tableName);
            return;
        }
        super.postorder(input);
    }

    @Override
    public String toString() {
        return super.toString() + "-" + this.tableName;
    }
}
