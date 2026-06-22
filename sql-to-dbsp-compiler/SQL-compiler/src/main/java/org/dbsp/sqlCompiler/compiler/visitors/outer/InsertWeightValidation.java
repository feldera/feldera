package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeightValidatorOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Inserts a {@link DBSPWeightValidatorOperator} after every
 * {@link DBSPSourceMultisetOperator} (input tables without a primary key). */
public class InsertWeightValidation extends CircuitCloneVisitor {
    public InsertWeightValidation(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator node) {
        super.replace(node);
        DBSPWeightValidatorOperator validator = new DBSPWeightValidatorOperator(
                node.getRelNode(), this.mapped(node.outputPort()), "Table " + node.tableName.name());
        this.remap.put(node.outputPort(), validator.outputPort());
        this.addOperator(validator);
    }
}
