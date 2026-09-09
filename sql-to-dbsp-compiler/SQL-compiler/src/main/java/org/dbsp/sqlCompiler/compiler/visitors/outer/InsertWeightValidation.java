package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeightValidatorOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.circuit.OutputPort;

/** Inserts a {@link DBSPWeightValidatorOperator} after every
 * {@link DBSPSourceMultisetOperator} (input tables without a primary key).
 * The validator checks the changes of a table, so the pass runs only on incremental circuits. */
public class InsertWeightValidation extends CircuitCloneVisitor {
    public InsertWeightValidation(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator node) {
        super.replace(node);
        OutputPort data = this.mapped(node.outputPort());
        DBSPWeightValidatorOperator validator = new DBSPWeightValidatorOperator(
                node.getRelNode(), data, "Table " + node.tableName.name());
        this.addOperator(validator);
        this.remap.put(node.outputPort(), validator.outputPort());
    }
}
