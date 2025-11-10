package org.dbsp.sqlCompiler.compiler.visitors.outer.recursive;

import org.dbsp.sqlCompiler.circuit.operator.DBSPApply2Operator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.Utilities;

/**
 * Check that all operators in recursive components are supported
 */
public class ValidateRecursiveOperators extends CircuitVisitor {
    public ValidateRecursiveOperators(DBSPCompiler compiler) {
        super(compiler);
    }

    boolean inRecursive() {
        return this.getParent().is(DBSPNestedOperator.class);
    }

    void reject(DBSPOperator operator, String operation, boolean bug) {
        if (!inRecursive())
            return;
        if (bug)
            throw new InternalCompilerError("Unsupported operation " + Utilities.singleQuote(operation) +
                    " in recursive code", operator.getNode());
        else
            throw new CompilationError("Unsupported operation " + Utilities.singleQuote(operation) +
                    " in recursive code", operator.getNode());
    }

    @Override
    public void postorder(DBSPApplyOperator node) {
        this.reject(node, "apply", true);
    }

    @Override
    public void postorder(DBSPApply2Operator node) {
        this.reject(node, "apply2", true);
    }

    @Override
    public void postorder(DBSPIndexedTopKOperator node) {
        this.reject(node, "TopK", false);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator node) {
        this.reject(node, "GC", true);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainValuesOperator node) {
        this.reject(node, "GC", true);
    }

    @Override
    public void postorder(DBSPLagOperator node) {
        this.reject(node, "LAG", false);
    }

    @Override
    public void postorder(DBSPNestedOperator node) {
        this.reject(node, "recursion", true);
    }

    @Override
    public void postorder(DBSPNowOperator node) {
        this.reject(node, "NOW", true);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator node) {
        this.reject(node, "OVER", false);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator node) {
        this.reject(node, "OVER", false);
    }

    @Override
    public void postorder(DBSPSourceBaseOperator node) {
        this.reject(node, "input", true);
    }

    @Override
    public void postorder(DBSPViewDeclarationOperator node) {
        // This is fine, all other SourceBaseOperators are not
    }

    @Override
    public void postorder(DBSPWaterlineOperator node) {
        this.reject(node, "WATERLINE", false);
    }

    @Override
    public void postorder(DBSPWindowOperator node) {
        this.reject(node, "WINDOW", false);
    }
}
