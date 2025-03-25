
package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;

/** Move comparators from some operators to global declarations. */
public class ComparatorDeclarations extends CircuitCloneVisitor {
    final DeclareComparators declareComparators;

    public ComparatorDeclarations(DBSPCompiler compiler, DeclareComparators transform) {
        super(compiler, false);
        this.declareComparators = transform;
    }

    static class HasCustomOrd extends InnerVisitor {
        boolean found = false;

        public HasCustomOrd(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPTypeWithCustomOrd type) {
            this.found = true;
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            this.found = false;
            super.startVisit(node);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean hasOrd(DBSPUnaryOperator operator) {
        HasCustomOrd hasOrd = new HasCustomOrd(this.compiler);
        hasOrd.apply(operator.outputType());
        return hasOrd.found;
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        if (!this.hasOrd(operator)) {
            super.postorder(operator);
            return;
        }
        IDBSPInnerNode transformed = this.declareComparators.apply(operator.getFunction());
        DBSPExpression function = transformed.to(DBSPExpression.class);
        OutputPort input = this.mapped(operator.input());
        DBSPSimpleOperator repl = new DBSPMapIndexOperator(
                operator.getRelNode(), function, operator.getOutputIndexedZSetType(), input);
        this.map(operator, repl);
    }

    @Override
    public void postorder(DBSPFlatMapIndexOperator operator) {
        if (!this.hasOrd(operator)) {
            super.postorder(operator);
            return;
        }
        IDBSPInnerNode transformed = this.declareComparators.apply(operator.getFunction());
        DBSPExpression function = transformed.to(DBSPExpression.class);
        OutputPort input = this.mapped(operator.input());
        DBSPSimpleOperator repl = new DBSPFlatMapIndexOperator(
                operator.getRelNode(), function, operator.getOutputIndexedZSetType(), operator.isMultiset, input);
        this.map(operator, repl);
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        for (var decl: this.declareComparators.newDeclarations)
            this.getUnderConstructionCircuit().addDeclaration(new DBSPDeclaration(decl));
        super.postorder(circuit);
    }
}