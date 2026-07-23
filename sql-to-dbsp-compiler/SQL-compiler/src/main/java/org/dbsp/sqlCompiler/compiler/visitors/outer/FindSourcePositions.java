package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRanges;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import java.util.HashSet;
import java.util.Set;

/** Visitor which extracts source position information from the various properties of an operator */
public class FindSourcePositions extends InnerVisitor {
    public final Set<SourcePositionRange> positions;
    private final boolean reset;

    public FindSourcePositions(DBSPCompiler compiler, boolean reset) {
        super(compiler);
        this.positions = new HashSet<>();
        this.reset = reset;
    }

    @Override
    public void postorder(DBSPExpression expression) {
        SourcePositionRange positionRange = expression.getNode().getPositionRange();
        if (positionRange.isValid())
            this.positions.add(positionRange);
    }

    @Override
    public void postorder(DBSPParameter parameter) {
        SourcePositionRange positionRange = parameter.getNode().getPositionRange();
        if (positionRange.isValid())
            this.positions.add(positionRange);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        super.startVisit(node);
        if (this.reset)
            this.positions.clear();
    }

    public SourcePositionRanges getPositions() {
        return new SourcePositionRanges(this.positions);
    }

    /** Find the source positions associated with the specified operator */
    public static SourcePositionRanges getPositions(DBSPCompiler compiler, DBSPOperator operator) {
        FindSourcePositions positions = new FindSourcePositions(compiler, true);
        operator.accept(positions);
        positions.positions.addAll(operator.getSourcePositions());
        return positions.getPositions();
    }
}
