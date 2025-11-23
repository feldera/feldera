package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.util.Utilities;

/** Trim unused fields from {@link org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator}. */
public class TrimWindows extends CircuitCloneWithGraphsVisitor {
    public TrimWindows(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        DBSPClosureExpression mapFunction = operator.getClosureFunction();
        Utilities.enforce(mapFunction.parameters.length == 1);
        DBSPParameter mapParam = mapFunction.parameters[0];
        if (source.node().is(DBSPWindowOperator.class) && inputFanout == 1) {
            DBSPWindowOperator window = source.node().to(DBSPWindowOperator.class);
            // We must preserve keys unchanged, but we can project away any fields of the value which are
            // unused on the left input of the window

            FindUnusedFields mapFinder = new FindUnusedFields(this.compiler);
            mapFunction = mapFinder.findUnusedFields(mapFunction);
            FieldUseMap mapUsed = mapFinder.parameterFieldMap.get(mapParam);
            mapUsed.setUsed(0);  // pretend all key fields are used
            if (mapUsed.hasUnusedFields(2)) {
                DBSPClosureExpression preProjection = mapUsed.getProjection(2);
                Utilities.enforce(preProjection != null);
                RewriteFields mapRewriter = mapFinder.getFieldRewriter(2);
                DBSPClosureExpression post = mapRewriter.rewriteClosure(mapFunction);

                DBSPMapIndexOperator pre = new DBSPMapIndexOperator(window.getRelNode(),
                        preProjection, window.left());
                this.addOperator(pre);

                DBSPWindowOperator newWindow = new DBSPWindowOperator(
                        window.getRelNode(), window.getFunctionNode(),
                        window.lowerInclusive, window.upperInclusive, pre.outputPort(), window.right());
                newWindow.setDerivedFrom(window);
                this.addOperator(newWindow);

                DBSPUnaryOperator postProj = new DBSPMapIndexOperator(
                        operator.getRelNode(), post, newWindow.outputPort());
                this.map(operator, postProj);
                return;
            }
        }
        super.postorder(operator);
    }
}
