package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.Port;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Find operators that have multiple outputs that are all projections.
 * Compute for each such operator a projection that is the "largest", and use it
 * as a root of a tree of projections.
 */
public class FindCommonProjections extends CircuitWithGraphsVisitor {
    /** For each operator that has multiple projection successors, this is the "largest" projection */
    public final Map<DBSPOperator, DBSPClosureExpression> outputProjection;
    /** For each operator that follows an operator with multiple projections, this
     * is the new input projection to use. */
    public final Map<DBSPOperator, DBSPClosureExpression> inputProjection;
    public final Map<DBSPOperator, Integer> originalOutputSize;

    public FindCommonProjections(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs);
        this.outputProjection = new HashMap<>();
        this.inputProjection = new HashMap<>();
        this.originalOutputSize = new HashMap<>();
    }

    @Override
    public void postorder(DBSPOperator operator) {
        List<Port<DBSPOperator>> successors = this.getGraph().getSuccessors(operator);
        if (successors.size() < 2 || operator.is(DBSPConstantOperator.class)) {
            super.postorder(operator);
            return;
        }

        boolean failed;
        List<FieldUseMap> usage = new ArrayList<>();
        List<FindUnusedFields> finders = new ArrayList<>();
        for (Port<DBSPOperator> p: successors) {
            failed = !p.node().is(DBSPMapOperator.class) && !p.node().is(DBSPMapIndexOperator.class);
            if (!failed) {
                DBSPUnaryOperator unary = p.node().to(DBSPUnaryOperator.class);
                failed = !unary.getFunction().is(DBSPClosureExpression.class);
                DBSPClosureExpression closure = unary.getClosureFunction();
                if (!failed) {
                    FindUnusedFields finder = new FindUnusedFields(compiler);
                    finder.findUnusedFields(closure);
                    finders.add(finder);
                    failed = !finder.foundUnusedFields(2);
                    if (!failed)
                        usage.add(finder.get(closure.parameters[0]));
                }
            }
            if (failed) {
                super.postorder(operator);
                return;
            }
        }

        FieldUseMap reduced = FieldUseMap.reduce(usage);
        if (!reduced.hasUnusedFields(2)) {
            super.postorder(operator);
            return;
        }

        int index = 0;
        for (FindUnusedFields finder: finders) {
            // There should be just 1 parameter.
            DBSPParameter param = finder.parameterFieldMap.getParameters().iterator().next();
            finder.setParameterUseMap(param, reduced);
            RewriteFields rewriter = finder.getFieldRewriter(2);
            DBSPClosureExpression clo = rewriter.rewriteClosure(finder.getClosure());
            DBSPOperator op = successors.get(index++).node();
            Utilities.putNew(this.inputProjection, op, clo);
        }
        DBSPClosureExpression proj = reduced.getProjection(2);
        Utilities.putNew(this.outputProjection, operator, Objects.requireNonNull(proj));
        Utilities.putNew(this.originalOutputSize, operator, reduced.size());
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.outputProjection.clear();
        this.inputProjection.clear();
        this.originalOutputSize.clear();
        return super.startVisit(node);
    }
}
