package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.Port;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Find operators that have multiple outputs that are all projections.
 * Compute for each such operator a projection that is the "largest", and use it
 * as a root of a tree of projections; that replacement is done in {@link ReplaceCommonProjections}. */
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
        if (operator.is(DBSPNestedOperator.class) ||
                successors.size() < 2 ||
                operator.is(DBSPConstantOperator.class)) {
            super.postorder(operator);
            return;
        }

        int depth = operator.outputType(0).is(DBSPTypeZSet.class) ? 1 : 2;
        boolean failed;
        List<FieldUseMap> usage = new ArrayList<>();
        List<DBSPUnaryOperator> readers = new ArrayList<>();
        List<ParameterFieldUse> uses = new ArrayList<>();
        for (Port<DBSPOperator> p: successors) {
            failed = !p.node().is(DBSPMapOperator.class) &&
                    !p.node().is(DBSPMapIndexOperator.class) &&
                    !p.node().is(DBSPDeindexOperator.class);
            if (!failed) {
                DBSPUnaryOperator unary = p.node().to(DBSPUnaryOperator.class);
                failed = !unary.getFunction().is(DBSPClosureExpression.class);
                DBSPClosureExpression closure = unary.getClosureFunction();
                if (!failed) {
                    FindUsedFields finder = new FindUsedFields(compiler);
                    var useMap = finder.findUsedFields(closure);
                    readers.add(unary);
                    uses.add(useMap);
                    failed = !useMap.hasUnusedFields(depth);
                    if (!failed)
                        usage.add(useMap.get(closure.parameters[0]));
                }
            }
            if (failed) {
                super.postorder(operator);
                return;
            }
        }

        FieldUseMap reduced = FieldUseMap.reduce(usage);
        if (!reduced.hasUnusedFields(depth)) {
            super.postorder(operator);
            return;
        }

        this.record(operator, readers, uses, depth, reduced);
    }

    private void record(DBSPOperator operator, List<DBSPUnaryOperator> readers, List<ParameterFieldUse> readerUses,
                        int depth, FieldUseMap reduced) {
        for (int i = 0; i < readers.size(); i++) {
            var reader = readers.get(i);
            var closure = reader.getClosureFunction();
            var uses = readerUses.get(i);
            uses.updateParameterValue(closure.parameters[0], reduced);
            var rewritten = uses.getFieldRewriter(this.compiler, depth).rewriteClosure(closure);
            Utilities.putNew(this.inputProjection, reader, rewritten);
        }
        DBSPClosureExpression proj = reduced.getProjection(depth);
        Utilities.putNew(this.outputProjection, operator, Objects.requireNonNull(proj));
        Utilities.putNew(this.originalOutputSize, operator, reduced.size());
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.clear();
        return super.startVisit(node);
    }

    public void clear() {
        this.outputProjection.clear();
        this.inputProjection.clear();
        this.originalOutputSize.clear();
    }
}
