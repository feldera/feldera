package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Merges the outputs of a recursive circuit that represent the same stream.
 * The merged output keeps one declaration. */
public class MergeRecursiveOutputs extends CircuitCloneVisitor {
    /** Key: a declaration that this pass removes; value: the declaration that replaces it. */
    final Map<DBSPViewDeclarationOperator, DBSPViewDeclarationOperator> replacement;
    /** Key: a recursive circuit with outputs to merge; value: for each of its outputs,
     * the index of the output that it merges into. */
    final Map<DBSPNestedOperator, List<Integer>> keptOutput;

    public MergeRecursiveOutputs(DBSPCompiler compiler) {
        super(compiler, false);
        this.replacement = new HashMap<>();
        this.keptOutput = new HashMap<>();
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.replacement.clear();
        this.keptOutput.clear();
        return super.startVisit(node);
    }

    /** For each output of {@code operator}, the index of the output that it merges into.
     * Of the outputs that are the same stream, the kept output is the one whose declaration
     * comes first, or the first output if none of them has a declaration. */
    private List<Integer> findKeptOutputs(DBSPNestedOperator operator) {
        // Key: an internal output; value: the index of the output kept for it
        Map<OutputPort, Integer> kept = new HashMap<>();
        for (DBSPOperator inner : operator.getAllOperators()) {
            DBSPViewDeclarationOperator declaration = inner.as(DBSPViewDeclarationOperator.class);
            if (declaration == null)
                continue;
            int index = operator.outputViews.indexOf(declaration.originalViewName());
            Utilities.enforce(index >= 0, () -> "Recursive view " + declaration.originalViewName().singleQuote() +
                    " is not an output of its recursive circuit");
            OutputPort port = operator.internalOutputs.get(index);
            if (port != null)
                kept.putIfAbsent(port, index);
        }
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < operator.outputCount(); i++) {
            OutputPort port = operator.internalOutputs.get(i);
            if (port == null) {
                result.add(i);
            } else {
                kept.putIfAbsent(port, i);
                result.add(kept.get(port));
            }
        }
        return result;
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator operator) {
        VisitDecision decision = super.preorder(operator);
        if (decision.stop())
            return decision;
        List<Integer> kept = this.findKeptOutputs(operator);
        boolean merge = false;
        for (int i = 0; i < kept.size(); i++) {
            int keptIndex = kept.get(i);
            if (keptIndex == i)
                continue;
            merge = true;
            DBSPViewDeclarationOperator declaration = operator.declarationByName.get(operator.outputViews.get(i));
            if (declaration != null) {
                // The kept output has a declaration, because a declaration chooses the kept output
                DBSPViewDeclarationOperator keptDeclaration = Utilities.getExists(
                        operator.declarationByName, operator.outputViews.get(keptIndex));
                Utilities.putNew(this.replacement, declaration, keptDeclaration);
            }
        }
        if (merge)
            Utilities.putNew(this.keptOutput, operator, kept);
        return decision;
    }

    @Override
    public void postorder(DBSPViewDeclarationOperator operator) {
        DBSPViewDeclarationOperator kept = this.replacement.get(operator);
        if (kept == null) {
            super.postorder(operator);
            return;
        }
        // The kept declaration comes first, so this visitor has already cloned it
        this.map(operator.outputPort(), this.mapped(kept.outputPort()), false);
    }

    @Override
    public void postorder(DBSPNestedOperator operator) {
        List<Integer> kept = this.keptOutput.get(operator);
        if (kept == null) {
            super.postorder(operator);
            return;
        }
        DBSPNestedOperator result = Utilities.removeLast(this.underConstruction).to(DBSPNestedOperator.class);
        result.copyAnnotations(operator);
        // Key: the index of a kept output of operator; value: the port of result that replaces it
        Map<Integer, OutputPort> resultPort = new HashMap<>();
        for (int i = 0; i < operator.outputCount(); i++) {
            if (kept.get(i) != i)
                continue;
            OutputPort internal = operator.internalOutputs.get(i);
            OutputPort newInternal = internal == null ? null : this.remap.get(internal);
            resultPort.put(i, result.addOutput(operator.outputViews.get(i), newInternal));
        }
        for (int i = 0; i < operator.outputCount(); i++)
            this.map(new OutputPort(operator, i), Utilities.getExists(resultPort, kept.get(i)), false);
        this.typecheckBackEdges(result);
        this.map(operator, result);
    }
}
