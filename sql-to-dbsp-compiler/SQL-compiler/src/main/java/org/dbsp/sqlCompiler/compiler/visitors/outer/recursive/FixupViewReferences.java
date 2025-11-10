package org.dbsp.sqlCompiler.compiler.visitors.outer.recursive;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.SCC;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In the circuit recursive views can be referred either by their declaration,
 * or by the view itself.  Normalize the representation such that
 * - references across SCCs go to the view
 * - references in the same SCC go to the declaration
 */
class FixupViewReferences extends CircuitCloneVisitor {
    final CircuitGraphs graphs;
    @Nullable
    SCC<DBSPOperator> scc = null;

    FixupViewReferences(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, false);
        this.graphs = graphs;
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        super.preorder(circuit);
        CircuitGraph graph = this.graphs.getGraph(circuit);
        this.scc = new SCC<>(circuit, graph);
        Logger.INSTANCE.belowLevel(this, 2)
                .appendSupplier(() -> this.scc.toString());
        for (DBSPDeclaration node : circuit.declarations)
            node.accept(this);
        // Do the nodes in the order of SCCs
        Utilities.enforce(this.scc != null);
        Map<ProgramIdentifier, DBSPViewOperator> viewByName = new HashMap<>();
        Map<ProgramIdentifier, DBSPViewDeclarationOperator> declByName = new HashMap<>();
        for (int i = 0; i < this.scc.count; i++) {
            // SCCs are in reverse topological order
            int sccId = this.scc.count - i - 1;
            // Operators in the circuit in circuit order from the current SCC
            List<DBSPOperator> operators = Linq.where(
                    circuit.allOperators, o -> this.scc.componentId.get(o) == sccId);
            for (DBSPOperator operator : operators) {
                // We only have simple operators at this stage
                DBSPSimpleOperator simple = operator.to(DBSPSimpleOperator.class);
                List<OutputPort> newSources = new ArrayList<>(simple.inputs.size());
                for (OutputPort port : simple.inputs) {
                    DBSPSimpleOperator source = port.simpleNode();
                    int sourceScc = Utilities.getExists(this.scc.componentId, source);
                    Utilities.enforce(sourceScc >= sccId);
                    OutputPort newPort = this.mapped(port);
                    if (source.is(DBSPViewDeclarationOperator.class)) {
                        DBSPViewDeclarationOperator decl = source.to(DBSPViewDeclarationOperator.class);
                        if (sourceScc != sccId) {
                            DBSPViewOperator view = Utilities.getExists(viewByName, decl.originalViewName());
                            newPort = view.outputPort();
                        }
                    } else if (source.is(DBSPViewOperator.class)) {
                        var view = source.to(DBSPViewOperator.class);
                        if (view.metadata.recursive && sourceScc == sccId) {
                            var decl = declByName.get(view.viewName);
                            if (decl != null) {
                                // decl can be null if the view is declared recursive, but in fact it is not
                                // used recursively in an SCC.  The view may still need to be declared recursive
                                // because the user may want to e.g., materialize it.
                                newPort = decl.outputPort();
                            } else {
                                newPort = this.mapped(view.outputPort());
                            }
                        }
                    }
                    newSources.add(newPort);
                }
                DBSPSimpleOperator result = simple
                        .withInputs(newSources, false)
                        .to(DBSPSimpleOperator.class);
                if (result.is(DBSPViewOperator.class)) {
                    DBSPViewOperator view = result.to(DBSPViewOperator.class);
                    if (operators.size() > 1 &&
                            !view.metadata.recursive &&
                            view.metadata.viewKind != SqlCreateView.ViewKind.LOCAL) {
                        List<DBSPOperator> recs =
                                Linq.where(operators, o -> o.is(DBSPViewOperator.class) && o != view);
                        Utilities.enforce(!recs.isEmpty());
                        throw new CompilationError(
                                "View " + view.viewName.singleQuote() + " must be declared" +
                                        " either as LOCAL or as RECURSIVE\n" +
                                        "since is is used in the computation of recursive view " +
                                        recs.get(0).to(DBSPViewOperator.class).viewName.singleQuote(),
                                view.getNode());
                    }
                    Utilities.putNew(viewByName, view.viewName, view);
                } else if (result.is(DBSPViewDeclarationOperator.class)) {
                    DBSPViewDeclarationOperator view = result.to(DBSPViewDeclarationOperator.class);
                    Utilities.putNew(declByName, view.originalViewName(), view);
                }
                result.setDerivedFrom(operator);
                this.map(simple, result, true);
            }
        }

        // This is normally done in postorder(DBSPPartialCircuit), but postorder is not executed.
        DBSPCircuit result = Utilities.removeLast(this.underConstruction).to(DBSPCircuit.class);
        if (result.sameCircuit(circuit))
            result = circuit;
        this.map(circuit, result);
        return VisitDecision.STOP;
    }
}
