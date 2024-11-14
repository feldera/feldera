package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.Recursive;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.OutputPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.SCC;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Handles recursive queries boxing them into NestedOperator operators */
public class RecursiveComponents extends Passes {
    public RecursiveComponents(IErrorReporter reporter) {
        super("Recursive", reporter);
        Graph graph = new Graph(reporter);
        this.add(graph);
        this.add(new FixupViewReferences(reporter, graph.getGraphs()));
        Graph graph2 = new Graph(reporter);
        this.add(graph2);
        this.add(new BuildNestedOperators(reporter, graph2.getGraphs()));
    }

    /** In the circuit recursive views can be referred either by their declaration,
     * or by the view itself.  Normalize the representation such that
     * - references across SCCs go to the view
     * - references in the same SCC go to the declaration */
    static class FixupViewReferences extends CircuitCloneVisitor {
        final CircuitGraphs graphs;
        @Nullable
        SCC<DBSPOperator> scc = null;

        FixupViewReferences(IErrorReporter reporter, CircuitGraphs graphs) {
            super(reporter, false);
            this.graphs = graphs;
        }

        @Override
        public VisitDecision preorder(DBSPPartialCircuit circuit) {
            super.preorder(circuit);
            CircuitGraph graph = this.graphs.getGraph(circuit);
            this.scc = new SCC<>(circuit, graph);
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> this.scc.toString());
            for (DBSPDeclaration node : circuit.declarations)
                node.accept(this);
            // Do the nodes in the order of SCCs
            assert this.scc != null;
            Map<String, DBSPViewOperator> viewByName = new HashMap<>();
            Map<String, DBSPViewDeclarationOperator> declByName = new HashMap<>();
            for (int i = 0; i < this.scc.count; i++) {
                // SCCs are in reverse topological order
                int sccId = this.scc.count - i - 1;
                // Operators in the circuit in circuit order from the current SCC
                List<DBSPOperator> operators = Linq.where(
                        circuit.allOperators, o -> this.scc.componentId.get(o) == sccId);
                for (DBSPOperator operator: operators) {
                    // We only have simple operators at this stage
                    DBSPSimpleOperator simple = operator.to(DBSPSimpleOperator.class);
                    List<OutputPort> newSources = new ArrayList<>(simple.inputs.size());
                    for (OutputPort port: simple.inputs) {
                        DBSPSimpleOperator source = port.simpleNode();
                        int sourceScc = Utilities.getExists(this.scc.componentId, source);
                        assert sourceScc >= sccId;
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
                                var decl = Utilities.getExists(declByName, view.viewName);
                                newPort = decl.outputPort();
                            }
                        }
                        newSources.add(newPort);
                    }
                    DBSPSimpleOperator result = simple.withInputs(newSources, false);
                    if (result.is(DBSPViewOperator.class)) {
                        DBSPViewOperator view = result.to(DBSPViewOperator.class);
                        Utilities.putNew(viewByName, view.viewName, view);
                    }
                    if (result.is(DBSPViewDeclarationOperator.class)) {
                        DBSPViewDeclarationOperator view = result.to(DBSPViewDeclarationOperator.class);
                        Utilities.putNew(declByName, view.originalViewName(), view);
                    }
                    result.setDerivedFrom(operator.id);
                    this.map(simple, result, true);
                }
            }

            // This is normally done in postorder(DBSPPartialCircuit), but postorder is not executed.
            DBSPPartialCircuit result = Utilities.removeLast(this.underConstruction).to(DBSPPartialCircuit.class);
            if (result.sameCircuit(circuit))
                result = circuit;
            this.map(circuit, result);
            return VisitDecision.STOP;
        }
    }

    /** Encloses recursive components into separate NestedOperator operators,
     * and does other circuit normalization operations. */
    static class BuildNestedOperators extends CircuitCloneVisitor {
        final CircuitGraphs graphs;
        @Nullable
        SCC<DBSPOperator> scc = null;
        final Map<Integer, DBSPNestedOperator> components;
        final Set<DBSPNestedOperator> toAdd;

        public CircuitGraph getGraph() {
            return this.graphs.getGraph(this.getParent());
        }

        BuildNestedOperators(IErrorReporter reporter, CircuitGraphs graphs) {
            super(reporter, false);
            this.graphs = graphs;
            this.components = new HashMap<>();
            this.toAdd = new HashSet<>();
        }

        @Override
        public void replace(DBSPSimpleOperator operator) {
            // Check if operator is in a larger connected component
            assert this.scc != null;
            int myComponent = Utilities.getExists(this.scc.componentId, operator);
            List<DBSPOperator> component = Utilities.getExists(this.scc.component, myComponent);
            if (component.size() == 1) {
                super.replace(operator);
                return;
            }

            DBSPNestedOperator block;
            if (!this.components.containsKey(myComponent)) {
                block = new DBSPNestedOperator(operator.getNode());
                block.addAnnotation(new Recursive());
                this.toAdd.add(block);
                Utilities.putNew(this.components, myComponent, block);
            } else {
                block = this.components.get(myComponent);
                if (operator.is(DBSPViewOperator.class) && this.toAdd.contains(block)) {
                    // This ensures that the whole recursive circuit is inserted in topological order
                    this.addOperator(block);
                    this.toAdd.remove(block);
                }
            }

            // Check if any inputs of the operator are in a different component
            // If they are, insert a delta + integrator operator in front.
            List<OutputPort> sources = new ArrayList<>();
            for (OutputPort input : operator.inputs) {
                OutputPort source = this.mapped(input);
                int sourceComp = Utilities.getExists(this.scc.componentId, input.node());
                if (sourceComp != myComponent) {
                    DBSPDeltaOperator delta = new DBSPDeltaOperator(operator.getNode(), source);
                    block.addOperator(delta);
                    DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getNode(), delta.outputPort());
                    block.addOperator(integral);
                    sources.add(integral.outputPort());
                } else if (source.node().is(DBSPViewDeclarationOperator.class)) {
                    // Insert an integrator after view declarations
                    DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getNode(), source);
                    block.addOperator(integral);
                    sources.add(integral.outputPort());
                } else {
                    sources.add(source);
                }
            }

            DBSPSimpleOperator result = operator.withInputs(sources, this.force);
            result.setDerivedFrom(operator.id);
            block.addOperator(result);
            DBSPViewOperator view = result.as(DBSPViewOperator.class);
            OutputPort port = result.outputPort();
            if (view != null && view.metadata.recursive) {
                // Insert a differentiator after the views.
                // All their outputs are in different SCCs from the previous pass
                DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(operator.getNode(), view.outputPort());
                block.addOperator(diff);
                // Use as port the output of the NestedOperator, and not the source view
                port = block.addOutput(diff.outputPort());
            }
            this.map(operator.outputPort(), port, false);
        }

        @Override
        public VisitDecision preorder(DBSPPartialCircuit circuit) {
            super.preorder(circuit);
            CircuitGraph graph = this.graphs.getGraph(circuit);
            this.scc = new SCC<>(circuit, graph);
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> this.scc.toString());
            return VisitDecision.CONTINUE;
        }
    }
}
