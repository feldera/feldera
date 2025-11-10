package org.dbsp.sqlCompiler.compiler.visitors.outer.recursive;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Recursive;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
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

/**
 * Encloses recursive components into separate {@link DBSPNestedOperator} operators.
 */
class BuildNestedOperators extends CircuitCloneWithGraphsVisitor {
    @Nullable
    SCC<DBSPOperator> scc = null;
    final Map<Integer, DBSPNestedOperator> components;
    final Set<DBSPNestedOperator> toAdd;
    /**
     * Maps each view in an SCC to its output.
     */
    final Map<ProgramIdentifier, OutputPort> viewPort;
    /**
     * Maps each component and operator to the delta that contains its output
     */
    final Map<DBSPNestedOperator, Map<OutputPort, DBSPDeltaOperator>> deltasCreated;

    BuildNestedOperators(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
        this.components = new HashMap<>();
        this.toAdd = new HashSet<>();
        this.viewPort = new HashMap<>();
        this.deltasCreated = new HashMap<>();
    }

    @Override
    public void replace(DBSPSimpleOperator operator) {
        // Check if operator is in a larger connected component
        Utilities.enforce(this.scc != null);
        int myComponent = Utilities.getExists(this.scc.componentId, operator);
        List<DBSPOperator> component = Utilities.getExists(this.scc.component, myComponent);
        if (component.size() == 1) {
            if (operator.is(DBSPViewDeclarationOperator.class)) {
                DBSPViewDeclarationOperator decl = operator.to(DBSPViewDeclarationOperator.class);
                // If the corresponding view is materialized we don't produce a warning, since
                // the declaration is necessary.
                DBSPViewOperator view = this.getCircuit().getView(decl.originalViewName());
                if (view == null || view.metadata.viewKind != SqlCreateView.ViewKind.MATERIALIZED) {
                    this.compiler.reportWarning(operator.getSourcePosition(), "View is not recursive",
                            "View " + decl.originalViewName().singleQuote() + " is declared" +
                                    " recursive, but is not used in any recursive computation");
                }
            }
            super.replace(operator);
            return;
        }

        DBSPNestedOperator block;
        if (!this.components.containsKey(myComponent)) {
            block = new DBSPNestedOperator(operator.getRelNode());
            block.addAnnotation(Recursive.INSTANCE, DBSPNestedOperator.class);
            this.toAdd.add(block);
            Utilities.putNew(this.components, myComponent, block);
        } else {
            block = this.components.get(myComponent);
            if (operator.is(DBSPViewOperator.class) && this.toAdd.contains(block)) {
                // This ensures that the whole recursive block is inserted in topological order
                this.addOperator(block);
                this.toAdd.remove(block);
            }
        }

        List<OutputPort> sources = new ArrayList<>();
        for (OutputPort input : operator.inputs) {
            OutputPort source = this.mapped(input);
            int sourceComp = Utilities.getExists(this.scc.componentId, input.node());
            if (sourceComp != myComponent) {
                // Check if any inputs of the operator are in a different component
                // If they are, insert a delta + integrator operator in front.
                DBSPDeltaOperator delta = null;
                Map<OutputPort, DBSPDeltaOperator> deltas;
                if (this.deltasCreated.containsKey(block)) {
                    deltas = this.deltasCreated.get(block);
                    if (deltas.containsKey(source))
                        delta = deltas.get(source);
                } else {
                    deltas = Utilities.putNew(this.deltasCreated, block, new HashMap<>());
                }
                if (delta == null) {
                    delta = new DBSPDeltaOperator(operator.getRelNode(), source);
                    block.addOperator(delta);
                    Utilities.putNew(deltas, source, delta);
                }

                DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), delta.outputPort());
                block.addOperator(integral);
                sources.add(integral.outputPort());
            } else if (source.node().is(DBSPViewDeclarationOperator.class)) {
                // Insert an integrator after view declarations
                DBSPIntegrateOperator integral = new DBSPIntegrateOperator(operator.getRelNode(), source);
                block.addOperator(integral);
                sources.add(integral.outputPort());
            } else if (input.node().is(DBSPViewOperator.class)) {
                // Same component and input is a view.  Can't use 'source', because
                // that is mapped to the differentiator.
                DBSPViewOperator view = input.node().to(DBSPViewOperator.class);
                OutputPort port = Utilities.getExists(this.viewPort, view.viewName);
                sources.add(port);
            } else {
                sources.add(source);
            }
        }

        DBSPSimpleOperator result = operator.withInputs(sources, this.force)
                .to(DBSPSimpleOperator.class);
        result.setDerivedFrom(operator);
        block.addOperator(result);
        DBSPViewOperator view = result.as(DBSPViewOperator.class);
        OutputPort port = result.outputPort();
        if (view != null) {
            // Insert a differentiator after each view
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(operator.getRelNode(), view.outputPort());
            block.addOperator(diff);
            // Map the output to the NestedOperator, and not the source view
            port = block.addOutput(view.viewName, diff.outputPort());
            Utilities.putNew(this.viewPort, view.viewName, view.outputPort());
        }
        this.map(operator.outputPort(), port, false);
    }

    // If an operator has a differentiator input, that input belongs logically to the
    // same SCC as the operator itself.  Make this adjustment.
    void adjustDifferentiators(DBSPCircuit circuit) {
        Utilities.enforce(this.scc != null);
        for (DBSPOperator operator : circuit.getAllOperators()) {
            int scc = this.scc.componentId.get(operator);
            // Only do this if the operator's SCC is non-trivial
            if (this.scc.component.get(scc).size() == 1) continue;
            for (OutputPort inputPort : operator.inputs) {
                DBSPOperator input = inputPort.node();
                if (input.is(DBSPDifferentiateOperator.class)) {
                    int inputScc = this.scc.componentId.get(input);
                    if (scc != inputScc) {
                        this.scc.move(input, scc);
                    }
                }
            }
        }
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        super.preorder(circuit);
        CircuitGraph graph = this.graphs.getGraph(circuit);
        this.scc = new SCC<>(circuit, graph);
        this.adjustDifferentiators(circuit);
        Logger.INSTANCE.belowLevel(this, 2)
                .appendSupplier(() -> this.scc.toString());
        return VisitDecision.CONTINUE;
    }
}
