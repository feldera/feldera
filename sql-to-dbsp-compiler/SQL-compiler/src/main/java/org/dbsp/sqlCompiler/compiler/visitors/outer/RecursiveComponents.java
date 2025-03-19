package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.Recursive;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApply2Operator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
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

/** Handles recursive queries.  Multiple passes
 * - Compute SCCs
 * - Fix SCC by normalizing connections between some operations
 * - Group SCC nodes into {@link DBSPNestedOperator} operators
 * - Validate contents of nested operators. */
public class RecursiveComponents extends Passes {
    public RecursiveComponents(DBSPCompiler compiler) {
        super("Recursive", compiler);
        Graph graph = new Graph(compiler);
        this.add(graph);
        this.add(new FixupViewReferences(compiler, graph.getGraphs()));
        Graph graph2 = new Graph(compiler);
        this.add(graph2);
        this.add(new BuildNestedOperators(compiler, graph2.getGraphs()));
        this.add(new ValidateRecursiveOperators(compiler));
        //this.add(new ShowCircuit(compiler));
        //this.add(new Graph(compiler));
    }

    /** Check that all operators in recursive components are supported */
    static class ValidateRecursiveOperators extends CircuitVisitor {
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

    /** In the circuit recursive views can be referred either by their declaration,
     * or by the view itself.  Normalize the representation such that
     * - references across SCCs go to the view
     * - references in the same SCC go to the declaration */
    static class FixupViewReferences extends CircuitCloneVisitor {
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
            assert this.scc != null;
            Map<ProgramIdentifier, DBSPViewOperator> viewByName = new HashMap<>();
            Map<ProgramIdentifier, DBSPViewDeclarationOperator> declByName = new HashMap<>();
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
                    DBSPSimpleOperator result = simple.withInputs(newSources, false);
                    if (result.is(DBSPViewOperator.class)) {
                        DBSPViewOperator view = result.to(DBSPViewOperator.class);
                        if (operators.size() > 1 &&
                                !view.metadata.recursive &&
                                view.metadata.viewKind != SqlCreateView.ViewKind.LOCAL) {
                            List<DBSPOperator> recs =
                                    Linq.where(operators, o -> o.is(DBSPViewOperator.class) && o != view);
                            assert !recs.isEmpty();
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
                    result.setDerivedFrom(operator.derivedFrom);
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

    /** Encloses recursive components into separate {@link DBSPNestedOperator} operators. */
    static class BuildNestedOperators extends CircuitCloneWithGraphsVisitor {
        @Nullable
        SCC<DBSPOperator> scc = null;
        final Map<Integer, DBSPNestedOperator> components;
        final Set<DBSPNestedOperator> toAdd;
        /** Maps each view in an SCC to its output. */
        final Map<ProgramIdentifier, OutputPort> viewPort;
        /** Maps each component and operator to the delta that contains its output */
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
            assert this.scc != null;
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

            DBSPSimpleOperator result = operator.withInputs(sources, this.force);
            result.setDerivedFrom(operator.derivedFrom);
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

        @Override
        public VisitDecision preorder(DBSPCircuit circuit) {
            super.preorder(circuit);
            CircuitGraph graph = this.graphs.getGraph(circuit);
            this.scc = new SCC<>(circuit, graph);
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(() -> this.scc.toString());
            return VisitDecision.CONTINUE;
        }
    }
}
