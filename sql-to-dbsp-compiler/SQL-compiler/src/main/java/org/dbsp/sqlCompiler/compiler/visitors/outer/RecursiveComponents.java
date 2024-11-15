package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
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
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
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
    public RecursiveComponents(IErrorReporter reporter) {
        super("Recursive", reporter);
        Graph graph = new Graph(reporter);
        this.add(graph);
        this.add(new FixupViewReferences(reporter, graph.getGraphs()));
        Graph graph2 = new Graph(reporter);
        this.add(graph2);
        this.add(new BuildNestedOperators(reporter, graph2.getGraphs()));
        this.add(new ValidateRecursiveOperators(reporter));
    }

    /** Check that all operators in recursive components are supported */
    static class ValidateRecursiveOperators extends CircuitVisitor {
        public ValidateRecursiveOperators(IErrorReporter errorReporter) {
            super(errorReporter);
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
                    if (simple.is(DBSPViewDeclarationOperator.class) && operators.size() == 1) {
                        DBSPViewDeclarationOperator decl = simple.to(DBSPViewDeclarationOperator.class);
                        this.errorReporter.reportWarning(simple.getSourcePosition(), "View is not recursive",
                                "View " + Utilities.singleQuote(decl.originalViewName()) + " is declared" +
                                " recursive, but is not used in any recursive computation");
                    }

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
                        if (operators.size() > 1 &&
                                !view.metadata.recursive &&
                                view.metadata.viewKind != SqlCreateView.ViewKind.LOCAL) {
                            List<DBSPOperator> recs =
                                    Linq.where(operators, o -> o.is(DBSPViewOperator.class) && o != view);
                            assert !recs.isEmpty();
                            throw new CompilationError(
                                    "View " + Utilities.singleQuote(view.viewName) + " must be declared" +
                                            " either as LOCAL or as RECURSIVE\n" +
                                    "since is is used in the computation of recursive view " +
                                    Utilities.singleQuote(recs.get(0).to(DBSPViewOperator.class).viewName),
                                    view.getNode());
                        }
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

    /** Encloses recursive components into separate {@link DBSPNestedOperator} operators. */
    static class BuildNestedOperators extends CircuitCloneWithGraphsVisitor {
        @Nullable
        SCC<DBSPOperator> scc = null;
        final Map<Integer, DBSPNestedOperator> components;
        final Set<DBSPNestedOperator> toAdd;

        BuildNestedOperators(IErrorReporter reporter, CircuitGraphs graphs) {
            super(reporter, graphs, false);
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
