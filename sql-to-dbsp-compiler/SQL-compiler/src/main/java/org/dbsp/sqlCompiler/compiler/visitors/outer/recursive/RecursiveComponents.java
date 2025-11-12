package org.dbsp.sqlCompiler.compiler.visitors.outer.recursive;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;

/** Handles recursive queries.  Multiple passes
 * - Compute SCCs
 * - Fix SCC by normalizing connections between some operations
 * - Group SCC nodes into {@link DBSPNestedOperator} operators
 * - Convert LeftJoin operators into joins + antijoins in recursive components
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
        this.add(new SubstituteLeftJoins(compiler));
    }
}
