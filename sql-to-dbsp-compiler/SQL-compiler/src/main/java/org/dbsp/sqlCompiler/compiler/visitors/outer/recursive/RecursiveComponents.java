package org.dbsp.sqlCompiler.compiler.visitors.outer.recursive;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;

import java.util.function.UnaryOperator;

/** Handles recursive queries.  Multiple passes
 * - Compute SCCs
 * - Fix SCC by normalizing connections between some operations
 * - Group SCC nodes into {@link DBSPNestedOperator} operators
 * - Convert LeftJoin operators into joins + antijoins in recursive components
 * - Validate contents of nested operators. */
public class RecursiveComponents extends Passes {
    public RecursiveComponents(DBSPCompiler compiler) {
        this(compiler, UnaryOperator.identity());
    }

    public RecursiveComponents(DBSPCompiler compiler, UnaryOperator<CircuitTransform> select) {
        super("RecursiveComponents", compiler);
        Graph graph = new Graph(compiler);
        this.add(select.apply(graph));
        this.add(select.apply(new FixupViewReferences(compiler, graph.getGraphs())));
        Graph graph2 = new Graph(compiler);
        this.add(select.apply(graph2));
        this.add(select.apply(new BuildNestedOperators(compiler, graph2.getGraphs())));
        this.add(select.apply(new ValidateRecursiveOperators(compiler)));
        this.add(select.apply(new SubstituteLeftJoins(compiler)));
    }
}
