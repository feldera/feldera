package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;

/** Find patterns where the same collection is indexed twice on the same key with different values
 * (followed by an integral) and try to share the indexing by expanding the values.
 *
 * <pre>
 *        source
 *        /    \
 *    index   index
 *     /         \
 *  join        join
 * </pre>
 * <p>when the two index nodes have the same key, is rewritten as
 * <pre>
 *     source
 *        |
 *      index
 *     /    \
 *  join   join
 * </pre>
 * <p>where the common index produces the union of the fields of the two indexes.
 * The two joins need to have their functions adjusted to read the appropriate fields.
 * */
public class ShareIndexes extends Passes {
    public ShareIndexes(DBSPCompiler compiler) {
        super("ShareIndexes", compiler);
        // Detect patterns of the form
        //    source
        //     /  \
        //   map  mapIndex
        //   ...     ...
        // mapIndex  mapIndex
        //   |       |
        //  join    join
        // And collapse them to
        //    source
        //     /   \
        // mapIndex mapIndex
        //   |       |
        //  join    join
        var mapChains = new FindMapChains(compiler);
        this.add(mapChains);
        this.add(new CollapseSharedChains(compiler, mapChains.chains));
        this.add(new DeadCode(compiler, true));

        // Give each join its own copy of a MapIndex
        Graph graph1 = new Graph(compiler);
        this.add(graph1);
        this.add(new DuplicateSharedIndexes(compiler, graph1.getGraphs()));
        this.add(new DeadCode(compiler, true));

        // Share the same MapIndex between many joins
        Graph graph2 = new Graph(compiler);
        this.add(graph2);
        FindSharedIndexes shared = new FindSharedIndexes(compiler, graph2.getGraphs());
        this.add(shared);
        this.add(new ReplaceSharedIndexes(compiler, shared));
        this.add(new DeadCode(compiler, true));
    }
}
