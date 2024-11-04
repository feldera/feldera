package org.dbsp.util.graph;

import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Strongly-connected components of a graph */
public class SCC<Node> {
    /** Count of strongly connected components */
    int count = 0;
    /** Visited nodes */
    final Set<Node> marked = new HashSet<>();
    /** Maps each node to the component id */
    public final Map<Node, Integer> componentId = new HashMap<>();
    /** The nodes in each component */
    public final Map<Integer, List<Node>> component = new HashMap<>();

    /** Compute the strongly connected components of a graph.
     *
     * @param reverseGraph  Reverse graph: edges are predecessors.
     * @param graph         Graph of successors. */
    public SCC(final DiGraph<Node> reverseGraph, final DiGraph<Node> graph) {
        DFSOrder<Node> dfs = new DFSOrder<>(reverseGraph);
        List<Node> post = Linq.list(dfs.reversePost());
        for (Node v : post) {
            if (!this.marked.contains(v)) {
                this.dfs(graph, v);
                this.count++;
            }
        }
    }

    void dfs(final DiGraph<Node> graph, Node v) {
        this.marked.add(v);
        Utilities.putNew(this.componentId, v, this.count);
        if (!this.component.containsKey(this.count))
            Utilities.putNew(this.component, this.count, new ArrayList<>());
        this.component.get(this.count).add(v);
        for (Port<Node> w : graph.getSuccessors(v)) {
            if (!this.marked.contains(w.node()))
                this.dfs(graph, w.node());
        }
    }
}
