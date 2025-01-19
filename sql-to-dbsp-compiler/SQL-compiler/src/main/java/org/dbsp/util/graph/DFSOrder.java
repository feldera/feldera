package org.dbsp.util.graph;

import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Computes depth-first order of a graph */
public class DFSOrder<Node> {
    final Set<Node> marked;
    final Map<Node, Integer> post;
    final List<Node> preorder;
    final List<Node> postorder;
    int postCounter;

    public DFSOrder(DiGraph<Node> graph) {
        this.post = new HashMap<>();
        this.postorder = new ArrayList<>();
        this.preorder = new ArrayList<>();
        this.marked = new HashSet<>();
        for (Node v: graph.getNodes())
            if (!this.marked.contains(v))
                this.dfs(graph, v);
    }

    // run DFS in digraph G from vertex v and compute preorder/postorder
    private void dfs(DiGraph<Node> graph, Node v) {
        this.marked.add(v);
        this.preorder.add(v);
        for (Port<Node> p : graph.getSuccessors(v)) {
            Node w = p.node();
            if (!this.marked.contains(w)) {
                this.dfs(graph, w);
            }
        }
        this.postorder.add(v);
        Utilities.putNew(this.post, v, this.postCounter++);
    }

    /** Reverse postorder of the graph.  Destroys the postorder list. */
    public Iterable<Node> reversePost() {
        Collections.reverse(this.postorder);
        return this.postorder;
    }
}
