package org.dbsp.util.graph;

import java.util.List;

/** Abstract representation of a graph. */
public interface DiGraph<Node> {
    /** Returns an iterator over the graph nodes */
    Iterable<Node> getNodes();

    /** Returns the successors of a node */
    List<Port<Node>> getSuccessors(Node node);

    default int getFanout(Node node) {
        return this.getSuccessors(node).size();
    }

    default String asString() {
        StringBuilder builder = new StringBuilder();
        for (Node node: this.getNodes()) {
            for (Port<Node> port: this.getSuccessors(node)) {
                builder.append(node)
                        .append(" -> ")
                        .append(port.node())
                        .append(System.lineSeparator());
            }
        }
        return builder.toString();
    }
}
