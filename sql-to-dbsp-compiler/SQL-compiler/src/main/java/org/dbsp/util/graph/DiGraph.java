package org.dbsp.util.graph;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/** Abstract representation of a graph. */
public interface DiGraph<Node> {
    /** Returns an iterator over the graph nodes */
    Iterable<Node> getNodes();

    /** Returns the successors of a node */
    List<Port<Node>> getSuccessors(Node node);

    /** The node closest downstream of {@code start} that satisfies {@code test}, searching
     * breadth first; null if none is reachable.  {@code start} is tested only when
     * a cycle leads back to it. */
    @Nullable
    default Node closestSuccessor(Node start, Predicate<Node> test) {
        Set<Node> visited = new HashSet<>();
        Deque<Node> queue = new ArrayDeque<>();
        for (Port<Node> port : this.getSuccessors(start))
            queue.add(port.node());
        while (!queue.isEmpty()) {
            Node current = queue.remove();
            if (!visited.add(current))
                continue;
            if (test.test(current))
                return current;
            for (Port<Node> port : this.getSuccessors(current))
                queue.add(port.node());
        }
        return null;
    }

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
