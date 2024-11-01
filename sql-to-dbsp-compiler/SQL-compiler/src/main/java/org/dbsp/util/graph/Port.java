package org.dbsp.util.graph;

/**
 * A port is an edge representing a destination node and an input/output number
 */
public class Port<Node> {
    public final Node node;
    public final int port;

    public Port(Node node, int port) {
        this.node = node;
        this.port = port;
    }
}
