package org.dbsp.util.graph;

import java.util.Objects;

/** A port is an edge representing a destination node and an input/output number */
public class Port<Node> {
    protected final Node node;
    protected final int port;

    public Port(Node node, int port) {
        this.node = node;
        this.port = port;
    }

    public Node node() {
        return node;
    }

    public int port() {
        return port;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Port) obj;
        return Objects.equals(this.node, that.node) &&
                this.port == that.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, port);
    }

    @Override
    public String toString() {
        return "Port[" +
                "node=" + node + ", " +
                "port=" + port + ']';
    }
}
