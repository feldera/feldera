package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.circuit.OutputPort;

import java.util.Objects;

/** Compare the predecessor and column copies that LeftJoinChains can follow from a port. */
public final class PortTransformOracle {
    private PortTransformOracle() {}

    public static boolean compare(OutputPort port, KeyAnalysis direct, KeyAnalysis hybrid) {
        var expected = direct.getPortSourceTransform(port);
        var actual = hybrid.getPortSourceTransform(port);
        if (expected == null && actual == null)
            return false;
        if (expected == null || actual == null || !expected.port().equals(actual.port()))
            throw new IllegalStateException("Different candidate-key predecessors at " + port);
        var shape = expected.port().getShape();
        if (shape != null) {
            for (var column : shape.columns()) {
                var left = expected.transform().copiesOf(column);
                var right = actual.transform().copiesOf(column);
                if (!Objects.equals(left, right))
                    throw new IllegalStateException("Different copies of " + column + " at " + port + ": " + left + " and " + right);
            }
        }
        return true;
    }
}
