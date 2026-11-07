package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;

/** The output half of an operator S that is split in two to break a cycle.
 * The output half has no inputs and produces the result of S.
 * The input half of S, an {@link ISplitInput}, receives the inputs of S and refers to this
 * output half with an implicit back edge.
 * In the circuit the output half comes before the input half.
 * {@link ISplitInput} shows the two halves in a diagram. */
public interface ISplitOutput {
    /** The port that produces the result of S. */
    OutputPort outputPort();
}
