package org.dbsp.sqlCompiler.circuit.operator;

/** The input half of an operator S that is split in two to break a cycle.
 * The input half receives the inputs of S and has no consumers.
 * The output half of S, an {@link ISplitOutput}, has no inputs and produces the result of S.
 * In the circuit the output half comes before the input half.
 *
 * <p>The cycle y = S(F(y)) after the split; the operators are in circuit order,
 * from top to bottom:
 * <pre>{@code
 *      .......> [ S output half ]    ISplitOutput: no inputs
 *      :               |
 *      :               | y
 *      :               v
 *      :             [ F ] --------> other consumers
 *      :               |
 *      :               | x
 *      :               v
 *      :....... [ S input half ]     ISplitInput: no consumers
 * }</pre>
 * The dotted line is the implicit back edge:
 * the input half refers to its output half with {@link #splitOutput()}.
 * The back edge is not a stream, so the circuit stays acyclic.
 * S always contains (or is) a delay: the value that the output half produces at a 
 * step depends only on the values that the input half received in earlier steps. */
public interface ISplitInput {
    /** The output half of S: the end of the implicit back edge. */
    ISplitOutput splitOutput();

    /** A copy of this input half whose output half is 'output'. */
    DBSPSimpleOperator withSplitOutput(ISplitOutput output);
}
