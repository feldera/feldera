package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/** A change stream is a sequence of InputOutputChange objects
 * that is used to test a streaming circuit.  Each InputOutputChange
 * corresponds to a circuit execution step.  This is not a real stream,
 * since it is always finite. */
public class InputOutputChangeStream {
    /** If non-empty it may be used to permute changes.
     * In this case input changes correspond to the tables. */
    public final List<String> inputTables;
    /** If non-empty it may be used to permute changes.
     * In this case outputs changes correspond to the views. */
    public final List<String> outputTables;
    public final List<IStreamCommand> commands;

    public InputOutputChangeStream(List<String> inputTables, List<String> outputTables) {
        this.commands = new ArrayList<>();
        this.inputTables = inputTables;
        this.outputTables = outputTables;
    }

    public InputOutputChangeStream() {
        this.commands = new ArrayList<>();
        this.inputTables = new ArrayList<>();
        this.outputTables = new ArrayList<>();
    }

    public InputOutputChangeStream addBlockForCompaction() {
        // We don't expect "block" to be the first command
        Utilities.enforce(! this.commands.isEmpty());
        this.commands.add(new BlockForCompaction());
        return this;
    }

    public InputOutputChangeStream addChange(InputOutputChange change) {
        Utilities.enforce(this.commands.isEmpty() || this.commands.get(0).compatible(change),
                () -> "Incompatible change");
        Utilities.enforce(this.inputTables.isEmpty() || change.inputs.getSetCount() == this.inputTables.size(),
                () -> "Change does not have the same number of input tables as specified");
        Utilities.enforce(this.outputTables.isEmpty() || change.outputs.getSetCount() == this.outputTables.size(),
                () -> "Change does not have the same number of output tables as specified");
        this.commands.add(change);
        return this;
    }

    public InputOutputChangeStream addPair(Change input, Change output) {
        this.addChange(new InputOutputChange(input, output));
        return this;
    }
}
