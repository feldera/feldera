
package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.types.WeightType;

import javax.annotation.Nullable;
import java.util.Objects;

public abstract class BaseOperator<Weight> {
    final WeightType<Weight> weightType;
    final BaseOperator<Weight>[] inputs;
    @Nullable
    BaseCollection<Weight> nextOutput;

    @SafeVarargs
    protected BaseOperator(WeightType<Weight> weightType, BaseOperator<Weight>... inputs) {
        this.weightType = weightType;
        this.inputs = inputs;
        this.nextOutput = null;
    }

    /** Execute one computation step: gather data from the inputs,
     * and compute the current output. */
    public abstract void step();

    public BaseCollection<Weight> getOutput() {
        return Objects.requireNonNull(this.nextOutput);
    }

    public int getInputCount() {
        return this.inputs.length;
    }
}
