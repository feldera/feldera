package org.dbsp.simulator;

import org.dbsp.simulator.util.TriFunction;

import java.util.function.Function;

public class AggregateDescription<Result, IntermediateResult, Data, Weight> {
    public final IntermediateResult initialValue;
    public final TriFunction<IntermediateResult, Data, Weight, IntermediateResult> update;
    public final Function<IntermediateResult, Result> finalize;

    public AggregateDescription(IntermediateResult initialValue,
                                TriFunction<IntermediateResult, Data, Weight, IntermediateResult> update,
                                Function<IntermediateResult, Result> finalize) {
        this.initialValue = initialValue;
        this.update = update;
        this.finalize = finalize;
    }
}
