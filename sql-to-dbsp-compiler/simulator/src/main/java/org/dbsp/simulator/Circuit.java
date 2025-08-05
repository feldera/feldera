package org.dbsp.simulator;

import com.fasterxml.jackson.databind.ser.Serializers;
import org.dbsp.simulator.operators.BaseOperator;
import org.dbsp.simulator.operators.InputOperator;
import org.dbsp.simulator.operators.OutputOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Circuit {
    //  Topological sort
    final List<BaseOperator> operators;
    final List<InputOperator> inputs;
    final List<OutputOperator> outputs;
    final Set<BaseOperator> members;

    public Circuit() {
        this.operators = new ArrayList<>();
        this.members = new HashSet<>();
        this.inputs = new ArrayList<>();
        this.outputs = new ArrayList<>();
    }

    public void addOperator(BaseOperator operator) {
        assert(!this.members.contains(operator));
        this.operators.add(operator);
        this.members.add(operator);
        if (operator.is(InputOperator.class))
            this.inputs.add(operator.to(InputOperator.class));
        if (operator.is(OutputOperator.class))
            this.outputs.add(operator.to(OutputOperator.class));
    }

    public void step() {
        for (BaseOperator op: this.operators) {
            op.step();
        }
    }

    public void reset() {
        for (BaseOperator op: this.operators) {
            op.reset();
        }
    }
}
