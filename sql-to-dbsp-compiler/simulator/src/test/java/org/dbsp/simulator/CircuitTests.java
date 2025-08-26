package org.dbsp.simulator;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.operators.InputOperator;
import org.dbsp.simulator.operators.IntegrateOperator;
import org.dbsp.simulator.operators.OutputOperator;
import org.dbsp.simulator.operators.SelectOperator;
import org.dbsp.simulator.types.FunctionType;
import org.dbsp.simulator.types.IntegerSqlType;
import org.dbsp.simulator.types.IntegerWeightType;
import org.dbsp.simulator.types.TupleSqlType;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.types.ZSetType;
import org.dbsp.simulator.values.DynamicSqlValue;
import org.dbsp.simulator.values.IntegerSqlValue;
import org.dbsp.simulator.values.RuntimeFunction;
import org.dbsp.simulator.values.SqlTuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class CircuitTests {
    @Test
    public void circuit() {
        WeightType w = IntegerWeightType.INSTANCE;

        Circuit circuit = new Circuit();
        InputOperator input = new InputOperator("T", new ZSetType(new TupleSqlType(List.of(IntegerSqlType.INSTANCE)), w));
        circuit.addOperator(input);
        Function<DynamicSqlValue, DynamicSqlValue> func = x ->
                new SqlTuple(new IntegerSqlValue(
                        Objects.requireNonNull(x.to(SqlTuple.class).get(0)
                                .to(IntegerSqlValue.class).getValue()) + 1));
        SelectOperator select = new SelectOperator(new ZSetType(new TupleSqlType(IntegerSqlType.INSTANCE), w),
                new RuntimeFunction<>(new FunctionType(IntegerSqlType.INSTANCE, IntegerSqlType.INSTANCE), func),
                input.getOutput());
        circuit.addOperator(select);
        OutputOperator output = new OutputOperator("V", select.getOutput());
        circuit.addOperator(output);

        ZSet<DynamicSqlValue> in = new ZSet<>(IntegerWeightType.INSTANCE);
        in.append(new SqlTuple(new IntegerSqlValue(10)), new IntegerWeightType.IntegerWeight(1));
        input.setValue(in);

        circuit.step();
        BaseCollection result = output.getValue();
        Assert.assertEquals("""
                {
                    [11] => 1
                }""", result.toString());
    }

    @Test
    public void integratorTest() {
        WeightType w = IntegerWeightType.INSTANCE;

        Circuit circuit = new Circuit();
        InputOperator input = new InputOperator("T", new ZSetType(new TupleSqlType(List.of(IntegerSqlType.INSTANCE)), w));
        circuit.addOperator(input);
        IntegrateOperator select = new IntegrateOperator(input.getOutput());
        circuit.addOperator(select);
        OutputOperator output = new OutputOperator("V", select.getOutput());
        circuit.addOperator(output);

        ZSet<DynamicSqlValue> in = new ZSet<>(IntegerWeightType.INSTANCE);
        in.append(new SqlTuple(new IntegerSqlValue(10)), new IntegerWeightType.IntegerWeight(1));
        input.setValue(in);
        circuit.step();

        BaseCollection result = output.getValue();
        Assert.assertEquals("""
                {
                    [10] => 1
                }""", result.toString());

        in = new ZSet<>(IntegerWeightType.INSTANCE);
        in.append(new SqlTuple(new IntegerSqlValue(20)), new IntegerWeightType.IntegerWeight(2));
        input.setValue(in);
        circuit.step();

        result = output.getValue();
        Assert.assertEquals("""
                {
                    [10] => 1,
                    [20] => 2
                }""", result.toString());
    }
}
