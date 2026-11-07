package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.annotation.RegionAnnotation;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.IPrimitive;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPAccumulatorOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPChainAggregateKernelOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPIntegralOutputOperator;
import org.dbsp.sqlCompiler.circuit.operator.primitives.DBSPJoinKernelOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.HashString;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** Replaces each operator with a group of {@link IPrimitive} operators.
 * In a recursive circuit the primitives run on the clock of the nested circuit. */
public class LowerToPrimitives extends CircuitCloneVisitor {
    /** Maps a region tag to the number of regions created with that tag */
    final Map<String, Integer> regionCount = new HashMap<>();

    public LowerToPrimitives(DBSPCompiler compiler) {
        super(compiler, false);
    }

    RegionAnnotation newRegion(DBSPSimpleOperator operator) {
        int id = this.regionCount.merge(operator.operation, 1, Integer::sum) - 1;
        return new RegionAnnotation(operator.operation, id);
    }

    /** The persistent id of the stream {@code port}: the global hash of its operator, followed
     * by the output number when it is not 0.  Null if the operator has no hash. */
    @Nullable
    static String streamId(OutputPort port) {
        HashString hash = OperatorHash.getHash(port.node(), true);
        if (hash == null)
            return null;
        return port.port() == 0 ? hash.toString() : hash + "." + port.port();
    }

    /** {@code id} followed by {@code suffix}, or null if {@code id} is null. */
    @Nullable
    static String extend(@Nullable String id, String suffix) {
        return id == null ? null : id + suffix;
    }

    /** Give {@code child}, one of the primitives that replace {@code parent}, the region
     * annotations of {@code parent}, then {@code region}, and the persistent id {@code id}.
     * Each primitive gets the id that dbsp gives to the same node, so the persistent ids
     * are the same with or without the lowering. */
    <T extends DBSPOperator> T inheritAnnotations(
            DBSPSimpleOperator parent, RegionAnnotation region, T child, @Nullable String id) {
        for (RegionAnnotation outer : parent.annotations.get(RegionAnnotation.class))
            child.addAnnotation(outer, DBSPOperator.class);
        child.addAnnotation(region, DBSPOperator.class);
        if (id != null)
            child.addAnnotation(new OperatorHash(new HashString(id), true), DBSPOperator.class);
        return child;
    }

    /** The chain aggregate keeps the integral of its output on a back-edge:
     * the kernel reads the changes of the input and the output before the step. */
    @Override
    public void postorder(DBSPChainAggregateOperator operator) {
        OutputPort input = this.mapped(operator.input());
        if (input.kind() != StreamKind.DELTA) {
            super.postorder(operator);
            return;
        }
        CalciteRelNode node = operator.getRelNode();
        RegionAnnotation region = this.newRegion(operator);
        // dbsp names the integral of the output after the chain aggregate
        @Nullable String aggregate = streamId(operator.outputPort());
        @Nullable String integral = extend(aggregate, ".accintegral");
        var delta = this.inheritAnnotations(operator, region, new DBSPAccumulatorOperator(node, input),
                extend(streamId(input), ".accumulator"));
        this.addOperator(delta);
        var previous = this.inheritAnnotations(operator, region,
                new DBSPIntegralOutputOperator(node, operator.outputType), integral);
        this.addOperator(previous);
        var kernel = this.inheritAnnotations(operator, region,
                new DBSPChainAggregateKernelOperator(node, operator.init, operator.getClosureFunction(),
                        operator.outputType, delta.getOutput(1), previous.outputPort()),
                aggregate);
        this.addOperator(kernel);
        var output = this.inheritAnnotations(operator, region,
                new DBSPAccumulatorOperator(node, kernel.outputPort()),
                extend(streamId(kernel.outputPort()), ".accumulator"));
        this.addOperator(output);
        this.addOperator(this.inheritAnnotations(operator, region,
                new DBSPIntegralInputOperator(node, output.getOutput(0), previous), integral));
        this.map(operator.outputPort(), kernel.outputPort(), false);
    }

    /** The incremental join of two streams of changes: the changes of L join R are
     * dL join R + L' join dR, where R holds the changes of the step and L' does not.  Each side
     * has an accumulator, whose output 1 is a snapshot of the changes of the step, and an
     * integral of that accumulator.  Kernel {@code dl-r} joins the changes of the left side with
     * the right side after the step; kernel {@code l-dr} joins the left side before the step with
     * the changes of the right side.
     *
     * @param function The function of the kernels: the key, the left value, and the right value
     *                 to the output. */
    void lowerJoin(DBSPJoinBaseOperator operator, DBSPClosureExpression function) {
        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        // The joins are IIncremental, so both inputs are streams of changes
        Utilities.enforce(left.kind() == StreamKind.DELTA && right.kind() == StreamKind.DELTA,
                () -> "Join " + operator + " reads " + left.kind() + " and " + right.kind());
        CalciteRelNode node = operator.getRelNode();
        RegionAnnotation region = this.newRegion(operator);
        @Nullable String join = streamId(operator.outputPort());
        // dbsp names the integral of an input after the input stream
        String integral = operator.balanced ? ".balanced.integral" : ".shard_accintegral";
        var leftChanges = this.inheritAnnotations(operator, region, new DBSPAccumulatorOperator(node, left),
                extend(streamId(left), ".accumulator"));
        this.addOperator(leftChanges);
        var leftIntegral = this.inheritAnnotations(operator, region,
                new DBSPIntegralOperator(node, leftChanges.getOutput(0)), extend(streamId(left), integral));
        this.addOperator(leftIntegral);
        var rightChanges = this.inheritAnnotations(operator, region, new DBSPAccumulatorOperator(node, right),
                extend(streamId(right), ".accumulator"));
        this.addOperator(rightChanges);
        var rightIntegral = this.inheritAnnotations(operator, region,
                new DBSPIntegralOperator(node, rightChanges.getOutput(0)), extend(streamId(right), integral));
        this.addOperator(rightIntegral);
        var leftDelta = this.inheritAnnotations(operator, region,
                new DBSPJoinKernelOperator(node, function, operator.outputType,
                        leftChanges.getOutput(1), rightIntegral.getOutput(1)), extend(join, ".dl-r"));
        this.addOperator(leftDelta);
        var rightDelta = this.inheritAnnotations(operator, region,
                new DBSPJoinKernelOperator(node, function, operator.outputType,
                        leftIntegral.getOutput(0), rightChanges.getOutput(1)), extend(join, ".l-dr"));
        this.addOperator(rightDelta);
        // The output of the join keeps the id of the join
        var sum = this.inheritAnnotations(operator, region,
                new DBSPSumOperator(node, leftDelta.outputPort(), rightDelta.outputPort()), join);
        this.map(operator.outputPort(), sum.outputPort(), true);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        this.lowerJoin(operator, operator.getClosureFunction());
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        this.lowerJoin(operator, operator.getClosureFunction());
    }

    /** The kernel function returns the optional output row: the filter and the map fold into
     * the join function. */
    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        DBSPClosureExpression function = LowerCircuitVisitor.lowerJoinFilterMapFunctions(
                this.compiler(), operator.getClosureFunction(), operator.filter, operator.map);
        this.lowerJoin(operator, function);
    }
}
