package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.IJoin;
import org.dbsp.sqlCompiler.circuit.operator.ILinear;
import org.dbsp.sqlCompiler.circuit.operator.ILinearAggregate;
import org.dbsp.sqlCompiler.circuit.operator.INonIncremental;
import org.dbsp.sqlCompiler.circuit.operator.INonLinearAggregate;
import org.dbsp.sqlCompiler.circuit.operator.IStateful;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Compute some simple circuit statistics.  To display the statistics the compiler can be invoked
 * with the following option: "-TCircuitStatistics=1" */
public class CircuitStatistics extends CircuitVisitor {
    static class Statistics {
        public int totalOperators;
        public int tables;
        public int views;
        public int joins;
        public int linear;
        public int linearAggregates;
        public int nonLinearAggregates;
        public int windows;
        public int gcOperators;
        public int nonIncremental;
        public int nested;
        public int maxTupleWidth;
        public int maxDepth;

        @Override
        public String toString() {
            return "{" +
                    "\n  \"totalOperators\": " + totalOperators +
                    ",\n  \"gcOperators\": " + gcOperators +
                    ",\n  \"tables\": " + tables +
                    ",\n  \"views\": " + views +
                    ",\n  \"joins\": " + joins +
                    ",\n  \"linear\": " + linear +
                    ",\n  \"linearAggregates\": " + linearAggregates +
                    ",\n  \"nonLinearAggregates\": " + nonLinearAggregates +
                    ",\n  \"nonIncremental\": " + nonIncremental +
                    ",\n  \"windows\": " + windows +
                    ",\n  \"nested\": " + nested +
                    ",\n  \"stateful\": " + stateful +
                    ",\n  \"maxTupleWidth\": " + maxTupleWidth +
                    "\n  \"maxDepth\": " + maxDepth +
                    "\n}";
        }

        public int stateful;

        void updateWidth(int width) {
            this.maxTupleWidth = Math.max(width, this.maxTupleWidth);
        }

        public void updateDepth(int depth) {
            this.maxDepth = Math.max(depth, this.maxDepth);
        }
    };

    final Statistics stats;
    final Map<DBSPOperator, Integer> depth;

    public CircuitStatistics(DBSPCompiler compiler) {
        super(compiler);
        this.stats = new Statistics();
        this.depth = new HashMap<>();
    }

    public int tupleWidth(DBSPType type) {
        if (type.is(DBSPTypeIndexedZSet.class)) {
            var ix = type.to(DBSPTypeIndexedZSet.class);
            return ix.keyType.getToplevelFieldCount() + ix.elementType.getToplevelFieldCount();
        }
        return type.getToplevelFieldCount();
    }

    @Override
    public void postorder(DBSPOperator operator) {
        this.stats.totalOperators++;
        if (operator.is(IInputOperator.class) && !operator.is(DBSPViewDeclarationOperator.class))
            this.stats.tables++;
        if (operator.is(DBSPViewBaseOperator.class))
            this.stats.views++;
        if (operator.is(IGCOperator.class))
            this.stats.gcOperators++;
        if (operator.is(DBSPWindowOperator.class))
            this.stats.windows++;
        if (operator.is(IJoin.class))
            this.stats.joins++;
        if (operator.is(ILinearAggregate.class))
            this.stats.linearAggregates++;
        if (operator.is(ILinear.class) && !operator.is(ILinearAggregate.class))
            this.stats.linear++;
        if (operator.is(IStateful.class))
            this.stats.stateful++;
        if (operator.is(INonLinearAggregate.class))
            this.stats.nonLinearAggregates++;
        if (operator.is(DBSPNestedOperator.class))
            this.stats.nested++;
        if (operator.is(INonIncremental.class))
            this.stats.nonIncremental++;
        int depth = 0;
        for (OutputPort port: operator.inputs) {
            int width = this.tupleWidth(port.outputType());
            this.stats.updateWidth(width);
            int inputDepth = this.depth.get(port.operator);
            depth = Math.max(inputDepth + 1, depth);
        }
        Utilities.putNew(this.depth, operator, depth);
        this.stats.updateDepth(depth);
    }

    @Override
    public void endVisit() {
        Logger.INSTANCE.belowLevel(this, 1)
                .appendSupplier(this.stats::toString)
                .append("\n");
        super.endVisit();
    }
}
