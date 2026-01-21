package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;

/** Tests for data with LATENESS */
public class LatenessTests  extends StreamingTestBase {
    record IO(DBSPZSetExpression input, DBSPZSetExpression output) {    }

    /** A table with a column that has lateness and an aggregate function computed over that column.
     * The table keeps track of the data waterline as batches are added. */
    static class TableWithLateness {
        final int lateness;
        final List<Integer> data;
        final Random random;
        /** Whether the aggregated column is nullable */
        final boolean nullable;
        int waterline = Integer.MIN_VALUE;
        int nextWaterline = Integer.MIN_VALUE;
        final BiFunction<Integer, Integer, Integer> aggregate;

        TableWithLateness(int lateness, BiFunction<Integer, Integer, Integer> aggregate, Random random, boolean nullable) {
            this.lateness = lateness;
            this.data = new ArrayList<>();
            this.aggregate = aggregate;
            this.random = random;
            this.nullable = nullable;
        }

        DBSPType getElementType() {
            return new DBSPTypeTuple(
                    DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true),
                    DBSPTypeString.varchar(true),
                    DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, this.nullable));
        }

        @Nullable
        Integer randomExistingValue() {
            if (this.data.isEmpty())
                return null;
            return this.data.get(this.random.nextInt(this.data.size()));
        }

        DBSPZSetExpression makeRow(int group, @Nullable Integer data) {
            boolean nullable = this.nullable;
            var dataLit = data == null ? new DBSPI32Literal() : new DBSPI32Literal(data, nullable);
            return new DBSPZSetExpression(
                    new DBSPTupleExpression(
                            new DBSPI32Literal(group, true),
                            new DBSPStringLiteral(data == null ? null : data.toString(), true),
                            dataLit));
        }

        @Nullable
        DBSPZSetExpression append(int group, @Nullable Integer data) {
            if (data != null && data >= this.waterline) {
                this.data.add(data);
                this.nextWaterline = Math.max(data, this.nextWaterline);
            }
            return makeRow(group, data);
        }

        @Nullable
        DBSPZSetExpression remove(int group, @Nullable Integer data) {
            if (data != null && data >= this.waterline) {
                this.data.remove(data);
                this.nextWaterline = Math.max(data, this.nextWaterline);
            }
            return makeRow(group, data).negate();
        }

        @Nullable Integer commit() {
            this.waterline = this.nextWaterline - this.lateness;

            Integer result = null;
            for (int d: this.data) {
                if (result == null)
                    result = d;
                else
                    result = this.aggregate.apply(result, d);
            }
            return result;
        }

        @Override
        public String toString() {
            return this.data + " WL: " + this.waterline;
        }
    }

    static class DataGen {
        final TableWithLateness table;
        final int batchSize;
        final List<Integer> data;
        final boolean stringOutput;
        @Nullable
        DBSPZSetExpression previousResult;

        DataGen(TableWithLateness table, int rows, int batchSize, Random random, boolean stringOutput) {
            this.table = table;
            this.batchSize = batchSize;
            this.data = new ArrayList<>();
            this.previousResult = null;
            this.stringOutput = stringOutput;
            for (int i = 0; i < rows; i++)
                data.add(i);
            Collections.shuffle(this.data, random);
        }

        DBSPType outputElementType() {
            return new DBSPTypeTuple(
                    DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true),
                    this.stringOutput ? DBSPTypeString.varchar(true) :
                            DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, this.table.nullable));
        }

        IO generate() {
            DBSPZSetExpression input = new DBSPZSetExpression(this.table.getElementType());
            // Remove before inserting
            Integer toDelete = table.randomExistingValue();
            // Insert some random data
            for (int i = 0; i < this.batchSize; i++) {
                if (this.data.isEmpty())
                    break;
                int toAdd = Utilities.removeLast(this.data);
                var row = this.table.append(0, toAdd);
                if (row != null)
                    input.append(row);
            }

            if (toDelete != null && !input.isEmpty()) {
                // Do not delete if there are no appends
                var row = this.table.remove(0, toDelete);
                if (row != null)
                    input.append(row);
            }

            Integer currentResult = this.table.commit();
            DBSPZSetExpression output;
            if (currentResult == null) {
                // No aggregation result -> table empty
                output = new DBSPZSetExpression(this.outputElementType());
            } else {
                DBSPExpression result = this.stringOutput ?
                        new DBSPStringLiteral(currentResult.toString(), true) :
                        new DBSPI32Literal(currentResult, this.table.nullable);
                output = new DBSPZSetExpression(
                        new DBSPTupleExpression(
                                new DBSPI32Literal(0, true),
                                result));
                Utilities.enforce(output.getElementType().sameType(this.outputElementType()));
            }

            var save = output.deepCopy();
            if (this.previousResult != null) {
                output.append(this.previousResult.negate());
            }
            this.previousResult = save;
            return new IO(input, output);
        }
    }

    void runLatenessTest(String sqlAgg, boolean nullable, BiFunction<Integer, Integer, Integer> agg) {
        final int lateness = 2;
        var ccs = this.getCCS("CREATE TABLE T(ts INT, z VARCHAR, x INT " + (nullable ? "" : "NOT NULL") +
                " LATENESS " + lateness + ");\n"
                + "CREATE VIEW V AS SELECT ts, " + sqlAgg + " FROM t GROUP BY ts;");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retain = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retain++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.retain);
            }
        });

        Random random = new Random();
        var seed = random.nextInt(10000);
        // This is like a proptest: print seed in case we need to reproduce a bug
        System.out.println(this.currentTestInformation + " random seed: " + seed);
        random.setSeed(seed);
        TableWithLateness table = new TableWithLateness(lateness, agg, random, nullable);
        DataGen gen = new DataGen(table, 30, 2, random, sqlAgg.contains("ARG"));
        while (true) {
            var io = gen.generate();
            if (io.input.isEmpty())
                break;
            /*
            System.out.println(io.input);
            System.out.println(table);
            System.out.println("out: " + io.output);
             */
            ccs.step(new Change("T", io.input), new Change("V", io.output));
        }
    }

    @Test
    public void testMaxLatenessNullable() {
        // Implemented using Max
        this.runLatenessTest("MAX(x)", true, Math::max);
    }

    @Test
    public void testMinLatenessNullable() {
        // Implemented using MinSome1
        this.runLatenessTest("MIN(x)", true, Math::min);
    }

    @Test
    public void testMaxLateness() {
        // Implemented using Max
        this.runLatenessTest("MAX(x)", false, Math::max);
    }

    @Test
    public void testArgMaxLateness() {
        // Implemented using Max
        this.runLatenessTest("ARG_MAX(z, x)", false, Math::max);
    }

    @Test
    public void testArgMaxLatenessNullable() {
        // Implemented using Max
        this.runLatenessTest("ARG_MAX(z, x)", true, Math::max);
    }

    @Test
    public void testMinLateness() {
        // Implemented using Min
        this.runLatenessTest("MIN(x)", false, Math::min);
    }

    @Test
    public void testArgMinLateness() {
        // Implemented using Min
        this.runLatenessTest("ARG_MIN(z, x)", false, Math::min);
    }

    @Test
    public void testArgMinLatenessNullable() {
        // Implemented using ArgMinSome
        this.runLatenessTest("ARG_MIN(z, x)", true, Math::min);
    }

    @Test
    public void testMix() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT, z VARCHAR, x INT LATENESS 2);
                CREATE VIEW V AS SELECT ts, MIN(x), MAX(x), ARG_MAX(z, x), ARG_MIN(z, x), SUM(x) FROM t GROUP BY ts;""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retain = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retain++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(4, this.retain);
            }
        });
    }

    @Test
    public void testMixNoGroup() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT, z VARCHAR, x INT LATENESS 2);
                CREATE VIEW V AS SELECT MIN(x), MAX(x), ARG_MAX(z, x), ARG_MIN(z, x), SUM(x) FROM t;""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retain = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retain++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(4, this.retain);
            }
        });
    }

    @Test
    public void reverseLateness() {
        // Generates GC even if the field compared does NOT have lateness
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT LATENESS 2, z VARCHAR, x INT);
                CREATE VIEW V AS SELECT ARG_MIN(ts, x) FROM T;""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retain = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retain++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.retain);
            }
        });
    }

    @Test
    public void latenessBoth() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT LATENESS 2, z VARCHAR, x INT LATENESS 3);
                CREATE VIEW V AS SELECT ARG_MIN(ts, x) FROM T;""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retain = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retain++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.retain);
            }
        });
    }
}
