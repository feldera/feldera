package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Projection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields.FindUnusedFields;
import org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields.RemoveUnusedFields;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Find and remove unused fields. */
public class UnusedFields extends Passes {
    static class RepeatRemove extends Repeat {
        public RepeatRemove(DBSPCompiler compiler) {
            super(compiler, new OnePass(compiler));
        }

        static class OnePass extends Passes {
            OnePass(DBSPCompiler compiler) {
                super("RemoveUnusedFields", compiler);
                this.add(new RemoveUnusedFields(compiler));
                this.add(new DeadCode(compiler, true, false));
                this.add(new OptimizeWithGraph(compiler, g -> new OptimizeMaps(compiler, true, g)));
            }
        }
    }

    static class UnusedInputFields extends CircuitVisitor {
        final Map<DBSPSourceMultisetOperator, FindUnusedFields.SizedBitSet> fieldsUsed;
        final Map<DBSPSourceMultisetOperator, DBSPMapOperator> successor;
        final CircuitGraphs graphs;

        public UnusedInputFields(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler);
            this.fieldsUsed = new HashMap<>();
            this.successor = new HashMap<>();
            this.graphs = graphs;
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            OutputPort source = operator.input();
            int inputFanout = this.graphs.getGraph(this.getParent()).getFanout(operator.input().node());
            if (inputFanout > 1)
                return;
            if (source.node().is(DBSPSourceMultisetOperator.class)) {
                DBSPSourceMultisetOperator src = source.node().to(DBSPSourceMultisetOperator.class);
                FindUnusedFields unused = new FindUnusedFields(this.compiler);
                DBSPClosureExpression function = operator.getClosureFunction();
                assert function.parameters.length == 1;
                unused.apply(function.ensureTree(this.compiler));

                if (unused.foundUnusedFields() && !src.metadata.materialized) {
                    FindUnusedFields.SizedBitSet map = unused.usedFields.get(function.parameters[0]);
                    Utilities.putNew(this.fieldsUsed, src, map);

                    if (map.hasUnusedBits()) {
                        for (int i = 0; i < map.size(); i++) {
                            if (!map.get(i)) {
                                InputColumnMetadata meta = src.metadata.getColumnMetadata(i);
                                this.compiler.reportWarning(
                                        meta.getPositionRange(), "Unused column",
                                        "Column " + meta.name.singleQuote() + " of table " +
                                                src.tableName.singleQuote() + " is unused");
                            }
                        }
                    }
                }
                if (operator.hasAnnotation(a -> a.is(Projection.class))) {
                    Utilities.putNew(this.successor, src, operator);
                }
            }
        }
    }

    static class RemoveUnusedInputFields extends CircuitCloneVisitor {
        final UnusedInputFields data;
        final Map<DBSPSourceMultisetOperator, DBSPSourceMultisetOperator> replacement;

        public RemoveUnusedInputFields(DBSPCompiler compiler, UnusedInputFields data) {
            super(compiler,false);
            this.data = data;
            this.replacement = new HashMap<>();
        }

        public void postorder(DBSPSourceMultisetOperator source) {
            if (!this.data.successor.containsKey(source)) {
                super.postorder(source);
                return;
            }

            FindUnusedFields.SizedBitSet used = data.fieldsUsed.get(source);
            if (used == null) {
                super.postorder(source);
                return;
            }
            assert used.hasUnusedBits();

            DBSPMapOperator map = this.data.successor.get(source);
            List<InputColumnMetadata> remainingColumns = new ArrayList<>();
            List<DBSPTypeStruct.Field> fields = new ArrayList<>();
            Iterator<ProgramIdentifier> fieldNames = source.originalRowType.getFieldNames();
            for (int i = 0; i < used.size(); i++) {
                ProgramIdentifier fieldName = fieldNames.next();
                if (used.get(i)) {
                    InputColumnMetadata meta = source.metadata.getColumnMetadata(i);
                    remainingColumns.add(meta);
                    DBSPTypeStruct.Field field = source.originalRowType.getField(fieldName);
                    fields.add(field);
                }
            }
            DBSPTypeStruct newType = new DBSPTypeStruct(
                    source.originalRowType.getNode(), source.originalRowType.name,
                    source.originalRowType.sanitizedName, fields, source.originalRowType.mayBeNull);

            TableMetadata metadata = new TableMetadata(
                    source.metadata.tableName,
                    remainingColumns, source.metadata.getForeignKeys(),
                    source.metadata.materialized, source.metadata.isStreaming());
            DBSPSourceMultisetOperator replacement = new DBSPSourceMultisetOperator(
                    source.getNode(), source.sourceName, map.getOutputZSetType(), newType,
                    metadata, source.tableName, source.comment);
            Utilities.putNew(this.replacement, source, replacement);
        }

        public void postorder(DBSPMapOperator map) {
            OutputPort originalSource = map.input();
            DBSPOperator source = originalSource.node();
            if (!source.is(DBSPSourceMultisetOperator.class)) {
                super.postorder(map);
                return;
            }

            DBSPSourceMultisetOperator src = originalSource.node().to(DBSPSourceMultisetOperator.class);
            DBSPSourceMultisetOperator result = this.replacement.get(src);
            if (result == null) {
                super.postorder(map);
                return;
            }

            // This map follows an input that has been trimmed: replace with the trimmed input.
            this.map(map, result);
        }
    }

    public UnusedFields(DBSPCompiler compiler) {
        super("UnusedFields", compiler);
        Graph graph = new Graph(compiler);
        this.add(new RepeatRemove(compiler));
        this.add(graph);
        UnusedInputFields unusedInputs = new UnusedInputFields(compiler, graph.getGraphs());
        this.add(unusedInputs);
        this.add(new Conditional(compiler,
                new RemoveUnusedInputFields(compiler, unusedInputs), () -> this.compiler().options.ioOptions.trimInputs));
    }
}
