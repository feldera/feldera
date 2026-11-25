package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.ProgramSchema;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CSE;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Conditional;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeProjections;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeWithGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/** Find and remove unused fields. */
public class UnusedFields extends Passes {
    static class RepeatRemove extends Repeat {
        public RepeatRemove(DBSPCompiler compiler,
                            AnalyzedSet<DBSPOperator> operatorsAnalyzed,
                            AnalyzedSet<DBSPOperator> mapOperators,
                            AnalyzedSet<DBSPOperator> mapAfterFilter,
                            UnaryOperator<CircuitTransform> select) {
            super(compiler, new OnePass(compiler, operatorsAnalyzed, mapOperators, mapAfterFilter, select));
        }

        static class OnePass extends Passes {
            OnePass(DBSPCompiler compiler,
                    AnalyzedSet<DBSPOperator> operatorsAnalyzed,
                    AnalyzedSet<DBSPOperator> mapOperators,
                    AnalyzedSet<DBSPOperator> mapAfterFilter,
                    UnaryOperator<CircuitTransform> select) {
                super("UnusedFieldsOnePass", compiler);
                this.add(new RemoveUnusedFields(compiler, operatorsAnalyzed));
                // Very important, because OptimizeMaps works backward
                this.add(select.apply(new DeadCode(compiler, true)));

                Graph graph0 = new Graph(compiler);
                this.add(select.apply(graph0));
                this.add(new OptimizeProjections(compiler, true, graph0.getGraphs(), mapOperators));
                this.add(select.apply(new DeadCode(compiler, true)));

                Graph graph = new Graph(compiler);
                this.add(select.apply(graph));
                FindCommonProjections fcp = new FindCommonProjections(compiler, graph.getGraphs());
                this.add(select.apply(fcp));
                this.add(select.apply(new ReplaceCommonProjections(compiler, fcp)));
                this.add(select.apply(new OptimizeWithGraph(compiler,
                        g -> new TrimFilters(compiler, g, mapAfterFilter), 1)));
                this.add(select.apply(new OptimizeWithGraph(compiler,
                        g -> new TrimWindows(compiler, g), 1)));
                this.add(select.apply(new CSE(compiler)));
            }
        }
    }

    public static class FindUnusedInputFields extends CircuitWithGraphsVisitor {
        record NarrowedSource(DBSPUnaryOperator reader, List<Integer> columns, @Nullable ParameterFieldUse uses) {
            ParameterFieldUse readerUses(DBSPCompiler compiler) {
                return this.uses != null ? this.uses :
                        new FindUsedFields(compiler).findUsedFields(this.reader.getClosureFunction());
            }
        }

        final Map<DBSPSourceMultisetOperator, NarrowedSource> narrowed = new HashMap<>();

        public FindUnusedInputFields(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            this.process(operator);
        }

        @Override
        public void postorder(DBSPMapIndexOperator operator) {
            this.process(operator);
        }

        void process(DBSPUnaryOperator operator) {
            OutputPort source = operator.input();
            int inputFanout = this.getGraph().getFanout(operator.input().node());
            if (inputFanout > 1)
                return;
            if (source.node().is(DBSPSourceMultisetOperator.class)) {
                DBSPSourceMultisetOperator src = source.node().to(DBSPSourceMultisetOperator.class);
                FindUsedFields unused = new FindUsedFields(this.compiler);
                DBSPClosureExpression function = operator.getClosureFunction();
                Utilities.enforce(function.parameters.length == 1);
                var useMap = unused.findUsedFields(function);

                if (useMap.hasUnusedFields(1)) {
                    var fields = useMap.get(function.parameters[0]).deref();
                    this.record(operator, src, fields.usageAtDepth(1), useMap);
                }
            }
        }

        public void recordColumns(DBSPUnaryOperator reader, List<Boolean> used) {
            this.record(reader, reader.input().to(DBSPSourceMultisetOperator.class), used, null);
        }

        private void record(DBSPUnaryOperator reader, DBSPSourceMultisetOperator source,
                            List<Boolean> used, @Nullable ParameterFieldUse uses) {
            var kept = new ArrayList<Integer>();
            for (int i = 0; i < used.size(); i++) {
                if (used.get(i)) {
                    kept.add(i);
                    continue;
                }
                InputColumnMetadata meta = source.metadata.getColumnMetadata(i);
                if (!meta.isPrimaryKey) {
                    this.compiler.reportWarning(meta.getPositionRange(), "Unused column",
                            "Column " + meta.name.singleQuote() + " of table " + source.tableName.singleQuote() + " is unused");
                    ProgramSchema schema = this.compiler.metadata.getTableDescription(source.tableName);
                    schema.markColumnUnused(i);
                    meta.setUnused();
                }
            }
            if (!source.metadata.materialized)
                Utilities.putNew(this.narrowed, source, new NarrowedSource(reader, List.copyOf(kept), uses));
        }
    }

    public static class TrimInputs extends CircuitCloneVisitor {
        final FindUnusedInputFields data;
        final Map<DBSPSourceMultisetOperator, DBSPSourceMultisetOperator> replacement;

        public TrimInputs(DBSPCompiler compiler, FindUnusedInputFields data) {
            super(compiler,false);
            this.data = data;
            this.replacement = new HashMap<>();
        }

        public record Layout(long reader, List<Integer> columns, String hashName) {}

        /** State from the preceding analysis. The struct name uses the Java type printer. */
        public Map<Long, Layout> layouts() {
            Map<Long, Layout> layouts = new HashMap<>();
            this.data.narrowed.forEach((source, narrowed) -> {
                var columns = narrowed.columns();
                var declared = List.copyOf(source.originalRowType.fields.values());
                var kept = columns.stream().map(declared::get).toList();
                layouts.put(source.id, new Layout(narrowed.reader().id, columns,
                        DBSPTypeStruct.generateStructHashName(source.originalRowType.name, kept)));
            });
            return layouts;
        }

        public void postorder(DBSPSourceMultisetOperator source) {
            var narrowed = this.data.narrowed.get(source);
            if (narrowed == null) {
                super.postorder(source);
                return;
            }
            DBSPTypeTuple row = source.getOutputZSetElementType().to(DBSPTypeTuple.class);
            var declared = List.copyOf(source.originalRowType.fields.values());
            var columnTypes = narrowed.columns().stream().map(row::getFieldType).toList();
            var remainingColumns = narrowed.columns().stream().map(source.metadata::getColumnMetadata).toList();
            var fields = narrowed.columns().stream().map(declared::get).toList();
            DBSPTypeStruct newType = new DBSPTypeStruct(
                    source.originalRowType.getNode(), source.originalRowType.name,
                    fields, source.originalRowType.mayBeNull);

            TableMetadata metadata = new TableMetadata(
                    source.metadata.tableName,
                    remainingColumns, source.metadata.getForeignKeys(), source.metadata.expectedSize,
                    source.metadata.materialized, source.metadata.isStreaming(), source.metadata.skipUnusedColumns);
            // Retain the circuit tuple types for nested ROW fields in the declared schema.
            DBSPTypeTuple outputRow = new DBSPTypeTuple(row.getNode(), row.mayBeNull, newType, columnTypes);
            DBSPSourceMultisetOperator replacement = new DBSPSourceMultisetOperator(
                    source.getRelNode(), source.sourceName, new DBSPTypeZSet(outputRow), newType,
                    metadata, source.tableName, source.kind, source.comment);
            this.addOperator(replacement);
            Utilities.putNew(this.replacement, source, replacement);
        }

        @Override
        public void postorder(DBSPMapOperator map) {
            OutputPort originalSource = map.input();
            DBSPOperator source = originalSource.node();
            if (!source.is(DBSPSourceMultisetOperator.class)) {
                super.postorder(map);
                return;
            }

            DBSPSourceMultisetOperator src = originalSource.node().to(DBSPSourceMultisetOperator.class);
            DBSPSourceMultisetOperator newSource = this.replacement.get(src);
            if (newSource == null) {
                super.postorder(map);
                return;
            }

            var finder = this.data.narrowed.get(src).readerUses(this.compiler);
            RewriteFields rw = finder.getFieldRewriter(this.compiler, 1);
            DBSPClosureExpression newMap = rw.rewriteClosure(map.getClosureFunction());
            DBSPSimpleOperator result = new DBSPMapOperator(map.getRelNode(), newMap, newSource.outputPort());
            this.map(map, result);
        }

        public void postorder(DBSPMapIndexOperator map) {
            OutputPort originalSource = map.input();
            DBSPOperator source = originalSource.node();
            if (!source.is(DBSPSourceMultisetOperator.class)) {
                super.postorder(map);
                return;
            }

            DBSPSourceMultisetOperator src = originalSource.node().to(DBSPSourceMultisetOperator.class);
            DBSPSourceMultisetOperator newSource = this.replacement.get(src);
            if (newSource == null) {
                super.postorder(map);
                return;
            }

            var finder = this.data.narrowed.get(src).readerUses(this.compiler);
            RewriteFields rw = finder.getFieldRewriter(this.compiler, 1);
            DBSPClosureExpression newMap = rw.rewriteClosure(map.getClosureFunction());
            DBSPSimpleOperator result = new DBSPMapIndexOperator(map.getRelNode(), newMap, newSource.outputPort());
            this.map(map, result);
        }
    }

    public UnusedFields(DBSPCompiler compiler) {
        this(compiler, UnaryOperator.identity());
    }

    public UnusedFields(DBSPCompiler compiler, UnaryOperator<CircuitTransform> select) {
        super("UnusedFields", compiler);

        AnalyzedSet<DBSPOperator> operatorsAnalyzed = new AnalyzedSet<>();
        AnalyzedSet<DBSPOperator> mapOperators = new AnalyzedSet<>();
        AnalyzedSet<DBSPOperator> mapAfterFilter = new AnalyzedSet<>();
        this.add(new RepeatRemove(compiler, operatorsAnalyzed, mapOperators, mapAfterFilter, select));
        Graph graph = new Graph(compiler);
        this.add(select.apply(graph));
        FindUnusedInputFields unusedInputs = new FindUnusedInputFields(compiler, graph.getGraphs());
        this.add(select.apply(unusedInputs));
        this.add(new Conditional(compiler,
                select.apply(new TrimInputs(compiler, unusedInputs)),
                () -> this.compiler().options.ioOptions.trimInputs));
    }
}
