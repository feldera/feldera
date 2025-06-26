package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CSE;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Conditional;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeMaps;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeWithGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Find and remove unused fields. */
public class UnusedFields extends Passes {
    static class RepeatRemove extends Repeat {
        public RepeatRemove(DBSPCompiler compiler,
                            AnalyzedSet<DBSPExpression> functionsAnalyzed,
                            AnalyzedSet<DBSPOperator> mapOperators,
                            AnalyzedSet<DBSPExpression> filterFunctions) {
            super(compiler, new OnePass(compiler, functionsAnalyzed, mapOperators, filterFunctions));
        }

        static class OnePass extends Passes {
            OnePass(DBSPCompiler compiler,
                    AnalyzedSet<DBSPExpression> functionsAnalyzed,
                    AnalyzedSet<DBSPOperator> mapOperators,
                    AnalyzedSet<DBSPExpression> filterFunctions) {
                super("UnusedFieldsOnePass", compiler);
                this.add(new RemoveUnusedFields(compiler, functionsAnalyzed));
                // Very important, because OptimizeMaps works backward
                this.add(new DeadCode(compiler, true, false));

                Graph graph0 = new Graph(compiler);
                this.add(graph0);
                this.add(new OptimizeMaps(compiler, true, graph0.getGraphs(), mapOperators));
                this.add(new DeadCode(compiler, true, false));

                Graph graph = new Graph(compiler);
                this.add(graph);
                FindCommonProjections fcp = new FindCommonProjections(compiler, graph.getGraphs());
                this.add(fcp);
                this.add(new ReplaceCommonProjections(compiler, fcp));
                this.add(new OptimizeWithGraph(compiler,
                        g -> new TrimFilters(compiler, g, filterFunctions), 1));
                this.add(new CSE(compiler));
            }
        }
    }

    static class FindUnusedInputFields extends CircuitWithGraphsVisitor {
        final Map<DBSPSourceMultisetOperator, FindUnusedFields> finders;
        final Map<DBSPSourceMultisetOperator, FieldUseMap> fieldsUsed;
        final Map<DBSPSourceMultisetOperator, DBSPMapOperator> successor;

        public FindUnusedInputFields(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
            this.fieldsUsed = new HashMap<>();
            this.finders = new HashMap<>();
            this.successor = new HashMap<>();
        }

        @Override
        public void postorder(DBSPMapOperator operator) {
            OutputPort source = operator.input();
            int inputFanout = this.getGraph().getFanout(operator.input().node());
            if (inputFanout > 1)
                return;
            if (source.node().is(DBSPSourceMultisetOperator.class)) {
                DBSPSourceMultisetOperator src = source.node().to(DBSPSourceMultisetOperator.class);
                FindUnusedFields unused = new FindUnusedFields(this.compiler);
                DBSPClosureExpression function = operator.getClosureFunction();
                Utilities.enforce(function.parameters.length == 1);
                unused.apply(function.ensureTree(this.compiler));

                if (unused.foundUnusedFields(1) && !src.metadata.materialized) {
                    FieldUseMap map = unused.parameterFieldMap.get(function.parameters[0]).deref();
                    Utilities.putNew(this.finders, src, unused);
                    Utilities.putNew(this.fieldsUsed, src, map);

                    if (map.hasUnusedFields(1)) {
                        for (int i = 0; i < map.size(); i++) {
                            if (!map.isUsed(i)) {
                                InputColumnMetadata meta = src.metadata.getColumnMetadata(i);
                                this.compiler.reportWarning(
                                        meta.getPositionRange(), "Unused column",
                                        "Column " + meta.name.singleQuote() + " of table " +
                                                src.tableName.singleQuote() + " is unused");
                                IHasSchema schema = this.compiler.metadata.getTableDescription(src.tableName);
                                schema.getColumns().get(i).setUnused();
                            }
                        }
                    }
                }
                Utilities.putNew(this.successor, src, operator);
            }
        }
    }

    static class TrimInputs extends CircuitCloneVisitor {
        final FindUnusedInputFields data;
        final Map<DBSPSourceMultisetOperator, DBSPSourceMultisetOperator> replacement;

        public TrimInputs(DBSPCompiler compiler, FindUnusedInputFields data) {
            super(compiler,false);
            this.data = data;
            this.replacement = new HashMap<>();
        }

        public void postorder(DBSPSourceMultisetOperator source) {
            if (!this.data.successor.containsKey(source)) {
                super.postorder(source);
                return;
            }

            FieldUseMap used = this.data.fieldsUsed.get(source);
            if (used == null) {
                super.postorder(source);
                return;
            }
            Utilities.enforce(used.hasUnusedFields(1));

            List<InputColumnMetadata> remainingColumns = new ArrayList<>();
            List<DBSPTypeStruct.Field> fields = new ArrayList<>();
            Iterator<ProgramIdentifier> fieldNames = source.originalRowType.getFieldNames();
            for (int i = 0; i < used.size(); i++) {
                ProgramIdentifier fieldName = fieldNames.next();
                if (used.isUsed(i)) {
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
                    source.getRelNode(), source.sourceName, new DBSPTypeZSet(newType.toTuple()), newType,
                    metadata, source.tableName, source.comment);
            this.addOperator(replacement);
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
            DBSPSourceMultisetOperator newSource = this.replacement.get(src);
            if (newSource == null) {
                super.postorder(map);
                return;
            }

            FindUnusedFields finder = this.data.finders.get(src);
            RewriteFields rw = finder.getFieldRewriter(1);
            DBSPClosureExpression newMap = rw.rewriteClosure(map.getClosureFunction());
            DBSPSimpleOperator result = new DBSPMapOperator(map.getRelNode(), newMap, newSource.outputPort());
            this.map(map, result);
        }
    }

    public UnusedFields(DBSPCompiler compiler) {
        super("UnusedFields", compiler);

        AnalyzedSet<DBSPExpression> functionsAnalyzed = new AnalyzedSet<>();
        AnalyzedSet<DBSPOperator> mapOperators = new AnalyzedSet<>();
        AnalyzedSet<DBSPExpression> filterFunctionsAnalyzed = new AnalyzedSet<>();
        this.add(new RepeatRemove(compiler, functionsAnalyzed, mapOperators, filterFunctionsAnalyzed));
        Graph graph = new Graph(compiler);
        this.add(graph);
        FindUnusedInputFields unusedInputs = new FindUnusedInputFields(compiler, graph.getGraphs());
        this.add(unusedInputs);
        this.add(new Conditional(compiler,
                new TrimInputs(compiler, unusedInputs),
                () -> this.compiler().options.ioOptions.trimInputs));
    }
}
