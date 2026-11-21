package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Utilities;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Independently capture Java's symbolic result before it is compressed into a FieldUseMap. */
public final class InputFieldOracle extends CircuitWithGraphsVisitor {
    private final ArrayNode observations;

    private InputFieldOracle(DBSPCompiler compiler, Graph graph, ArrayNode observations) {
        super(compiler, graph.getGraphs());
        this.observations = observations;
    }

    public static void capture(DBSPCompiler compiler, DBSPCircuit circuit, ArrayNode observations) {
        long names = DBSPVariablePath.nextGeneratedName();
        try {
            var graph = new Graph(compiler);
            graph.apply(circuit);
            new InputFieldOracle(compiler, graph, observations).apply(circuit);
        } finally {
            DBSPVariablePath.restoreGeneratedNames(names);
        }
    }

    public static void common(DBSPCompiler compiler, DBSPCircuit circuit, FindCommonProjections analysis, ArrayNode observed) {
        long names = DBSPVariablePath.nextGeneratedName();
        try {
            var graph = new Graph(compiler);
            graph.apply(circuit);
            new CircuitWithGraphsVisitor(compiler, graph.getGraphs()) {
                @Override
                public void postorder(org.dbsp.sqlCompiler.circuit.operator.DBSPOperator producer) {
                    if (!analysis.outputProjection.containsKey(producer))
                        return;
                    var group = observed.addObject().put("producer", producer.id);
                    var readers = group.putArray("readers");
                    var union = orderedPaths();
                    for (var edge : this.getGraph().getSuccessors(producer)) {
                        var reader = edge.node().to(DBSPUnaryOperator.class);
                        readers.add(reader.id);
                        var closure = reader.getClosureFunction().ensureTree(compiler).to(DBSPClosureExpression.class);
                        var finder = new FindUsedFields(compiler);
                        finder.apply(closure);
                        paths(finder.get(closure.body), union);
                    }
                    group.put("depth", producer.outputType(0).is(org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet.class) ? 1 : 2);
                    group.set("paths", Utilities.deterministicObjectMapper().valueToTree(union));
                }
            }.apply(circuit);
        } finally {
            DBSPVariablePath.restoreGeneratedNames(names);
        }
    }

    private static TreeSet<List<Integer>> orderedPaths() {
        return new TreeSet<>((a, b) -> Arrays.compare(a.toArray(Integer[]::new), b.toArray(Integer[]::new)));
    }

    private static void paths(IUsedFields used, Set<List<Integer>> paths) {
        if (used instanceof ParameterField field)
            paths.add(field.indexes());
        else if (used instanceof BorrowedField borrowed)
            paths.add(borrowed.field().indexes());
        else if (used instanceof TupleOfFields tuple)
            tuple.fields().forEach(field -> paths(field, paths));
        else if (used instanceof FieldSet set)
            set.used().forEach(field -> paths(field, paths));
        else
            throw new IllegalStateException("Unknown field-use result " + used);
    }

    private void record(DBSPUnaryOperator reader) {
        var source = reader.input().node();
        if (!(source instanceof DBSPSourceMultisetOperator) || this.getGraph().getFanout(source) > 1)
            return;
        var closure = reader.getClosureFunction().ensureTree(this.compiler).to(DBSPClosureExpression.class);
        var analysis = new FindUsedFields(this.compiler);
        analysis.apply(closure);
        var paths = orderedPaths();
        paths(analysis.get(closure.body), paths);
        this.observations.addObject().put("reader", reader.id)
                .set("paths", Utilities.deterministicObjectMapper().valueToTree(paths));
    }

    @Override
    public void postorder(DBSPMapOperator reader) {
        this.record(reader);
    }

    @Override
    public void postorder(DBSPMapIndexOperator reader) {
        this.record(reader);
    }
}
