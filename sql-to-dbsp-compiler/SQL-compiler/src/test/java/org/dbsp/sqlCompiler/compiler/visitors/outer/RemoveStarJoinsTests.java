package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.DeltaExpandOperators;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.StarJoinFilterMapDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RemoveStarJoinsTests {
    /** Splitting a filtered five-way join takes two iterations. Its fifth input
     * passes unpaired through two levels of the balanced tree. */
    @Test
    public void filteredJoinPreservesLeafColumns() {
        checkFilteredJoin(false, false);
        checkFilteredJoin(true, false);
    }

    @Test
    public void nestedFilteredJoinIsFullyExpanded() {
        checkFilteredJoin(true, true);
    }

    @Test
    public void deltaTermsUseOneChangingInputEach() {
        var options = new CompilerOptions();
        options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.optimizer.rustPasses");
        var compiler = new DBSPCompiler(options);
        var circuit = new DBSPCircuit(new ProgramMetadata(), true);
        var star = addFilteredJoin(circuit, true);
        var expand = new DeltaExpandOperators(compiler);
        expand.apply(circuit);
        var expansion = (StarJoinFilterMapDeltaExpansion) expand.expansion.get(star);
        Assert.assertEquals(5, expansion.joins.size());
        for (int i = 0; i < expansion.joins.size(); i++) {
            var term = expansion.joins.get(i);
            Assert.assertEquals(4, term.joins.size());
            Assert.assertTrue(term.joins.stream().allMatch(DBSPStreamJoinIndexOperator.class::isInstance));
            var leaves = List.of(term.joins.get(0).left(), term.joins.get(0).right(),
                    term.joins.get(1).left(), term.joins.get(1).right(), term.joins.get(3).right());
            for (int j = 0; j < leaves.size(); j++) {
                var integral = expansion.integrators.get(j);
                Assert.assertEquals(j == i ? integral.input() : integral.outputPort(), leaves.get(j));
            }
            Assert.assertEquals(term.result.outputPort(), expansion.sum.inputs.get(i));
        }
        Assert.assertEquals(expansion.sum.outputPort(), expansion.filter.input());
        Assert.assertEquals(expansion.filter.outputPort(), expansion.last().inputs.getFirst());
    }

    private static DBSPStarJoinFilterMapOperator addFilteredJoin(ICircuit body, boolean withMap) {
        var site = CalciteEmptyRel.INSTANCE;
        var integer = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false);
        var row = new DBSPTypeTuple(integer);
        var index = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, row, row);
        List<OutputPort> inputs = new ArrayList<>();
        List<DBSPVariablePath> parameters = new ArrayList<>();
        parameters.add(row.ref().var());
        for (int i = 0; i < 5; i++) {
            var source = new DBSPConstantOperator(site, new DBSPIndexedZSetExpression(CalciteObject.EMPTY, index));
            var delta = new DBSPDifferentiateOperator(site, source.outputPort());
            body.addOperator(source);
            body.addOperator(delta);
            inputs.add(delta.outputPort());
            parameters.add(row.ref().var());
        }
        var tuple = new DBSPTupleExpression(parameters.get(5).deref().field(0), parameters.get(1).deref().field(0));
        var function = tuple.closure(parameters.toArray(DBSPVariablePath[]::new));
        var input = tuple.getType().ref().var();
        var predicate = new DBSPBoolLiteral(true).closure(input);
        var projected = new DBSPTupleExpression(input.deref().field(1));
        var map = withMap ? projected.closure(input) : null;
        var result = new DBSPTypeZSet(withMap ? projected.getType() : tuple.getType());
        var star = new DBSPStarJoinFilterMapOperator(site, result, function, predicate, map, inputs);
        body.addOperator(star);
        return star;
    }

    private static void checkFilteredJoin(boolean withMap, boolean nested) {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var circuit = new DBSPCircuit(new ProgramMetadata(), true);
        var site = CalciteEmptyRel.INSTANCE;
        ICircuit body = circuit;
        if (nested) {
            var region = new DBSPNestedOperator(site);
            circuit.addOperator(region);
            body = region;
        }
        addFilteredJoin(body, withMap);

        CircuitTransform remove = new RemoveStarJoins(compiler);
        if (Boolean.getBoolean("feldera.optimizer.rustPasses"))
            remove = new RustCircuitPass(compiler, remove, new OptimizerSnapshots(compiler));
        var expanded = remove.apply(circuit);
        new ValidateStreamKinds(compiler).apply(expanded);
        List<DBSPOperator> operators = new ArrayList<>();
        ICircuit expandedBody = nested ? expanded.allOperators.get(0).to(DBSPNestedOperator.class) : expanded;
        expandedBody.getAllOperators().forEach(operators::add);
        Assert.assertFalse(operators.stream().anyMatch(DBSPStarJoinBaseOperator.class::isInstance));
        var joins = operators.stream().filter(DBSPJoinIndexOperator.class::isInstance)
                .map(DBSPJoinIndexOperator.class::cast).toList();
        Assert.assertEquals(4, joins.size());
        Assert.assertEquals(joins.get(0).outputPort(), joins.get(2).left());
        Assert.assertEquals(joins.get(1).outputPort(), joins.get(2).right());
        Assert.assertEquals(joins.get(2).outputPort(), joins.get(3).left());

        var filter = operators.stream().filter(DBSPFilterOperator.class::isInstance)
                .map(DBSPFilterOperator.class::cast).findFirst().orElseThrow();
        var read = filter.input().operator.to(DBSPMapOperator.class);
        Assert.assertEquals(joins.get(3).outputPort(), read.input());
        var key = new DBSPTupleExpression(new DBSPI32Literal(0));
        DBSPExpression[] columns = new DBSPExpression[5];
        for (int i = 0; i < columns.length; i++)
            columns[i] = new DBSPI32Literal(i + 1);
        var values = new DBSPTupleExpression(columns);
        var pair = new DBSPRawTupleExpression(key.borrow(), values.borrow());
        var actual = read.getClosureFunction().call(pair).reduce(compiler);
        Assert.assertTrue(actual.toString(), new DBSPTupleExpression(columns[4], columns[0]).equivalent(actual));
        var last = operators.get(operators.size() - 1);
        if (withMap)
            Assert.assertEquals(filter.outputPort(), last.to(DBSPMapOperator.class).input());
        else
            Assert.assertSame(filter, last);
    }
}
