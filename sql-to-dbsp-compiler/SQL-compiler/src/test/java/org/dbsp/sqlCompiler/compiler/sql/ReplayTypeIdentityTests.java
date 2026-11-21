package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator.Computation;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator.ComputationChain;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator.ComputationKind;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

public class ReplayTypeIdentityTests {
    @Test public void rankRetainsItsSerializedOutputType() throws Exception {
        checkRankedOutputType(DBSPRankOperator.class,
                "SELECT k, v, RANK() OVER (PARTITION BY k ORDER BY v) AS r FROM t");
    }

    @Test public void topKRetainsItsSerializedOutputType() throws Exception {
        checkRankedOutputType(DBSPIndexedTopKOperator.class, """
                SELECT * FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS r FROM t)
                WHERE r <= 3""");
    }

    private static <T extends DBSPSimpleOperator> void checkRankedOutputType(Class<T> kind, String query)
            throws Exception {
        var options = new CompilerOptions();
        options.languageOptions.incrementalize = true;
        var compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation("CREATE TABLE t (k INT, v INT); CREATE VIEW ranked AS " + query + ";");
        var circuit = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        var rank = circuit.allOperators.stream().filter(kind::isInstance).map(kind::cast).findFirst().orElseThrow();
        var writer = ToJsonOuterVisitor.createForReplay(compiler);
        writer.apply(circuit);
        var mapper = Utilities.deterministicObjectMapper();
        var decoder = new JsonDecoder();
        decoder.decodeOuter(mapper.readTree(writer.getJsonString()), DBSPCircuit.class);
        var decodedRank = decoder.decodeOuter(mapper.createObjectNode().put("node", rank.id), kind);
        var typeReference = mapper.createObjectNode().put("node", rank.outputType.getId());
        Assert.assertSame(decodedRank.outputType, decoder.decodeInner(typeReference, DBSPTypeIndexedZSet.class));
    }

    @Test public void successiveChainsRetainTheirSharedCollectionType() throws Exception {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var row = integer.ref().var();
        var map = new Computation(ComputationKind.Map, row.deref().closure(row));
        var filter = new Computation(ComputationKind.Filter, new DBSPBoolLiteral(true).closure(row));
        var first = new DBSPChainOperator(site,
                new ComputationChain(input.outputType).add(map).add(filter), input.outputPort());
        var second = new DBSPChainOperator(site,
                new ComputationChain(first.outputType).add(filter).add(filter), first.outputPort());
        circuit.addOperator(input);
        circuit.addOperator(first);
        circuit.addOperator(second);

        var writer = ToJsonOuterVisitor.createForReplay(compiler);
        writer.apply(circuit);
        var decoder = new JsonDecoder();
        var restored = decoder.decodeOuter(Utilities.deterministicObjectMapper().readTree(writer.getJsonString()),
                DBSPCircuit.class);
        var consumer = restored.allOperators.getLast().to(DBSPChainOperator.class);
        Assert.assertSame(consumer.input().outputType(), consumer.chain.inputType());
        Assert.assertSame(consumer.chain.inputType(), consumer.outputType);
    }

    private static void preservesSharedResultType(DBSPExpression expression) throws Exception {
        var tuple = new DBSPTupleExpression(expression, expression.getType().var());
        var stream = new JsonStream(new IndentStream(new StringBuilder()));
        tuple.accept(new ToJsonInnerVisitor(new DBSPCompiler(new CompilerOptions()), stream, 1));
        var json = Utilities.deterministicObjectMapper().readTree(stream.toString());
        var decoded = new JsonDecoder().decodeInner(json, DBSPTupleExpression.class);
        Assert.assertNotNull(decoded.fields);
        Assert.assertSame("The decoder must retain shared type identity, not merely structural equality",
                decoded.fields[0].getType(), decoded.fields[1].getType());
        Assert.assertTrue(expression.getType().sameType(decoded.fields[0].getType()));
    }

    @Test public void nullableTupleUnwrapKeepsSharedType() throws Exception {
        var nullable = new DBSPTypeTuple(CalciteObject.EMPTY, true, null,
                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false));
        preservesSharedResultType(new DBSPUnwrapExpression(CalciteObject.EMPTY, "", nullable.var()));
    }

    @Test public void unsignedWindowKeyKeepsSharedType() throws Exception {
        preservesSharedResultType(new DBSPUnsignedWrapExpression(CalciteObject.EMPTY,
                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false).var(), true, false));
    }

    @Test public void nullPredicateKeepsSharedType() throws Exception {
        preservesSharedResultType(new DBSPIsNullExpression(CalciteObject.EMPTY,
                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true).var()));
    }

    @Test public void nullableTupleFieldKeepsSharedType() throws Exception {
        var nullable = new DBSPTypeTuple(CalciteObject.EMPTY, true, null,
                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false));
        preservesSharedResultType(nullable.var().field(0));
    }

    @Test public void borrowKeepsSharedType() throws Exception {
        preservesSharedResultType(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false).var().borrow());
    }

    @Test public void someKeepsSharedType() throws Exception {
        preservesSharedResultType(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false).var().some());
    }

    @Test public void dereferencedCollectionKeepsSharedType() throws Exception {
        var element = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false);
        preservesSharedResultType(new DBSPTypeArray(element, false).var().deref());
        preservesSharedResultType(new DBSPTypeMap(element, element, true).var().deref());
    }

    private static DBSPNoComparatorExpression tupleComparator() {
        return new DBSPNoComparatorExpression(CalciteObject.EMPTY,
                new DBSPTypeTuple(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false)));
    }

    @Test public void emptyComparatorKeepsSharedType() throws Exception {
        preservesSharedResultType(tupleComparator());
    }

    @Test public void fieldComparatorKeepsSharedType() throws Exception {
        preservesSharedResultType(tupleComparator().field(0, true));
    }

    @Test public void directComparatorKeepsSharedType() throws Exception {
        preservesSharedResultType(new DBSPDirectComparatorExpression(CalciteObject.EMPTY, tupleComparator(), true));
    }

    @Test public void customOrderKeepsSharedType() throws Exception {
        var comparator = tupleComparator().field(0, true);
        preservesSharedResultType(new DBSPCustomOrdExpression(CalciteObject.EMPTY,
                comparator.comparedValueType().var(), comparator));
    }

    @Test public void closureKeepsSharedFunctionType() throws Exception {
        var parameter = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false).var();
        preservesSharedResultType(parameter.closure(parameter.asParameter()));
    }

    @Test public void sortKeepsSharedFunctionType() throws Exception {
        var comparator = tupleComparator().field(0, true);
        preservesSharedResultType(new DBSPSortExpression(CalciteObject.EMPTY,
                comparator.comparedValueType(), comparator));
    }
}
