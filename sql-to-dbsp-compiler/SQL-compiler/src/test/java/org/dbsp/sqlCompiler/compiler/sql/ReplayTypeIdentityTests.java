package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

public class ReplayTypeIdentityTests {
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
