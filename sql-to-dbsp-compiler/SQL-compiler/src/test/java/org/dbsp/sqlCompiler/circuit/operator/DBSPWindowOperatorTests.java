package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DBSPWindowOperatorTests {
    private static DBSPConstantOperator dataInput() {
        DBSPTypeIndexedZSet type = new DBSPTypeIndexedZSet(
                CalciteObject.EMPTY, DBSPTypeDate.INSTANCE, DBSPTypeTuple.EMPTY);
        return new DBSPConstantOperator(CalciteEmptyRel.INSTANCE,
                new DBSPIndexedZSetExpression(CalciteObject.EMPTY, type), false);
    }

    private static OutputPort controlInput(
            DBSPExpression lower, DBSPExpression upper) {
        DBSPRawTupleExpression bounds = new DBSPRawTupleExpression(
                DBSPTypeTypedBox.wrapTypedBox(lower, false),
                DBSPTypeTypedBox.wrapTypedBox(upper, false));
        DBSPApplyOperator apply = new DBSPApplyOperator(
                CalciteEmptyRel.INSTANCE,
                bounds.closure(DBSPTypeTuple.EMPTY.ref().var()),
                bounds.getType(),
                new TestScalarOperator().outputPort(),
                null);
        return apply.outputPort();
    }

    private static DBSPWindowOperator window(
            DBSPExpression lower, DBSPExpression upper) {
        return new DBSPWindowOperator(CalciteEmptyRel.INSTANCE, true, true,
                dataInput().outputPort(), controlInput(lower, upper));
    }

    private static final class TestScalarOperator extends DBSPSimpleOperator {
        TestScalarOperator() {
            super(CalciteEmptyRel.INSTANCE, "apply_test_source", null,
                    DBSPTypeTuple.EMPTY, false);
        }

        @Override
        public DBSPSimpleOperator with(
                DBSPExpression function,
                DBSPType outputType,
                List<OutputPort> newInputs,
                boolean force) {
            return this;
        }

        @Override
        public void accept(CircuitVisitor visitor) {
            visitor.push(this);
            visitor.pop(this);
        }
    }

    @Test
    public void acceptsMatchingBoundTypes() {
        DBSPWindowOperator window = window(
                new DBSPDateLiteral("2025-01-01"),
                new DBSPDateLiteral("2025-12-31"));
        Assert.assertTrue(window.left().getOutputIndexedZSetType().keyType
                .sameType(DBSPTypeDate.INSTANCE));
    }

    @Test
    public void rejectsMismatchedUpperBoundType() {
        Assert.assertThrows(InternalCompilerError.class, () -> window(
                new DBSPDateLiteral("2025-01-01"),
                new DBSPTimestampLiteral("2025-12-31 00:00:00", false)));
    }

    @Test
    public void rejectsMismatchedLowerBoundType() {
        Assert.assertThrows(InternalCompilerError.class, () -> window(
                new DBSPTimestampLiteral("2025-01-01 00:00:00", false),
                new DBSPDateLiteral("2025-12-31")));
    }
}
