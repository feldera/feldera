package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SetopOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ControlFlowException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/** Rewrites each long chain of one associative set operator (UNION, UNION ALL, INTERSECT, INTERSECT ALL)
 * into a balanced tree, with the same operands in the same order.  The parser builds left-deep chains:
 * a chain of N queries is N - 1 calls deep, and the recursive visitors of the compiler and of Calcite
 * overflow the stack on long chains.  The balanced tree is log2(N) calls deep.
 *
 * <p>SQL types a chain pairwise, in the left-deep grouping, and the casts that make the column types
 * agree can depend on the grouping.  The calls of a balanced tree use a {@link BalancedSetOperator},
 * which throws {@link MixedTypes} during validation when the column types of its operands need such
 * casts; the statement is then validated again in the left-deep form ({@link #leftDeep}).
 * {@link FelderaSqlToRelConverter} converts each balanced tree to a single set operation. */
public class BalanceSetOperations extends SqlShuttle {
    /** Chains with at most this many queries keep their shape: they are shallow enough
     * for the recursive visitors, and their plans and error messages stay unchanged. */
    public static final int MAX_CHAIN = 100;

    /** Thrown during validation when the column types of the operands of a balanced call
     * need casts that depend on the grouping */
    public static final class MixedTypes extends ControlFlowException {}

    /** Accepts operands whose column types give the same result type and values in every grouping;
     * throws {@link MixedTypes} otherwise */
    static final class AssociativeColumnTypes extends SetopOperandTypeChecker {
        /** True if the columns of types 'left' and 'right' differ at most in nullability, which needs
         * no casts, or are strings.  For strings of different lengths the type system returns VARCHAR,
         * so in every grouping no string is padded, and each string is converted to VARCHAR at most once. */
        static boolean associative(RelDataTypeFactory factory, RelDataType left, RelDataType right) {
            if (SqlTypeUtil.equalSansNullability(factory, left, right))
                return true;
            return SqlTypeUtil.isCharacter(left) && SqlTypeUtil.isCharacter(right)
                    && factory.getTypeSystem().shouldConvertRaggedUnionTypesToVarying();
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            RelDataTypeFactory factory = callBinding.getTypeFactory();
            RelDataType left = callBinding.getOperandType(0);
            RelDataType right = callBinding.getOperandType(1);
            if (left.getFieldCount() != right.getFieldCount())
                throw new MixedTypes();
            for (int i = 0; i < left.getFieldCount(); i++) {
                RelDataType leftType = left.getFieldList().get(i).getType();
                RelDataType rightType = right.getFieldList().get(i).getType();
                if (!associative(factory, leftType, rightType))
                    throw new MixedTypes();
            }
            return super.checkOperandTypes(callBinding, throwOnFailure);
        }
    }

    /** The operator of the calls of a balanced tree: it validates like 'standard',
     * except that it rejects operands whose column types need casts that depend on the grouping */
    public static final class BalancedSetOperator extends SqlSetOperator {
        public final SqlSetOperator standard;

        BalancedSetOperator(SqlSetOperator standard) {
            // getLeftPrec() is the precedence of a left-associative operator
            super(standard.getName(), standard.getKind(), standard.getLeftPrec(), standard.isAll(),
                    Objects.requireNonNull(standard.getReturnTypeInference()),
                    standard.getOperandTypeInference(), new AssociativeColumnTypes());
            this.standard = standard;
        }

        /** {@link SqlOperator#deriveType} without its lookup in the operator table,
         * which would replace this operator in the call with 'standard' */
        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            for (SqlNode operand : call.getOperandList())
                validator.deriveType(scope, operand);
            RelDataType type = this.validateOperands(validator, scope, call);
            type = this.adjustType(validator, call, type);
            SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type);
            return type;
        }
    }

    static final List<BalancedSetOperator> BALANCED = List.of(
            new BalancedSetOperator(SqlStdOperatorTable.UNION),
            new BalancedSetOperator(SqlStdOperatorTable.UNION_ALL),
            new BalancedSetOperator(SqlStdOperatorTable.INTERSECT),
            new BalancedSetOperator(SqlStdOperatorTable.INTERSECT_ALL));

    /** The operator of the balanced calls that replace calls to 'operator',
     * or null if 'operator' is not an associative set operator */
    static @Nullable BalancedSetOperator balanced(SqlOperator operator) {
        for (BalancedSetOperator balanced : BALANCED) {
            if (balanced.standard == operator)
                return balanced;
        }
        return null;
    }

    /** The operands of the chain of calls to the operator of 'call', left to right, without recursion. */
    static List<SqlNode> chainOperands(SqlCall call) {
        SqlOperator operator = call.getOperator();
        List<SqlNode> operands = new ArrayList<>();
        Deque<SqlNode> toVisit = new ArrayDeque<>();
        toVisit.push(call);
        while (!toVisit.isEmpty()) {
            SqlNode node = toVisit.pop();
            if (node instanceof SqlCall nodeCall && nodeCall.getOperator() == operator) {
                List<SqlNode> nodeOperands = nodeCall.getOperandList();
                for (int i = nodeOperands.size() - 1; i >= 0; i--)
                    toVisit.push(nodeOperands.get(i));
            } else {
                operands.add(node);
            }
        }
        return operands;
    }

    /** A balanced tree of calls to 'operator' that combines operands[start, end) */
    static SqlNode balance(SqlOperator operator, List<SqlNode> operands, int start, int end) {
        if (end - start == 1)
            return operands.get(start);
        int middle = (start + end + 1) / 2;
        SqlNode left = balance(operator, operands, start, middle);
        SqlNode right = balance(operator, operands, middle, end);
        return operator.createCall(ExtendedSqlParserPos.span(left, right), left, right);
    }

    @Override
    public @Nullable SqlNode visit(SqlCall call) {
        BalancedSetOperator balanced = balanced(call.getOperator());
        if (balanced == null)
            return super.visit(call);
        List<SqlNode> operands = chainOperands(call);
        if (operands.size() <= MAX_CHAIN)
            return super.visit(call);
        List<SqlNode> visited = new ArrayList<>(operands.size());
        for (SqlNode operand : operands)
            visited.add(Objects.requireNonNull(operand.accept(this)));
        return balance(balanced, visited, 0, visited.size());
    }

    /** Rebuilds each balanced tree as the left-deep chain of the standard operator */
    static class LeftDeep extends SqlShuttle {
        @Override
        public @Nullable SqlNode visit(SqlCall call) {
            if (!(call.getOperator() instanceof BalancedSetOperator balanced))
                return super.visit(call);
            List<SqlNode> operands = chainOperands(call);
            SqlNode result = Objects.requireNonNull(operands.get(0).accept(this));
            for (int i = 1; i < operands.size(); i++) {
                SqlNode operand = Objects.requireNonNull(operands.get(i).accept(this));
                result = balanced.standard.createCall(ExtendedSqlParserPos.span(result, operand), result, operand);
            }
            return result;
        }
    }

    /** 'node' with every balanced tree in the left-deep form that the parser builds */
    public static SqlNode leftDeep(SqlNode node) {
        return Objects.requireNonNull(new LeftDeep().visitNode(node));
    }
}
