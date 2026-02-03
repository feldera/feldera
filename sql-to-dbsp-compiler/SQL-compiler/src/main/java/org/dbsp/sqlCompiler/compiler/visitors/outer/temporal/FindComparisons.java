package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Break a Boolean expression into a series of conjunctions, some of which
 * may be implemented as temporal filters.
 */
class FindComparisons {
    final DBSPParameter parameter;
    /**
     * List of Boolean expressions; a NonTemporalComparison may appear in the last position only
     */
    final List<BooleanExpression> comparisons;
    final MonotoneTransferFunctions mono;
    final ContainsNow containsNow;
    final Set<DBSPOpcode> compOpcodes;

    FindComparisons(DBSPClosureExpression closure, MonotoneTransferFunctions mono) {
        this.mono = mono;
        this.comparisons = new ArrayList<>();
        this.containsNow = new ContainsNow(mono.compiler, true);
        this.compOpcodes = new HashSet<>() {{
            add(DBSPOpcode.LT);
            add(DBSPOpcode.LTE);
            add(DBSPOpcode.GTE);
            add(DBSPOpcode.GT);
            add(DBSPOpcode.EQ);
        }};

        DBSPClosureExpression clo = closure.to(DBSPClosureExpression.class);
        Utilities.enforce(clo.parameters.length == 1);
        this.parameter = clo.parameters[0];

        DBSPExpression expression = clo.body;
        if (expression.is(DBSPUnaryExpression.class)) {
            DBSPUnaryExpression unary = expression.to(DBSPUnaryExpression.class);
            // If the filter is wrap_bool(expression), analyze expression
            if (unary.opcode == DBSPOpcode.WRAP_BOOL)
                expression = unary.source;
        }
        this.analyzeConjunction(expression);
    }

    /** Analyze a filter expression from a filter operator.
     * @param function A function to decompose.  May not be exactly
     *                 the closure of the operator -- it may be rewritten as a tree.
     * @return A decomposition of the function into a list of comparisons */
    public static List<BooleanExpression> decomposeIntoTemporalFilters(
            DBSPCompiler compiler, DBSPFilterOperator operator, DBSPClosureExpression function) {
        Utilities.enforce(function.parameters.length == 1);
        DBSPParameter param = function.parameters[0];
        IMaybeMonotoneType nonMonotone = NonMonotoneType.nonMonotone(param.getType().deref());
        MonotoneTransferFunctions mono = new MonotoneTransferFunctions(
                compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, nonMonotone);
        mono.apply(function);

        FindComparisons analyzer = new FindComparisons(function, mono);
        return analyzer.comparisons;
    }

    BooleanExpression nonTemporal(DBSPExpression expression) {
        this.containsNow.apply(expression);
        if (this.containsNow.found)
            return new NonTemporalFilter(expression);
        return new NoNow(expression);
    }

    /**
     * Analyze a conjunction; return 'false' if some NonTemporalFilters were found.
     * When this function returns, this.comparisons contains a full
     * decomposition of 'expression'.
     */
    boolean analyzeConjunction(DBSPExpression expression) {
        DBSPBinaryExpression binary = expression.as(DBSPBinaryExpression.class);
        if (binary == null) {
            this.comparisons.add(this.nonTemporal(expression));
            return false;
        }
        if (binary.opcode == DBSPOpcode.AND) {
            boolean foundLeft = this.analyzeConjunction(binary.left);
            if (!foundLeft) {
                BooleanExpression last = Utilities.removeLast(this.comparisons);
                BooleanExpression right = this.nonTemporal(binary.right);
                if (last.is(NonTemporalFilter.class) ||
                        (last.is(NoNow.class) && right.is(NoNow.class)))
                    this.comparisons.add(last.combine(right));
                else {
                    this.comparisons.add(last);
                    this.comparisons.add(right);
                }
                return false;
            }
            return this.analyzeConjunction(binary.right);
        } else {
            boolean decomposed = this.findComparison(binary);
            if (!decomposed) {
                BooleanExpression be = this.nonTemporal(expression);
                this.comparisons.add(be);
            }
            return decomposed;
        }
    }

    /**
     * See if a binary expression can be implemented as a temporal filter.
     * Return 'false' if the expression contains now() but is not a temporal filter.
     * If it returns 'true', the expression is added to this.comparisons.
     */
    boolean findComparison(DBSPBinaryExpression binary) {
        this.containsNow.apply(binary);
        if (!this.containsNow.found) {
            NoNow expression = new NoNow(binary);
            this.comparisons.add(expression);
            return true;
        }

        if (!this.compOpcodes.contains(binary.opcode))
            return false;
        this.containsNow.apply(binary.left);
        boolean leftHasNow = this.containsNow.found;
        this.containsNow.apply(binary.right);
        boolean rightHasNow = this.containsNow.found;
        if (leftHasNow == rightHasNow) {
            // Both true or both false
            return false;
        }

        DBSPExpression withNow = leftHasNow ? binary.left : binary.right;
        DBSPExpression withoutNow = leftHasNow ? binary.right : binary.left;

        // The expression containing now() must be monotone
        MonotoneExpression me = this.mono.maybeGet(withNow);
        if (me == null || !me.mayBeMonotone())
            return false;

        DBSPOpcode opcode = leftHasNow ? RewriteNow.inverse(binary.opcode) : binary.opcode;
        TemporalFilter comp = new TemporalFilter(this.parameter, withoutNow, withNow, opcode);
        this.comparisons.add(comp);
        return true;
    }
}
