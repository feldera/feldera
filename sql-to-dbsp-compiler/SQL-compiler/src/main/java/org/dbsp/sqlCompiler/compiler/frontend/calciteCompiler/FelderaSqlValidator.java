package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.RejectUnsupportedPlans;
import org.dbsp.util.Utilities;

import java.util.Locale;
import java.util.function.UnaryOperator;

/** Extension of Calcite's validator. 
 * Rejects these comparisons with a ROW value:
 * <ul>
 *   <li>{@code IN}, {@code NOT IN}, and the comparisons with {@code SOME}, {@code ANY},
 *   or {@code ALL} whose left operand is a ROW value, such as {@code (a, b)};</li>
 *   <li>comparisons between a ROW value and a scalar sub-query.</li>
 * </ul>
 */
public class FelderaSqlValidator extends SqlValidatorImpl {
    /** Maps a position in the statement to the position reported to the user */
    private final UnaryOperator<SqlParserPos> sourcePositionRemap;

    public FelderaSqlValidator(
            SqlOperatorTable operatorTable, SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory, Config config,
            UnaryOperator<SqlParserPos> sourcePositionRemap) {
        super(operatorTable, catalogReader, typeFactory, config);
        this.sourcePositionRemap = sourcePositionRemap;
    }

    /** True if the expression has a ROW type */
    private boolean isRow(SqlValidatorScope scope, SqlNode expression) {
        return this.deriveType(scope, expression).isStruct();
    }

    /** The position of 'node' in the program that the user wrote */
    private SourcePositionRange getPositionRange(SqlNode node) {
        return new SourcePositionRange(this.sourcePositionRemap.apply(node.getParserPosition()));
    }

    private CompilationError rowComparisonError(SqlCall call, String message) {
        return new CompilationError(this.getPositionRange(call), RejectUnsupportedPlans.ERROR_KIND,
                message + ".\n" + RejectUnsupportedPlans.ROW_DOCUMENTATION.citation());
    }

    /** Throw if the left operand of IN, NOT IN, SOME, or ALL is a ROW value */
    private void checkQuantified(SqlValidatorScope scope, SqlCall call, SqlInOperator operator) {
        if (!this.isRow(scope, call.operand(0)))
            return;
        String rewrite = call.getKind() == SqlKind.NOT_IN || call.getKind() == SqlKind.ALL ?
                "NOT EXISTS" : "EXISTS";
        throw this.rowComparisonError(call, "ROW values cannot be compared using " +
                Utilities.singleQuote(operator.getName()) + "; consider using " +
                Utilities.singleQuote(rewrite) + " instead");
    }

    /** Throw if a comparison that Feldera rejects on ROW values compares a ROW value
     * with a scalar sub-query */
    private void checkScalarQuery(SqlValidatorScope scope, SqlCall call) {
        SqlNode left = call.operand(0);
        SqlNode right = call.operand(1);
        SqlNode other;
        if (left.getKind() == SqlKind.SCALAR_QUERY)
            other = right;
        else if (right.getKind() == SqlKind.SCALAR_QUERY)
            other = left;
        else
            return;
        String operator = Utilities.singleQuote(call.getOperator().getName().toUpperCase(Locale.ROOT));
        String message = RejectUnsupportedPlans.rowComparisonMessage(call.getKind(), operator, true);
        if (message != null && this.isRow(scope, other))
            throw this.rowComparisonError(call, message);
    }

    /** Throw if 'node' is one of the rejected comparisons with a ROW value */
    private void checkRowComparison(SqlValidatorScope scope, SqlNode node) {
        if (node instanceof SqlCall call) {
            if (call.getOperator() instanceof SqlInOperator operator)
                this.checkQuantified(scope, call, operator);
            else if (call.getKind().belongsTo(SqlKind.BINARY_COMPARISON))
                this.checkScalarQuery(scope, call);
        }
    }

    // Calcite derives the types of the operands of a comparison while it infers the
    // types of unknown operands, before it derives the type of the comparison
    @Override
    protected void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
        this.checkRowComparison(scope, node);
        super.inferUnknownTypes(inferredType, scope, node);
    }

    @Override
    public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
        this.checkRowComparison(scope, expr);
        return super.deriveType(scope, expr);
    }
}
