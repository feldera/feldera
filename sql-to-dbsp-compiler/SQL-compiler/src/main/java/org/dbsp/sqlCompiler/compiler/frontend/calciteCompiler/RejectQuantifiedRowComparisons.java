package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.RejectUnsupportedPlans;
import org.dbsp.util.Utilities;

import java.util.function.UnaryOperator;

/** Rejects {@code IN}, {@code NOT IN}, and the quantified comparisons with {@code SOME},
 * {@code ANY}, or {@code ALL} whose left operand is a ROW value, such as {@code (a, b)}.
 * The conversion to relational algebra splits these comparisons into comparisons of the
 * fields, so the check runs on the validated statement. */
public class RejectQuantifiedRowComparisons extends SqlBasicVisitor<Void> {
    private final SqlValidator validator;
    /** Maps a position in the statement to the position reported to the user */
    private final UnaryOperator<SqlParserPos> sourcePositionRemap;

    public RejectQuantifiedRowComparisons(
            SqlValidator validator, UnaryOperator<SqlParserPos> sourcePositionRemap) {
        this.validator = validator;
        this.sourcePositionRemap = sourcePositionRemap;
    }

    /** True if the expression is a row constructor or has a ROW type */
    boolean isRow(SqlNode expression) {
        if (expression.getKind() == SqlKind.ROW)
            return true;
        RelDataType type = this.validator.getValidatedNodeTypeIfKnown(expression);
        return type != null && type.isStruct();
    }

    @Override
    public Void visit(SqlCall call) {
        if (call.getOperator() instanceof SqlInOperator in && this.isRow(call.operand(0))) {
            String rewrite = call.getKind() == SqlKind.NOT_IN || call.getKind() == SqlKind.ALL ?
                    "NOT EXISTS" : "EXISTS";
            SourcePositionRange range = new SourcePositionRange(
                    this.sourcePositionRemap.apply(call.getParserPosition()));
            // Throw: Calcite cannot convert some of these comparisons
            throw new CompilationError(range, RejectUnsupportedPlans.ERROR_KIND,
                    "ROW values cannot be compared using " + Utilities.singleQuote(in.getName()) +
                            "; consider using " + Utilities.singleQuote(rewrite) + " instead.\n" +
                            RejectUnsupportedPlans.ROW_DOCUMENTATION.citation());
        }
        return super.visit(call);
    }
}
