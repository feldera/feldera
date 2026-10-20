package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.util.Utilities;

/** A RexBuilder that builds expressions with Feldera's semantics where Calcite's
 * RexBuilder would build expressions that differ from the Feldera runtime. */
public class FelderaRexBuilder extends RexBuilder {
    public FelderaRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    @Override
    public RexNode makeCast(SqlParserPos pos, RelDataType type, RexNode exp,
                            boolean matchNullability, boolean safe, RexLiteral format) {
        if (format.isNull() && exp instanceof RexLiteral literal && this.keepCast(type, literal))
            // makeAbstractCast returns the CAST call with the literal unconverted
            return this.makeAbstractCast(pos, type, exp, safe, format);
        // super.makeCast may replace the call with a converted literal
        return super.makeCast(pos, type, exp, matchNullability, safe, format);
    }

    /** True for a cast of {@code literal} to {@code type} that Calcite converts differently
     * from the runtime */
    private boolean keepCast(RelDataType type, RexLiteral literal) {
        SqlTypeName to = type.getSqlTypeName();
        SqlTypeName from = literal.getType().getSqlTypeName();
        if ((to == SqlTypeName.TIME || to == SqlTypeName.TIMESTAMP)
                && SqlTypeUtil.isCharacter(literal.getType())) {
            // For a string with more fractional seconds than the precision of the type
            // Calcite truncates the excess digits, the runtime rounds them
            NlsString string = literal.getValueAs(NlsString.class);
            return string != null && Utilities.fractionalDigits(string.getValue().trim()) > type.getPrecision();
        }
        if (SqlTypeUtil.isCharacter(type)) {
            // For a value with fractional seconds Calcite generates the significant digits,
            // the runtime generates 9 digits for a TIME and 6 for a TIMESTAMP
            if (from == SqlTypeName.TIME) {
                TimeString time = literal.getValueAs(TimeString.class);
                return time != null && Utilities.hasFraction(time.toString());
            }
            if (from == SqlTypeName.TIMESTAMP) {
                TimestampString timestamp = literal.getValueAs(TimestampString.class);
                return timestamp != null && Utilities.hasFraction(timestamp.toString());
            }
        }
        return false;
    }
}
