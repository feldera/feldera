package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Optionality;

import java.util.List;

/** The ARRAY_AGG aggregate function.  Same as Calcite's
 * {@link org.apache.calcite.sql.fun.SqlLibraryOperators#ARRAY_AGG}, except that
 * {@link #skipsNullInputs()} is false: the array keeps the NULL values. */
public class SqlArrayAggFunction extends SqlAggFunction {
    public static final SqlArrayAggFunction INSTANCE = new SqlArrayAggFunction();

    private SqlArrayAggFunction() {
        super("ARRAY_AGG", null, SqlKind.ARRAY_AGG,
                ReturnTypes.andThen(ReturnTypes::stripOrderBy, ReturnTypes.TO_ARRAY_NULLABLE),
                null, OperandTypes.ANY, SqlFunctionCategory.SYSTEM,
                false, false, Optionality.FORBIDDEN);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.ORDERED_FUNCTION;
    }

    @Override
    public boolean allowsNullTreatment() {
        return true;
    }

    @Override
    public boolean skipsNullInputs() {
        return false;
    }

    /** The parser stores the ORDER BY clause as the last operand; it is not an argument */
    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        SqlCall strippedCall = ReturnTypes.stripOrderBy(call);
        RelDataType type = super.deriveType(validator, scope, strippedCall);
        // The validator can replace the arguments with casts; copy them back to the call
        List<SqlNode> arguments = strippedCall.getOperandList();
        for (int i = 0; i < arguments.size(); i++)
            call.setOperand(i, arguments.get(i));
        return type;
    }
}
