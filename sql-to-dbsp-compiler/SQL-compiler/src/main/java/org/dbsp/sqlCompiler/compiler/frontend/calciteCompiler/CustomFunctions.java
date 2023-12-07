package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.type.OperandTypes.family;
import static org.apache.calcite.sql.type.ReturnTypes.ARG1;

/** Several functions that we define and add to the existing ones. */
public class CustomFunctions {
    public static final CustomFunctions INSTANCE = new CustomFunctions();
    private final List<SqlFunction> functions = new ArrayList<>();

    private CustomFunctions() {
        this.functions.add(new SqlDivisionFunction());
        this.functions.add(new RlikeFunction());
        this.functions.add(new WriteLogFunction());
    }

    static class RlikeFunction extends SqlFunction {
        public RlikeFunction() {
            super("RLIKE",
                    SqlKind.RLIKE,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.STRING_STRING,
                    SqlFunctionCategory.STRING);
        }

        @Override
        public boolean isDeterministic() {
            // TODO: change this when we learn how to constant-fold in the RexToLixTranslator
            return false;
        }
    }

    /**
     * WRITELOG(format, arg) returns its argument 'arg' unchanged but also logs
     * its value to stdout.  Used for debugging.  In the format string
     * each occurrence of %% is replaced with the arg */
    public static class WriteLogFunction extends SqlFunction {
        public WriteLogFunction() {
            super("WRITELOG",
                    SqlKind.OTHER_FUNCTION,
                    ARG1,
                    null,
                    family(SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }
    }

    static class SqlDivisionFunction extends SqlFunction {
        // Custom implementation of type inference DIVISION for our division operator.
        static final SqlReturnTypeInference divResultInference = new SqlReturnTypeInference() {
            @Override
            public @org.checkerframework.checker.nullness.qual.Nullable
            RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                // Default policy for division.
                RelDataType result = ReturnTypes.QUOTIENT_NULLABLE.inferReturnType(opBinding);
                List<RelDataType> opTypes = opBinding.collectOperandTypes();
                // If all operands are integer or decimal, result is nullable
                // otherwise it's not.
                boolean nullable = true;
                for (RelDataType type: opTypes) {
                    if (SqlTypeName.APPROX_TYPES.contains(type.getSqlTypeName())) {
                        nullable = false;
                        break;
                    }
                }
                if (nullable)
                    result = opBinding.getTypeFactory().createTypeWithNullability(result, true);
                return result;
            }
        };

        public SqlDivisionFunction() {
            super("DIVISION",
                    SqlKind.OTHER_FUNCTION,
                    divResultInference,
                    null,
                    OperandTypes.NUMERIC_NUMERIC,
                    SqlFunctionCategory.NUMERIC);
        }

        @Override
        public boolean isDeterministic() {
            // TODO: change this when we learn how to constant-fold in the RexToLixTranslator
            // https://issues.apache.org/jira/browse/CALCITE-3394 may give a solution
            return false;
        }
    }

    /**
     * Create a new user-defined function with a specified signature.
     * @param name         Function name.
     * @param signature    Description of arguments as a struct.
     * @param returnType   Return type of function.
     */
    public SqlFunction createUDF(String name, RelDataType signature, RelDataType returnType) {
        throw new UnimplementedException();
    }

    /** Return the list of user-defined functions */
    public List<SqlFunction> getUDFs() {
        return this.functions;
    }
}
