package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.type.OperandTypes.family;
import static org.apache.calcite.sql.type.ReturnTypes.ARG1;

/** Several functions that we define and add to the existing ones. */
public class CustomFunctions {
    private final List<SqlFunction> initial = new ArrayList<>();
    private final HashMap<String, ExternalFunction> udf;

    public CustomFunctions() {
        this.initial.add(new RlikeFunction());
        this.initial.add(new GunzipFunction());
        this.initial.add(new WriteLogFunction());
        this.initial.add(new SequenceFunction());
        this.udf = new HashMap<>();
    }

    /** RLIKE used as a function.  RLIKE in SQL uses infix notation */
    static class RlikeFunction extends SqlFunction {
        public RlikeFunction() {
            super("RLIKE",
                    SqlKind.RLIKE,
                    ReturnTypes.BOOLEAN_NULLABLE,
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
     * GUNZIP(binary) returns the string that results from decompressing the
     * input binary using the GZIP algorithm.  The input binary must be a
     * valid GZIP binary string.
     */
    public static class GunzipFunction extends SqlFunction {
        public GunzipFunction() {
            super("GUNZIP",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.VARCHAR
                            .andThen(SqlTypeTransforms.TO_NULLABLE),
                    null,
                    OperandTypes.BINARY,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
        }

        @Override
        public boolean isDeterministic() {
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

    /**
     * SEQUENCE(start, end) returns an array of integers from start to end (inclusive).
     * The array is empty if start > end.
     */
    public static class SequenceFunction extends SqlFunction {
        public SequenceFunction() {
            super("SEQUENCE",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER
                            .andThen(SqlTypeTransforms.TO_ARRAY)
                            .andThen(SqlTypeTransforms.TO_NULLABLE),
                    null,
                    family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }
    }

    /** A variant ot OperandTypes.TypeNameChecker which does not extend the interface
     * ImplicitCastOperandTypeChecker.  We do not want to allow implicit casts for these operands. */
    private static class ExactTypeNameChecker implements SqlSingleOperandTypeChecker {
        final SqlTypeName typeName;

        ExactTypeNameChecker(SqlTypeName typeName) {
            this.typeName = requireNonNull(typeName, "typeName");
        }

        @Override public boolean checkSingleOperandType(SqlCallBinding callBinding,
                                                        SqlNode operand, int iFormalOperand, boolean throwOnFailure) {
            final RelDataType operandType =
                    callBinding.getValidator().getValidatedNodeType(operand);
            return operandType.getSqlTypeName() == typeName;
        }

        @Override public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName + "(" + typeName.getSpaceName() + ")";
        }
    }

    public static class ExternalFunction extends SqlFunction {
        public final List<RelDataTypeField> parameterList;

        static SqlOperandTypeChecker createTypeChecker(String function, List<RelDataTypeField> parameters) {
            if (parameters.isEmpty())
                return OperandTypes.NILADIC;
            SqlSingleOperandTypeChecker[] checkers = new SqlSingleOperandTypeChecker[parameters.size()];
            StringBuilder builder = new StringBuilder();
            builder.append(function).append("(");
            for (int i = 0; i < parameters.size(); i++) {
                RelDataTypeField field = parameters.get(i);
                SqlTypeName typeName = field.getType().getSqlTypeName();
                // This type checker is a bit too strict.
                checkers[i] = new ExactTypeNameChecker(typeName);
                // This type checker allows implicit casts from any type in the type family,
                // which is not great.
                // checkers[i] = OperandTypes.typeName(typeName);
                if (i > 0)
                    builder.append(", ");
                builder.append("<")
                        .append(field.getType().toString())
                        .append(">");
            }
            builder.append(")");
            return OperandTypes.sequence(builder.toString(), checkers);
        }

        static SqlOperandTypeInference createTypeinference(List<RelDataTypeField> parameters) {
            return InferTypes.explicit(Linq.map(parameters, RelDataTypeField::getType));
        }

        public ExternalFunction(SqlIdentifier name, RelDataType returnType,
                                List<RelDataTypeField> parameters) {
            super(name.getSimple(), SqlKind.OTHER_FUNCTION, ReturnTypes.explicit(returnType),
                    createTypeinference(parameters), createTypeChecker(name.getSimple(), parameters),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
            this.parameterList = parameters;
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.of(this.parameterList.size());
        }
    }

    /**
     * Create a new user-defined function with a specified signature.
     *
     * @param name       Function name.
     * @param signature  Description of arguments as a struct.
     * @param returnType Return type of function.
     */
    public SqlFunction createUDF(CalciteObject node, SqlIdentifier name, RelDataType signature, RelDataType returnType) {
        List<RelDataTypeField> parameterList = signature.getFieldList();
        ExternalFunction result = new ExternalFunction(name, returnType, parameterList);
        String functionName = result.getName();
        if (this.udf.containsKey(functionName)) {
            throw new CompilationError("Function with name " +
                    Utilities.singleQuote(functionName) + " already exists", node);
        }
        Utilities.putNew(this.udf, functionName, result);
        return result;
    }

    @Nullable
    public ExternalFunction getImplementation(String function) {
        return this.udf.get(function);
    }

    /** Return the list custom functions we added to the library. */
    public List<SqlFunction> getInitialFunctions() {
        return this.initial;
    }
}
