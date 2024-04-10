package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

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

    /** SEQUENCE(start, end) returns an array of integers from start to end (inclusive).
     * The array is empty if start > end. */
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

    /**
     * Create a new user-defined function with a specified signature.
     *
     * @param name       Function name.
     * @param signature  Description of arguments as a struct.
     * @param returnType Return type of function.
     */
    public ExternalFunction createUDF(CalciteObject node, SqlIdentifier name, RelDataType signature, RelDataType returnType) {
        List<RelDataTypeField> parameterList = signature.getFieldList();
        String functionName = name.getSimple();
        boolean generated = functionName.toLowerCase(Locale.ENGLISH).startsWith("jsonstring_as_");
        ExternalFunction result = new ExternalFunction(name, returnType, parameterList, generated);
        if (this.udf.containsKey(functionName)) {
            throw new CompilationError("Function with name " +
                    Utilities.singleQuote(functionName) + " already exists", node);
        }
        Utilities.putNew(this.udf, functionName, result);
        return result;
    }

    @Nullable
    public ExternalFunction getSignature(String function) {
        return this.udf.get(function);
    }

    /** Return the list custom functions we added to the library. */
    public List<SqlFunction> getInitialFunctions() {
        return this.initial;
    }
}
