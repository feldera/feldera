package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
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
    private static final List<NonOptimizedFunction> initial = new ArrayList<>() {{
        add(RlikeFunction.INSTANCE);
        add(GunzipFunction.INSTANCE);
        add(WriteLogFunction.INSTANCE);
        add(SequenceFunction.INSTANCE);
        add(ToIntFunction.INSTANCE);
        add(NowFunction.INSTANCE);
        add(ParseJsonFunction.INSTANCE);
        add(ToJsonFunction.INSTANCE);
        add(BlackboxFunction.INSTANCE);
        add(ParseTimeFunction.INSTANCE);
        add(ParseDateFunction.INSTANCE);
        add(ParseTimestampFunction.INSTANCE);
        add(FormatDateFunction.INSTANCE);
        add(ArrayExcept.INSTANCE);
        add(ArrayUnion.INSTANCE);
        add(ArrayIntersect.INSTANCE);
    }};

    private final List<NonOptimizedFunction> functions;
    private final HashMap<ProgramIdentifier, ExternalFunction> udf;

    public CustomFunctions() {
        this.functions = initial;
        this.udf = new HashMap<>();
    }

    /** Make a copy of the other object */
    public CustomFunctions(CustomFunctions other) {
        this.functions = new ArrayList<>(other.functions);
        this.udf = new HashMap<>(other.udf);
    }

    public static List<? extends FunctionDocumentation.FunctionDescription> getInitialDescriptions() {
        return CustomFunctions.initial;
    }

    /** Function that has no implementation for the optimizer */
    static abstract class NonOptimizedFunction extends SqlFunction
        implements FunctionDocumentation.FunctionDescription
    {
        final String documentationFile;

        public NonOptimizedFunction(
                String name, SqlKind kind,
                @org.checkerframework.checker.nullness.qual.Nullable SqlReturnTypeInference returnTypeInference,
                @org.checkerframework.checker.nullness.qual.Nullable SqlOperandTypeChecker operandTypeChecker,
                SqlFunctionCategory category,
                String documentationFile) {
            super(name, kind, returnTypeInference, null, operandTypeChecker, category);
            this.documentationFile = documentationFile;
        }

        public NonOptimizedFunction(
                String name,
                @org.checkerframework.checker.nullness.qual.Nullable SqlReturnTypeInference returnTypeInference,
                @org.checkerframework.checker.nullness.qual.Nullable SqlOperandTypeChecker operandTypeChecker,
                SqlFunctionCategory category,
                String documentationFile) {
            super(name, SqlKind.OTHER_FUNCTION, returnTypeInference,
                    null, operandTypeChecker, category);
            this.documentationFile = documentationFile;
        }

        @Override
        public boolean isDeterministic() {
            // Pretend that the function is not deterministic, so that the constant
            // folding code never tries to optimize it.
            return false;
        }

        @Override
        public String functionName() {
            return this.getName();
        }

        @Override
        public String documentation() {
            return this.documentationFile;
        }

        @Override
        public boolean aggregate() {
            return false;
        }
    }

    /** A clone of a Calcite SqlLibraryOperator function, but which is non-optimized */
    static abstract class CalciteFunctionClone extends NonOptimizedFunction {
        public CalciteFunctionClone(SqlFunction calciteFunction, String documentationFile) {
            super(calciteFunction.getName(), calciteFunction.kind,
                    calciteFunction.getReturnTypeInference(), calciteFunction.getOperandTypeChecker(),
                    calciteFunction.getFunctionType(), documentationFile);
        }
    }

    static class FormatDateFunction extends CalciteFunctionClone {
        private FormatDateFunction() {
            super(SqlLibraryOperators.FORMAT_DATE, "datetime");
        }

        public static final FormatDateFunction INSTANCE = new FormatDateFunction();
    }

    static class ParseJsonFunction extends NonOptimizedFunction {
        private ParseJsonFunction() {
            super("PARSE_JSON",
                    ReturnTypes.VARIANT.andThen(SqlTypeTransforms.TO_NULLABLE),
                    OperandTypes.STRING,
                    SqlFunctionCategory.STRING, "json");
        }

        public static final ParseJsonFunction INSTANCE = new ParseJsonFunction();
    }

    static class ToJsonFunction extends NonOptimizedFunction {
        private ToJsonFunction() {
            super("TO_JSON",
                    ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE),
                    OperandTypes.VARIANT,
                    SqlFunctionCategory.STRING, "json");
        }

        public static final ToJsonFunction INSTANCE = new ToJsonFunction();
    }

    /** Similar to PARSE_TIME in Calcite, but always nullable */
    static class ParseTimeFunction extends NonOptimizedFunction {
        private ParseTimeFunction() {
            super("PARSE_TIME", ReturnTypes.TIME.andThen(SqlTypeTransforms.FORCE_NULLABLE),
                    OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE, "datetime");
        }

        public static final ParseTimeFunction INSTANCE = new ParseTimeFunction();
    }

    /** Similar to PARSE_DATE in Calcite, but always nullable */
    static class ParseDateFunction extends NonOptimizedFunction {
        private ParseDateFunction() {
            super("PARSE_DATE", ReturnTypes.DATE.andThen(SqlTypeTransforms.FORCE_NULLABLE),
                    OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE, "datetime");
        }

        public static final ParseDateFunction INSTANCE = new ParseDateFunction();
    }

    /* Similar to PARSE_TIMESTAMP in Calcite, but always nullable */
    static class ParseTimestampFunction extends NonOptimizedFunction {
        private ParseTimestampFunction() {
            super("PARSE_TIMESTAMP", ReturnTypes.TIMESTAMP.andThen(SqlTypeTransforms.FORCE_NULLABLE),
                    OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE, "datetime");
        }

        public static final ParseTimestampFunction INSTANCE = new ParseTimestampFunction();
    }

    /** RLIKE used as a function.  RLIKE in SQL uses infix notation */
    static class RlikeFunction extends NonOptimizedFunction {
        private RlikeFunction() {
            super("RLIKE",
                    SqlKind.RLIKE,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    OperandTypes.STRING_STRING,
                    SqlFunctionCategory.STRING, "string");
        }

        public static final RlikeFunction INSTANCE = new RlikeFunction();
    }

    static class NowFunction extends NonOptimizedFunction {
        private NowFunction() {
            super("NOW",
                    ReturnTypes.TIMESTAMP,
                    OperandTypes.NILADIC,
                    SqlFunctionCategory.TIMEDATE, "datetime");
        }

        public static final NowFunction INSTANCE = new NowFunction();
    }

    /** GUNZIP(binary) returns the string that results from decompressing the
     * input binary using the GZIP algorithm.  The input binary must be a
     * valid GZIP binary string. */
    static class GunzipFunction extends NonOptimizedFunction {
        private GunzipFunction() {
            super("GUNZIP",
                    ReturnTypes.VARCHAR
                            .andThen(SqlTypeTransforms.TO_NULLABLE),
                    OperandTypes.BINARY,
                    SqlFunctionCategory.STRING, "binary");
        }

        public static final GunzipFunction INSTANCE = new GunzipFunction();
    }

    /** WRITELOG(format, arg) returns its argument 'arg' unchanged but also logs
     * its value to stdout.  Used for debugging.  In the format string
     * each occurrence of %% is replaced with the arg */
    static class WriteLogFunction extends NonOptimizedFunction {
        private WriteLogFunction() {
            super("WRITELOG",
                    ARG1,
                    family(SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION, "");
        }

        public static final WriteLogFunction INSTANCE = new WriteLogFunction();
    }

    /** SEQUENCE(start, end) returns an array of integers from start to end (inclusive).
     * The array is empty if start > end. */
    static class SequenceFunction extends NonOptimizedFunction {
        private SequenceFunction() {
            super("SEQUENCE",
                    ReturnTypes.INTEGER
                            .andThen(SqlTypeTransforms.TO_ARRAY)
                            .andThen(SqlTypeTransforms.TO_NULLABLE),
                    family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION, "integer");
        }

        public static final SequenceFunction INSTANCE = new SequenceFunction();
    }

    /** TO_INT(BINARY) returns an integers from a BINARY object which has less than 4 bytes.
     * For VARBINARY objects it converts only the first 4 bytes. */
    static class ToIntFunction extends NonOptimizedFunction {
        private ToIntFunction() {
            super("TO_INT",
                    ReturnTypes.INTEGER
                            .andThen(SqlTypeTransforms.TO_NULLABLE),
                    OperandTypes.BINARY,
                    SqlFunctionCategory.NUMERIC, "binary");
        }

        public static final ToIntFunction INSTANCE = new ToIntFunction();
    }

    static class BlackboxFunction extends NonOptimizedFunction {
        private BlackboxFunction() {
            super("BLACKBOX",
                    ReturnTypes.ARG0,
                    OperandTypes.ANY,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION, "");
        }

        public static final BlackboxFunction INSTANCE = new BlackboxFunction();
    }

    // This function is non-deterministic in Calcite, since it does not
    // establish the order of elements in the result.
    static class ArrayExcept extends CalciteFunctionClone {
        private ArrayExcept() {
            super(SqlLibraryOperators.ARRAY_EXCEPT, "array");
        }
        public static final ArrayExcept INSTANCE = new ArrayExcept();
    }

    // This function is non-deterministic in Calcite, since it does not
    // establish the order of elements in the result.
    static class ArrayUnion extends CalciteFunctionClone {
        private ArrayUnion() {
            super(SqlLibraryOperators.ARRAY_UNION, "array");
        }
        public static final ArrayUnion INSTANCE = new ArrayUnion();
    }

    // This function is non-deterministic in Calcite, since it does not
    // establish the order of elements in the result.
    static class ArrayIntersect extends CalciteFunctionClone {
        private ArrayIntersect() {
            super(SqlLibraryOperators.ARRAY_INTERSECT, "array");
        }
        public static final ArrayIntersect INSTANCE = new ArrayIntersect();
    }

    /**
     * Create a new user-defined function.
     * @param name       Function name.
     * @param signature  Description of arguments as a struct.
     * @param returnType Return type of function.
     * @param body       Optional body of the function.  If missing,
     *                   the function is defined in Rust.
     */
    public ExternalFunction createUDF(CalciteObject node, SqlIdentifier name,
                                      RelDataType signature, RelDataType returnType, @Nullable RexNode body) {
        List<RelDataTypeField> parameterList = signature.getFieldList();
        ProgramIdentifier functionName = Utilities.toIdentifier(name);
        boolean generated = functionName.name().toLowerCase(Locale.ENGLISH).startsWith("jsonstring_as_") || body != null;
        ExternalFunction result = new ExternalFunction(name, returnType, parameterList, body, generated);
        if (this.udf.containsKey(functionName)) {
            throw new CompilationError("Function with name " +
                    functionName.singleQuote() + " already exists", node);
        }
        Utilities.putNew(this.udf, functionName, result);
        return result;
    }

    @Nullable
    public ExternalFunction getSignature(ProgramIdentifier function) {
        return this.udf.get(function);
    }

    /** Return the list custom functions we added to the library. */
    public List<? extends SqlFunction> getInitialFunctions() {
        return this.functions;
    }
}
