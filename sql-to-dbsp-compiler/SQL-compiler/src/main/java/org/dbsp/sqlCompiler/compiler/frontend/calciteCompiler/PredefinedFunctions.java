package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.sql.SqlFunction;

import java.util.List;

/** Registers the predefined functions which do not exist in Calcite */
public class PredefinedFunctions implements FunctionDocumentation.FunctionRegistry {
    private PredefinedFunctions() {}

    record PredefinedFunction(SqlFunction function, String documentation)
            implements FunctionDocumentation.FunctionDescription {
        @Override
        public String functionName() {
            return this.function.getName();
        }

        @Override
        public String documentation() {
            return this.documentation;
        }

        @Override
        public boolean aggregate() {
            return false;
        }
    }

    public static final FunctionDocumentation.FunctionRegistry INSTANCE = new PredefinedFunctions();

    @Override
    public List<FunctionDocumentation.FunctionDescription> getDescriptions() {
        return List.of(
                new PredefinedFunction(CustomFunctions.RlikeFunction.INSTANCE, "string"),
                new PredefinedFunction(CustomFunctions.GunzipFunction.INSTANCE, "binary"),
                new PredefinedFunction(CustomFunctions.SequenceFunction.INSTANCE, "integer"),
                new PredefinedFunction(CustomFunctions.ToIntFunction.INSTANCE, "binary"),
                new PredefinedFunction(CustomFunctions.NowFunction.INSTANCE, "datetime"),
                new PredefinedFunction(CustomFunctions.ParseJsonFunction.INSTANCE, "json"),
                new PredefinedFunction(CustomFunctions.ToJsonFunction.INSTANCE, "json"),
                new PredefinedFunction(CustomFunctions.BlackboxFunction.INSTANCE, ""),
                new PredefinedFunction(CustomFunctions.ParseTimeFunction.INSTANCE, "datetime"),
                new PredefinedFunction(CustomFunctions.ParseDateFunction.INSTANCE, "datetime"),
                new PredefinedFunction(CustomFunctions.ParseTimestampFunction.INSTANCE, "datetime")
        );
    }
}
