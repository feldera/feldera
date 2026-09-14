package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

/** Calcite-level implementation of the ARRAY_FILTER function, which keeps the
 * array elements for which a predicate lambda returns true */
class ArrayFilterFunction extends CustomFunctions.NonOptimizedFunction {
    private ArrayFilterFunction() {
        super("ARRAY_FILTER",
                FILTER_INFERENCE,
                new ArrayLambdaOperandTypeChecker(
                        "ARRAY_FILTER(<ARRAY>, <FUNCTION(ARRAY_ELEMENT_TYPE)->BOOLEAN>)",
                        SqlTypeName.BOOLEAN),
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                "array#array_filter", FunctionDocumentation.NO_FILE);
    }

    /** The input array type.  An input without a component type is an untyped NULL
     * literal, or a parameter of an enclosing lambda whose type is not yet inferred
     * (ANY); the result is then an array of that type, so that it still type-checks
     * as an array in the enclosing expression. */
    static final SqlReturnTypeInference FILTER_INFERENCE = opBinding -> {
        RelDataType arrayType = opBinding.getOperandType(0);
        if (arrayType.getComponentType() == null)
            return new ArraySqlType(arrayType, arrayType.isNullable());
        return arrayType;
    };

    // Must follow FILTER_INFERENCE: static initializers run in textual order
    static final ArrayFilterFunction INSTANCE = new ArrayFilterFunction();
}
