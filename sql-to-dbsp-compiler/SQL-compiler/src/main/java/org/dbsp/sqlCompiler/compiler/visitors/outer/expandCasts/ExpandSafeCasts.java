package org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSqlResult;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/**
 * Expand SAFE_CAST invocations that convert complex types to simpler invocations
 * For example, a SAFE_CAST to an ARRAY TYPE is expanded into calls of SAFE_CAST for
 * the array elements, and a call to the array_map function to apply it to all elements.
 */
 /*
 Safe casts are tricky, because a safe cast can expand into a complex expression evaluation,
 and yet only the outermost cast should catch an "exception" and return None.  Here is an example
 of implementing SAFE_CAST(string_array, INT ARRAY):

 handle_error_safe(array_map_safe__(
   array,
   move |p1: &SqlString| -> SqlResult<Option<i32>> {
      cast_to_i32N_s((*p1).clone())
   }
 ))

 The array_map_safe__ function from sqllib takes a function that returns SqlResult, and it returns SqlResult itself.
 This gets more involved when recursively converting nested data structures, such as INT ARRAY ARRAY, since the
 array_map_safe function needs to invoke itself, yet the errors need to be handled only in the outermost layer
 by handle_error_safe. */
public class ExpandSafeCasts extends ExpressionTranslator {
    public ExpandSafeCasts(DBSPCompiler compiler) {
        super(compiler);
    }

    static final DBSPCastExpression.CastType SAFE = DBSPCastExpression.CastType.SqlSafe;
    static final DBSPCastExpression.CastType UNWRAP = SAFE.getUnwrap();

    /** Create a function which converts (safely) values from type {@code source} to type {@code destination}.
     * The function will return SqlResult. */
    DBSPClosureExpression converterFunction(
            CalciteObject node, DBSPType source, DBSPType dest) {
        boolean nullable = dest.mayBeNull;
        Utilities.enforce(!dest.is(DBSPTypeSqlResult.class));
        DBSPType functionResultType = new DBSPTypeSqlResult(dest);

        DBSPVariablePath var = source.ref().var();
        DBSPExpression convertValue = var.deref();
        if (convertValue.getType().is(DBSPTypeBaseType.class))
            convertValue = convertValue.applyCloneIfNeeded();
        // Safe casts require the destination type to always be NULL
        var cast = convertValue.cast(node, dest.withMayBeNull(true), ExpandSafeCasts.SAFE);
        // Recursively apply the translation to the produced cast
        DBSPExpression expanded = this.analyze(cast).to(DBSPExpression.class);

        if (expanded.is(DBSPCastExpression.class)) {
            // The produced result must look like unwrap(cast(...)).
            // Strip the outer unwrap cast
            DBSPCastExpression outer = expanded.to(DBSPCastExpression.class);
            Utilities.enforce(outer.getType().sameType(dest.withMayBeNull(true)));
            Utilities.enforce(outer.safe.isUnwrap());
            expanded = outer.source;
        } else {
            // The conversion didn't require applying any casts, but we still need to produce a SqlResult
            // This function converts T to SqlResult<T> using Ok().
            expanded = new DBSPApplyExpression(node, "wrap_sql_result", functionResultType, expanded);
        }
        if (!nullable) {
            // We have adjusted the result type to be nullable, now we have to strip it out.
            // This function converts SqlResult<Option<T>> to SqlResult<T>
            expanded = new DBSPApplyExpression(node, "unwrap_sql_result", functionResultType, expanded);
        }
        Utilities.enforce(expanded.getType().sameType(functionResultType));
        return expanded.closure(var);
    }

    @Nullable
    DBSPExpression convertToArray(final CalciteObject node, DBSPExpression source, final DBSPTypeArray type) {
        DBSPType sourceType = source.getType();
        if (sourceType.is(DBSPTypeVariant.class)) {
            if (type.getElementType().is(DBSPTypeBaseType.class)) {
                // Default conversion is fine
                return null;
            } else {
                // Convert to a Vector of VARIANT, and then...
                DBSPTypeArray vecVType = new DBSPTypeArray(DBSPTypeVariant.create(false), sourceType.mayBeNull);
                DBSPExpression vecV = source.cast(node, vecVType, SAFE);
                // ...convert each element recursively to the target element type
                DBSPExpression convert = this.converterFunction(
                        node, vecVType.getElementType(), type.getElementType());

                DBSPType arrType = new DBSPTypeArray(type.getElementType(), true);
                source = new DBSPBinaryExpression(node, new DBSPTypeSqlResult(arrType),
                        DBSPOpcode.ARRAY_CONVERT_SAFE, vecV, convert);
                source = source.cast(node, type, UNWRAP);
            }
            return source.cast(node, type, SAFE);
        } else if (sourceType.is(DBSPTypeArray.class)) {
            DBSPTypeArray sourceVecType = sourceType.to(DBSPTypeArray.class);
            // If the element type does not match, need to convert all elements
            if (!type.getElementType().equals(sourceVecType.getElementType())) {
                if (sourceVecType.getElementType().is(DBSPTypeAny.class)) {
                    if (source.is(DBSPArrayExpression.class)) {
                        DBSPArrayExpression arr = source.to(DBSPArrayExpression.class);
                        if (arr.data == null)
                            return new DBSPArrayExpression(type, true);
                        if (arr.data.isEmpty())
                            return new DBSPArrayExpression(type, false);
                        throw new CompilationError("Could not infer a type for array elements; " +
                                "please specify it using CAST(array AS X ARRAY)", source.getNode());
                    }
                }

                DBSPExpression convert = this.converterFunction(
                        node, sourceVecType.getElementType(), type.getElementType());
                source = new DBSPBinaryExpression(node,
                        new DBSPTypeSqlResult(new DBSPTypeArray(type.getElementType(), true)),
                        DBSPOpcode.ARRAY_CONVERT_SAFE, source.applyClone(), convert);
                return source.cast(node, type, UNWRAP);
            } else {
                ExpandCasts.unsupported(source, type);
            }
            return source.cast(node, type, SAFE);
        } else if (sourceType.is(DBSPTypeNull.class)) {
            return new DBSPArrayExpression(type, true);
        } else {
            ExpandCasts.unsupported(source, type);
            // unreachable
            return source;
        }
    }

    @Nullable
    DBSPExpression convertToMap(
            CalciteObject node, DBSPExpression source, DBSPTypeMap type) {
        DBSPType sourceType = source.getType();
        if (!sourceType.is(DBSPTypeMap.class))
            return null;

        DBSPTypeMap sourceMap = sourceType.to(DBSPTypeMap.class);
        DBSPClosureExpression convertKey = this.converterFunction(node, sourceMap.getKeyType(), type.getKeyType());
        DBSPClosureExpression convertValue = this.converterFunction(node, sourceMap.getValueType(), type.getValueType());
        DBSPType convertedType = new DBSPTypeMap(type.getKeyType(), type.getValueType(), sourceType.mayBeNull);
        source = new DBSPBinaryExpression(node,
                new DBSPTypeSqlResult(convertedType), DBSPOpcode.MAP_CONVERT_SAFE,
                source, new DBSPRawTupleExpression(convertKey, convertValue));
        return source.cast(node, type, UNWRAP);
    }

    @Override
    public void postorder(DBSPCastExpression expression) {
        if (this.getEN(expression) != null) {
            // Already translated
            return;
        }
        CalciteObject node = expression.getNode();
        this.push(expression);
        DBSPExpression source = this.getE(expression.source);
        DBSPType sourceType = source.getType();
        DBSPType type = expression.getType();
        DBSPExpression result = null;

        DBSPCastExpression.CastType castType = expression.safe;
        if (!castType.isSql() || !castType.isSafe()) {
            result = source.cast(expression.getNode(), type, castType);
            this.map(expression, result);
            this.pop(expression);
            return;
        }

        if (type.sameTypeIgnoringNullability(sourceType)) {
            if (type.sameType(sourceType)) {
                result = expression.source;
            } else if (type.mayBeNull) {
                // Cast from T to Option<T>
                result = expression.source.applyCloneIfNeeded().someIfNeeded();
            } else if (sourceType.mayBeNull && !type.is(DBSPTypeVariant.class)) {
                // Cast from Option<T> to T
                // Only VARIANT has a different implementation
                result = expression.source.unwrap("NULL value should be impossible here").applyCloneIfNeeded();
            }
        } else if (type.is(DBSPTypeVariant.class)) {
            // convert to an unsafe cast, since it cannot fail
            result = source.cast(node, type, DBSPCastExpression.CastType.SqlUnsafe);
        } else if (type.is(DBSPTypeArray.class)) {
            result = this.convertToArray(node, source, type.to(DBSPTypeArray.class));
        } else if (type.is(DBSPTypeTuple.class)) {
            // These should be rejected in the front-end; they are documented as not supported
            // We may support them someday if there is demand.
            throw new UnimplementedException("SAFE_CAST to tuple type");
        } else if (type.is(DBSPTypeMap.class)) {
            result = this.convertToMap(node, source, type.to(DBSPTypeMap.class));
        }
        if (result == null) {
            // Default implementation, for scalar types
            result = source
                    // Invoke actual cast function, which returns SqlResult
                    .cast(node, new DBSPTypeSqlResult(type), DBSPCastExpression.CastType.RustCast)
                    // Unwrap the SqlResult to get the actual value
                    .cast(node, type, UNWRAP);
        }
        this.pop(expression);
        DBSPType computedType = result.getType();
        Utilities.enforce(expression.getType().sameType(computedType), () ->
                "ExpandSafeCasts converted an expression producing " + expression.getType() +
                        " to an expression producing " + computedType);
        this.map(expression, result);
    }
}
