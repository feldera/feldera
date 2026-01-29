package org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFailExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSqlResult;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Rewrites unsafe casts between complex types into sequences of operations.
 * Since unsafe casts panic at the first failed conversion, we handle each error locally. */
public class ExpandUnsafeCasts extends ExpressionTranslator {
    // This pass may be iterated many times, depending on the complexity of the expressions involved.
    // Expanding some casts generates other casts.
    public ExpandUnsafeCasts(DBSPCompiler compiler) {
        super(compiler);
    }

    static final DBSPCastExpression.CastType UNSAFE = DBSPCastExpression.CastType.SqlUnsafe;
    static final DBSPCastExpression.CastType UNWRAP = UNSAFE.getUnwrap();

    /** Create a function which converts values from type source to type destination.
     * This function will return a value with the original type. */
    DBSPClosureExpression converterFunction(CalciteObject node, DBSPType source, DBSPType dest) {
        DBSPVariablePath var = source.ref().var();
        DBSPExpression convertValue = var.deref();
        if (convertValue.getType().is(DBSPTypeBaseType.class))
            convertValue = convertValue.applyCloneIfNeeded();
        var cast = convertValue.cast(node, dest, UNSAFE);
        // Recursively apply the translation to the produced cast
        DBSPExpression expanded = this.analyze(cast).to(DBSPExpression.class);
        return expanded.closure(var);
    }

    @Nullable DBSPExpression convertToVariant(CalciteObject node, DBSPExpression source, boolean mayBeNull) {
        DBSPExpression expression;
        if (source.type.is(DBSPTypeTuple.class)) {
            // Convert a tuple to a VARIANT MAP indexed by the field names
            DBSPTypeTuple tuple = source.getType().to(DBSPTypeTuple.class);
            DBSPTypeMap type = new DBSPTypeMap(
                    DBSPTypeString.varchar(false),
                    DBSPTypeVariant.create(false),
                    source.getType().mayBeNull);

            if (tuple.originalStruct == null) {
                throw new UnimplementedException("Cast between Tuple type and " +
                        tuple.asSqlString() + " not implemented", source.getNode());
            }
            List<DBSPExpression> keys = new ArrayList<>();
            List<DBSPExpression> values = new ArrayList<>();
            List<ProgramIdentifier> names = Linq.list(tuple.originalStruct.getFieldNames());
            for (int i = 0; i < tuple.size(); i++) {
                ProgramIdentifier fieldName = names.get(i);
                keys.add(new DBSPStringLiteral(fieldName.toString()));

                DBSPExpression field = source.field(i);
                if (!field.getType().hasCopy())
                    field = field.applyCloneIfNeeded();
                DBSPExpression rec = field.cast(node, DBSPTypeVariant.create(false), UNSAFE);
                values.add(rec);
            }
            expression = new DBSPMapExpression(type, keys, values);
        } else if (source.type.is(DBSPTypeArray.class)) {
            // Convert a vector by converting all elements to Variant
            DBSPTypeArray vecType = source.type.to(DBSPTypeArray.class);
            DBSPType elementType = vecType.getElementType();
            if (elementType.is(DBSPTypeVariant.class)) {
                // nothing to do
                return null;
            }
            DBSPVariablePath var = elementType.ref().var();
            // This expression may need to be recursively converted
            DBSPExpression converter = var.deref()
                    .applyCloneIfNeeded().cast(node, DBSPTypeVariant.create(false), UNSAFE)
                    .closure(var);
            expression = new DBSPBinaryExpression(node,
                    new DBSPTypeArray(DBSPTypeVariant.create(false), vecType.mayBeNull),
                    DBSPOpcode.ARRAY_CONVERT, source, converter);
        } else {
            return null;
        }
        return new DBSPCastExpression(node, expression, DBSPTypeVariant.create(mayBeNull), UNSAFE);
    }

    public DBSPExpression convertToStructOrTuple(
            CalciteObject node, DBSPExpression source, DBSPTypeTuple type) {
        List<DBSPExpression> fields = new ArrayList<>();
        DBSPTypeStruct struct = type.originalStruct;
        List<ProgramIdentifier> names = null;
        if (struct != null)
            names = Linq.list(type.originalStruct.getFieldNames());

        DBSPType sourceType = source.getType();
        for (int i = 0; i < type.size(); i++) {
            DBSPType fieldType = type.getFieldType(i);
            DBSPExpression field;
            if (sourceType.is(DBSPTypeTupleBase.class)) {
                field = source.field(i);
            } else if (sourceType.is(DBSPTypeVariant.class)) {
                if (struct == null) {
                    field = new DBSPBinaryExpression(
                            // Result of index is always nullable
                            node, DBSPTypeVariant.create(true),
                            DBSPOpcode.RUST_INDEX, source.applyCloneIfNeeded(),
                            new DBSPUSizeLiteral(i));
                } else {
                    ProgramIdentifier fieldName = names.get(i);
                    field = new DBSPBinaryExpression(
                            // Result of index is always nullable
                            node, DBSPTypeVariant.create(true),
                            DBSPOpcode.VARIANT_INDEX, source.applyCloneIfNeeded(),
                            new DBSPStringLiteral(fieldName.toString()));
                }
            } else {
                throw new InternalCompilerError("Unexpected source type " + sourceType);
            }
            DBSPExpression expression = field.applyCloneIfNeeded().cast(node, fieldType, UNSAFE);
            // Convert recursively
            expression = this.analyze(expression).to(DBSPExpression.class);
            fields.add(expression);
        }

        DBSPExpression result = new DBSPTupleExpression(source.getNode(), type, fields);
        if (source.getType().mayBeNull) {
            if (type.mayBeNull) {
                result = new DBSPIfExpression(node, source.is_null(), type.none(), result);
            } else {
                result = new DBSPIfExpression(node, source.is_null(),
                        new DBSPFailExpression(source.getNode(), type, "Cast to non-nullable value applied to NULL"),
                        result);
            }
        }

        return result;
    }

    DBSPExpression convertToRawTuple(
            CalciteObject node, DBSPExpression source, DBSPTypeRawTuple type) {
        // Users cannot write casts to raw tuples, these are only generated internally by the compiler and
        // have very limited shapes
        List<DBSPExpression> fields = new ArrayList<>();
        DBSPType sourceType = source.getType();
        if (!sourceType.is(DBSPTypeRawTuple.class))
            throw new InternalCompilerError("Cast to RAW tuple from " + sourceType);
        for (int i = 0; i < type.size(); i++) {
            DBSPType fieldType = type.getFieldType(i);
            DBSPExpression field = source.field(i);
            DBSPExpression expression;
            expression = field.cast(node, fieldType, UNSAFE);
            // Convert recursively
            expression = this.analyze(expression).to(DBSPExpression.class);
            fields.add(expression);
        }
        return new DBSPRawTupleExpression(fields);
    }

    @Nullable DBSPExpression convertToArray(final CalciteObject node, DBSPExpression source, final DBSPTypeArray type) {
        DBSPType sourceType = source.getType();
        if (sourceType.is(DBSPTypeVariant.class)) {
            if (type.getElementType().is(DBSPTypeBaseType.class)) {
                // Default conversion is fine
                return null;
            } else {
                // Convert to a Vector of VARIANT, and then...
                DBSPTypeArray vecVType = new DBSPTypeArray(DBSPTypeVariant.create(false), sourceType.mayBeNull);
                DBSPExpression vecV = source.cast(node, vecVType, UNSAFE);
                // ...convert each element recursively to the target element type
                DBSPExpression convert = this.converterFunction(node, vecVType.getElementType(), type.getElementType());
                DBSPType arrType = new DBSPTypeArray(type.getElementType(), sourceType.mayBeNull);
                source = new DBSPBinaryExpression(node, arrType, DBSPOpcode.ARRAY_CONVERT, vecV, convert);
            }
            return source.cast(node, type, UNSAFE);
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

                DBSPExpression convert = this.converterFunction(node, sourceVecType.getElementType(), type.getElementType());
                source = new DBSPBinaryExpression(node,
                        new DBSPTypeArray(type.getElementType(), sourceType.mayBeNull),
                        DBSPOpcode.ARRAY_CONVERT, source, convert);
                return source.nullabilityCast(type, UNSAFE);
            } else {
                ExpandCasts.unsupported(source, type);
            }
            return source.cast(node, type, UNSAFE);
        } else if (sourceType.is(DBSPTypeNull.class)) {
            return new DBSPArrayExpression(type, true);
        } else {
            ExpandCasts.unsupported(source, type);
            // unreachable
            return source;
        }
    }

    @Nullable DBSPExpression convertToMap(CalciteObject node, final DBSPExpression source, DBSPTypeMap type) {
        DBSPType sourceType = source.getType();
        if (!sourceType.is(DBSPTypeMap.class))
            return null;

        DBSPTypeMap sourceMap = sourceType.to(DBSPTypeMap.class);
        DBSPClosureExpression convertKey = this.converterFunction(node, sourceMap.getKeyType(), type.getKeyType());
        DBSPClosureExpression convertValue = this.converterFunction(node, sourceMap.getValueType(), type.getValueType());
        // Nullability is inherited from the source type
        DBSPType convertedType = new DBSPTypeMap(type.getKeyType(), type.getValueType(), sourceType.mayBeNull);
        return new DBSPBinaryExpression(node,
                convertedType, DBSPOpcode.MAP_CONVERT,
                source, new DBSPRawTupleExpression(convertKey, convertValue));
    }

    @Override
    public VisitDecision preorder(DBSPExpression expression) {
        if (this.getEN(expression) != null)
            // Already translated; translation is context-independent
            return VisitDecision.STOP;
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(final DBSPCastExpression expression) {
        if (this.getEN(expression) != null) {
            // Already translated
            return;
        }
        final CalciteObject node = expression.getNode();
        this.push(expression);
        final DBSPExpression source = this.getE(expression.source);
        final DBSPType sourceType = source.getType();
        final DBSPType type = expression.getType();
        DBSPExpression result = null;

        final DBSPCastExpression.CastType castType = expression.safe;
        if (!castType.isSql()) {
            // Leave unchanged
            result = source.cast(expression.getNode(), type, castType);
            this.map(expression, result);
            this.pop(expression);
            return;
        }

        Utilities.enforce(!castType.isSafe());
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
            result = this.convertToVariant(node, source, type.mayBeNull);
        } else if (type.is(DBSPTypeArray.class)) {
            result = this.convertToArray(node, source, type.to(DBSPTypeArray.class));
        } else if (type.is(DBSPTypeTuple.class)) {
            result = this.convertToStructOrTuple(node, source, type.to(DBSPTypeTuple.class));
        } else if (type.is(DBSPTypeRawTuple.class)) {
            result = this.convertToRawTuple(node, source, type.to(DBSPTypeRawTuple.class));
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
        // If the conversion has not produced the exact requested type, insert one more cast,
        // which will have to be recursively expanded
        if (!result.getType().sameType(type))
            result = result.cast(node, type, UNSAFE);

        this.pop(expression);
        DBSPType computedType = result.getType();
        Utilities.enforce(expression.getType().sameType(computedType), () ->
                "ExpandUnsafeCasts converted an expression producing " + expression.getType() +
                " to an expression producing " + computedType);
        this.map(expression, result);
    }
}
