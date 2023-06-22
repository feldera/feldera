/*
 * Copyright 2023 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.backend.jit.ir;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToJitVisitor;
import org.dbsp.sqlCompiler.compiler.backend.jit.TypeCatalog;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITScalarType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITUnitType;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;

import java.util.*;

/**
 * The JIT only supports "simple" parameters and return values.
 * So we have to convert more complex types into simpler ones.
 * - An input parameter that has a type that is a nested tuple
 *   is decomposed into multiple parameters, one for each tuple field.
 * - A return value that has a non-scalar type is decomposed into
 *   multiple parameters.
 */
public class JITParameterMapping {
    /**
     * Convert the type into a tuple type.
     * Type may be either a reference type, or a RawTuple, or a normal Tuple.
     */
    public static DBSPTypeTuple makeTupleType(DBSPType type) {
        DBSPTypeRef ref = type.as(DBSPTypeRef.class);
        if (ref != null)
            type = ref.type;
        if (type.is(DBSPTypeTuple.class))
            return type.to(DBSPTypeTuple.class);
        else if (type.is(DBSPTypeRawTuple.class))
            return new DBSPTypeTuple(type.to(DBSPTypeRawTuple.class).tupFields);
        throw new UnimplementedException("Conversion to Tuple", type);
    }

    /**
     * We are given a type that is a Tuple or RawTuple.
     * Expand it into a list of Tuple types as follows:
     * - if the type is a Tuple, just return a singleton list.
     * - if the type is a RawTuple, add all its components to the list.
     * Each component is expected to be a Tuple.
     * This is used for functions like stream.index_with, which
     * take a closure that returns a tuple of values.  The JIT does
     * not support nested tuples.
     */
    public static List<DBSPTypeTuple> expandToTuples(DBSPType type) {
        List<DBSPTypeTuple> types = new ArrayList<>();
        if (type.is(DBSPTypeRawTuple.class)) {
            for (DBSPType field : type.to(DBSPTypeRawTuple.class).tupFields) {
                DBSPTypeTuple tuple = makeTupleType(field);
                types.add(tuple);
            }
        } else {
            DBSPTypeRef ref = type.as(DBSPTypeRef.class);
            if (ref != null)
                type = ref.type;
            types.add(type.to(DBSPTypeTuple.class));
        }
        return types;
    }

    /**
     * Each input parameter is mapped to a
     */
    final TypeCatalog typeCatalog;

    public JITParameterMapping(TypeCatalog catalog) {
        this.typeCatalog = catalog;
    }

    int parameterIndex = 1;
    public final List<JITParameter> parameters = new ArrayList<>();

    public void addParameter(DBSPParameter param, JITParameter.Direction direction, ToJitVisitor jitVisitor) {
        DBSPType paramType = param.getType();
        boolean mayBeNull = paramType.mayBeNull;
        JITRowType t = this.typeCatalog.convertTupleType(paramType, jitVisitor);
        JITParameter p = new JITParameter(this.parameterIndex, param.name, direction, t, mayBeNull);
        this.parameters.add(p);
        this.parameterIndex++;
    }

    /**
     * Prefix of the name of the fictitious parameters that are added to closures that return structs.
     * The JIT only supports returning scalar values, so a closure that returns a
     * tuple actually receives an additional parameter that is "output" (equivalent to a "mut").
     * Moreover, a closure that returns a RawTuple will return one value for each field of the raw tuple.
     */
    private static final String RETURN_PARAMETER_PREFIX = "$retval";

    /**
     * Add parameters that correspond to the returned value.
     * @param returnType  The actual type returned by the synthesized function.
     *                    If the returned type is a scalar, that's the actual
     *                    returned type of the JIT.  If the returned type is
     *                    a tuple, the JIT will use OUT parameters
     *                    and return a Unit type.
     */
    public JITScalarType addReturn(@Nullable DBSPType returnType, ToJitVisitor jitVisitor) {
        if (jitVisitor.isScalarType(returnType)) {
            if (returnType == null) {
                return JITUnitType.INSTANCE;
            } else {
                return jitVisitor.scalarType(returnType);
            }
        }
        // The result could be a tuple of tuples, then make each of
        // the fields a separate parameter.
        Objects.requireNonNull(returnType);
        List<DBSPTypeTuple> types = expandToTuples(returnType);
        for (DBSPTypeTuple type: types) {
            JITRowType t = this.typeCatalog.convertTupleType(type, jitVisitor);
            String varName = RETURN_PARAMETER_PREFIX + "_" + this.parameters.size();
            JITParameter p = new JITParameter(this.parameterIndex, varName, JITParameter.Direction.OUT, t, type.mayBeNull);
            this.parameterIndex++;
            this.parameters.add(p);
        }
        return JITUnitType.INSTANCE;
    }
}
