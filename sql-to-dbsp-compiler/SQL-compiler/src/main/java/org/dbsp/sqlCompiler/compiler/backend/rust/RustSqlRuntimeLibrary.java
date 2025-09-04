/*
 * Copyright 2022 VMware, Inc.
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

package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

/** This class encodes (part of) the interface to the SQL
 * runtime library: support functions that implement the SQL semantics. */
@SuppressWarnings({"SpellCheckingInspection"})
public class RustSqlRuntimeLibrary {
    private final LinkedHashMap<String, DBSPOpcode> universalFunctions = new LinkedHashMap<>();
    private final LinkedHashMap<String, DBSPOpcode> arithmeticFunctions = new LinkedHashMap<>();
    private final LinkedHashMap<String, DBSPOpcode> dateFunctions = new LinkedHashMap<>();
    private final LinkedHashMap<String, DBSPOpcode> stringFunctions = new LinkedHashMap<>();
    private final LinkedHashMap<String, DBSPOpcode> booleanFunctions = new LinkedHashMap<>();
    private final LinkedHashMap<String, DBSPOpcode> otherFunctions = new LinkedHashMap<>();

    public static final RustSqlRuntimeLibrary INSTANCE = new RustSqlRuntimeLibrary();

    protected RustSqlRuntimeLibrary() {
        this.universalFunctions.put("eq", DBSPOpcode.EQ);
        this.universalFunctions.put("neq", DBSPOpcode.NEQ);
        this.universalFunctions.put("lt", DBSPOpcode.LT);
        this.universalFunctions.put("gt", DBSPOpcode.GT);
        this.universalFunctions.put("lte", DBSPOpcode.LTE);
        this.universalFunctions.put("gte", DBSPOpcode.GTE);
        this.universalFunctions.put(DBSPOpcode.IS_DISTINCT.toString(), DBSPOpcode.IS_DISTINCT);
        this.universalFunctions.put(DBSPOpcode.MIN.toString(), DBSPOpcode.MIN);
        this.universalFunctions.put(DBSPOpcode.MAX.toString(), DBSPOpcode.MAX);
        this.universalFunctions.put(DBSPOpcode.MIN_IGNORE_NULLS.toString(), DBSPOpcode.MIN_IGNORE_NULLS);
        this.universalFunctions.put(DBSPOpcode.MAX_IGNORE_NULLS.toString(), DBSPOpcode.MAX_IGNORE_NULLS);
        this.universalFunctions.put(DBSPOpcode.AGG_LTE.toString(), DBSPOpcode.AGG_LTE);
        this.universalFunctions.put(DBSPOpcode.AGG_GTE.toString(), DBSPOpcode.AGG_GTE);
        this.universalFunctions.put(DBSPOpcode.AGG_MIN.toString(), DBSPOpcode.AGG_MIN);
        this.universalFunctions.put(DBSPOpcode.AGG_MAX.toString(), DBSPOpcode.AGG_MAX);
        this.universalFunctions.put(DBSPOpcode.AGG_MIN1.toString(), DBSPOpcode.AGG_MIN1);
        this.universalFunctions.put(DBSPOpcode.AGG_MAX1.toString(), DBSPOpcode.AGG_MAX1);

        this.arithmeticFunctions.put("plus", DBSPOpcode.ADD);
        this.arithmeticFunctions.put("minus", DBSPOpcode.SUB);
        this.arithmeticFunctions.put("modulo", DBSPOpcode.MOD);
        this.arithmeticFunctions.put("times", DBSPOpcode.MUL);
        this.arithmeticFunctions.put("div", DBSPOpcode.DIV);
        this.arithmeticFunctions.put(DBSPOpcode.DIV_NULL.toString(), DBSPOpcode.DIV_NULL);
        this.arithmeticFunctions.put("band", DBSPOpcode.BW_AND);
        this.arithmeticFunctions.put("bor", DBSPOpcode.BW_OR);
        this.arithmeticFunctions.put("bxor", DBSPOpcode.XOR);
        this.arithmeticFunctions.put("mul_by_ref", DBSPOpcode.MUL_WEIGHT);
        this.arithmeticFunctions.put(DBSPOpcode.AGG_ADD.toString(), DBSPOpcode.AGG_ADD);
        this.arithmeticFunctions.put(DBSPOpcode.AGG_ADD_NON_NULL.toString(), DBSPOpcode.AGG_ADD_NON_NULL);
        this.arithmeticFunctions.put(DBSPOpcode.AGG_AND.toString(), DBSPOpcode.AGG_AND);
        this.arithmeticFunctions.put(DBSPOpcode.AGG_OR.toString(), DBSPOpcode.AGG_OR);
        this.arithmeticFunctions.put(DBSPOpcode.AGG_XOR.toString(), DBSPOpcode.AGG_XOR);
        this.arithmeticFunctions.put(DBSPOpcode.CONTROLLED_FILTER_GTE.toString(),
                DBSPOpcode.CONTROLLED_FILTER_GTE);

        this.dateFunctions.put("plus", DBSPOpcode.TS_ADD);
        this.dateFunctions.put("minus", DBSPOpcode.TS_SUB);
        this.dateFunctions.put("times", DBSPOpcode.INTERVAL_MUL);
        this.dateFunctions.put("div", DBSPOpcode.INTERVAL_DIV);
        this.dateFunctions.put(DBSPOpcode.CONTROLLED_FILTER_GTE.toString(), DBSPOpcode.CONTROLLED_FILTER_GTE);

        this.stringFunctions.put("concat", DBSPOpcode.CONCAT);
        this.stringFunctions.put(DBSPOpcode.CONTROLLED_FILTER_GTE.toString(),
                DBSPOpcode.CONTROLLED_FILTER_GTE);

        this.booleanFunctions.put("and", DBSPOpcode.AND);
        this.booleanFunctions.put("or", DBSPOpcode.OR);
        this.booleanFunctions.put(DBSPOpcode.IS_FALSE.toString(), DBSPOpcode.IS_FALSE);
        this.booleanFunctions.put(DBSPOpcode.IS_NOT_TRUE.toString(), DBSPOpcode.IS_NOT_TRUE);
        this.booleanFunctions.put(DBSPOpcode.IS_TRUE.toString(), DBSPOpcode.IS_TRUE);
        this.booleanFunctions.put(DBSPOpcode.IS_NOT_FALSE.toString(), DBSPOpcode.IS_NOT_FALSE);
        this.booleanFunctions.put(DBSPOpcode.CONTROLLED_FILTER_GTE.toString(),
                DBSPOpcode.CONTROLLED_FILTER_GTE);

        // These are defined for VARBIT types
        this.otherFunctions.put(DBSPOpcode.AGG_AND.toString(), DBSPOpcode.AGG_AND);
        this.otherFunctions.put(DBSPOpcode.AGG_OR.toString(), DBSPOpcode.AGG_OR);
        this.otherFunctions.put(DBSPOpcode.AGG_XOR.toString(), DBSPOpcode.AGG_XOR);
        this.otherFunctions.put("concat", DBSPOpcode.CONCAT);
        this.otherFunctions.put(DBSPOpcode.CONTROLLED_FILTER_GTE.toString(),
                DBSPOpcode.CONTROLLED_FILTER_GTE);
    }

    public static class FunctionDescription {
        public final String function;
        public final DBSPType returnType;

        public FunctionDescription(String function, DBSPType returnType) {
            this.function = function;
            this.returnType = returnType;
        }

        @Override
        public String toString() {
            return "FunctionDescription{" +
                    "function='" + function + '\'' +
                    ", returnType=" + returnType +
                    '}';
        }
    }

    public static FunctionDescription getWindowBound(CalciteObject node,
            DBSPType unsignedType, DBSPType sortType, DBSPType boundType) {
        // we ignore nullability because window bounds are constants and cannot be null
        if (boundType.is(DBSPTypeMonthsInterval.class)) {
            throw new UnsupportedException("""
                    Currently the compiler only supports constant OVER window bounds.
                    Intervals such as 'INTERVAL 1 MONTH' or 'INTERVAL 1 YEAR' are not constant.
                    Can you rephrase the query using an interval such as 'INTERVAL 30 DAYS' instead?""", node);
        }

        Function<DBSPType, String> boundFunction;
        if (boundType.is(IsIntervalType.class))
            boundFunction = t -> t.withMayBeNull(false).to(DBSPTypeBaseType.class).shortName();
        else
            boundFunction = t -> t.withMayBeNull(false).baseTypeWithSuffix();

        return new FunctionDescription("to_bound_" +
                boundFunction.apply(boundType) + "_" +
                boundFunction.apply(sortType) + "_" +
                unsignedType.baseTypeWithSuffix(), unsignedType);
    }

    /** The name of the Rust function in the sqllib which implements a specific operation
     * @param node                Calcite node.
     * @param opcode              Operation opcode.
     * @param expectedReturnType  Return type of the function.
     * @param ltype               Type of first argument.
     * @param rtype               Type of second argument; null if function takes only one argument.
     * @return                    The function name.
     */
    public String getFunctionName(CalciteObject node,
                                  DBSPOpcode opcode, DBSPType expectedReturnType,
                                  DBSPType ltype, @Nullable DBSPType rtype) {
        if (ltype.is(DBSPTypeAny.class) || (rtype != null && rtype.is(DBSPTypeAny.class)))
            throw new InternalCompilerError("Unexpected type _ for operand of " + opcode, ltype);
        HashMap<String, DBSPOpcode> map = null;
        String suffixReturn = "";  // suffix based on the return type

        if (opcode.isComparison() ||
            opcode == DBSPOpcode.MAX || opcode == DBSPOpcode.MIN ||
            opcode == DBSPOpcode.MAX_IGNORE_NULLS || opcode == DBSPOpcode.MIN_IGNORE_NULLS ||
            opcode == DBSPOpcode.AGG_GTE || opcode == DBSPOpcode.AGG_LTE ||
            opcode == DBSPOpcode.AGG_MIN || opcode == DBSPOpcode.AGG_MAX ||
            opcode == DBSPOpcode.AGG_MIN1 || opcode == DBSPOpcode.AGG_MAX1 ||
            opcode == DBSPOpcode.IS_DISTINCT) {
            map = this.universalFunctions;
        } else if (ltype.as(DBSPTypeBool.class) != null) {
            map = this.booleanFunctions;
        } else if (ltype.is(IsTimeRelatedType.class)) {
            map = this.dateFunctions;
            if (opcode == DBSPOpcode.TS_SUB || opcode == DBSPOpcode.TS_ADD) {
                if (ltype.is(IsTimeRelatedType.class)) {
                    Utilities.enforce(rtype != null);
                    suffixReturn = "_" + expectedReturnType.to(DBSPTypeBaseType.class).shortName()
                            + expectedReturnType.nullableSuffix();
                    if (rtype.is(IsNumericType.class))
                        throw new CompilationError("Cannot apply operation " + Utilities.singleQuote(opcode.toString()) +
                                " to arguments of type " + ltype.asSqlString() + " and " + rtype.asSqlString(), node);
                }
            }
        } else if (ltype.is(IsNumericType.class)) {
            map = this.arithmeticFunctions;
        } else if (ltype.is(DBSPTypeString.class)) {
            map = this.stringFunctions;
        } else if (ltype.is(DBSPTypeBinary.class) || ltype.is(DBSPTypeVariant.class)) {
            map = this.otherFunctions;
        }
        if (rtype != null && rtype.is(IsIntervalType.class)) {
            if (opcode == DBSPOpcode.INTERVAL_MUL || opcode == DBSPOpcode.INTERVAL_DIV) {
                // e.g., 10 * INTERVAL
                map = this.dateFunctions;
            }
        }

        String suffixl = ltype.nullableSuffix();
        String suffixr = rtype == null ? "" : rtype.nullableSuffix();
        String tsuffixl;
        String tsuffixr;
        if (map == universalFunctions) {
            Utilities.enforce(rtype != null);
            tsuffixl = "";
            tsuffixr = "";
            if (opcode == DBSPOpcode.AGG_MIN1 || opcode == DBSPOpcode.AGG_MAX1) {
                // The function has 2 two-tuple arguments of types (L, R).
                // The name is generated based on the types of the L component only
                Utilities.enforce(ltype.is(DBSPTypeRawTuple.class));
                Utilities.enforce(rtype.is(DBSPTypeRawTuple.class));
                ltype = ltype.to(DBSPTypeRawTuple.class).getFieldType(0);
                rtype = rtype.to(DBSPTypeRawTuple.class).getFieldType(0);
            }
            suffixl = ltype.nullableSuffix();
            suffixr = rtype.nullableSuffix();
        } else if (opcode == DBSPOpcode.CONTROLLED_FILTER_GTE) {
            tsuffixl = "";
            tsuffixr = "";
        } else {
            tsuffixl = ltype.to(DBSPTypeBaseType.class).shortName();
            tsuffixr = (rtype == null) ? "" : rtype.to(DBSPTypeBaseType.class).shortName();
        }
        if (map == null)
            throw new UnimplementedException("Rust implementation for " +
                    Utilities.singleQuote(opcode.toString()) + " not found", node);
        for (String k: map.keySet()) {
            DBSPOpcode inMap = map.get(k);
            if (opcode.equals(inMap)) {
                return k + "_" + tsuffixl + suffixl + "_" + tsuffixr + suffixr + suffixReturn;
            }
        }
        throw new UnimplementedException("Rust implementation for " +
                Utilities.singleQuote(opcode.toString()) + "/" + ltype + " not found", node);
    }
}
