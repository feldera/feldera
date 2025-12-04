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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
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
    private final HashMap<DBSPOpcode, String> universalFunctions = new HashMap<>();
    private final HashMap<DBSPOpcode, String > arithmeticFunctions = new HashMap<>();
    private final HashMap<DBSPOpcode, String> dateFunctions = new HashMap<>();
    private final HashMap<DBSPOpcode, String> stringFunctions = new HashMap<>();
    private final HashMap<DBSPOpcode, String> booleanFunctions = new HashMap<>();
    private final HashMap<DBSPOpcode, String> otherFunctions = new HashMap<>();

    public static final RustSqlRuntimeLibrary INSTANCE = new RustSqlRuntimeLibrary();

    protected RustSqlRuntimeLibrary() {
        this.universalFunctions.put(DBSPOpcode.EQ,  "eq");
        this.universalFunctions.put(DBSPOpcode.NEQ, "neq");
        this.universalFunctions.put(DBSPOpcode.LT,  "lt");
        this.universalFunctions.put(DBSPOpcode.GT,  "gt");
        this.universalFunctions.put(DBSPOpcode.LTE, "lte");
        this.universalFunctions.put(DBSPOpcode.GTE, "gte");
        this.universalFunctions.put(DBSPOpcode.IS_DISTINCT, DBSPOpcode.IS_DISTINCT.toString());
        this.universalFunctions.put(DBSPOpcode.MIN, DBSPOpcode.MIN.toString());
        this.universalFunctions.put(DBSPOpcode.MAX, DBSPOpcode.MAX.toString());
        this.universalFunctions.put(DBSPOpcode.MIN_IGNORE_NULLS, DBSPOpcode.MIN_IGNORE_NULLS.toString());
        this.universalFunctions.put(DBSPOpcode.MAX_IGNORE_NULLS, DBSPOpcode.MAX_IGNORE_NULLS.toString());
        this.universalFunctions.put(DBSPOpcode.AGG_LTE, DBSPOpcode.AGG_LTE.toString());
        this.universalFunctions.put(DBSPOpcode.AGG_GTE, DBSPOpcode.AGG_GTE.toString());
        this.universalFunctions.put(DBSPOpcode.AGG_MIN, DBSPOpcode.AGG_MIN.toString());
        this.universalFunctions.put(DBSPOpcode.AGG_MAX, DBSPOpcode.AGG_MAX.toString());
        this.universalFunctions.put(DBSPOpcode.AGG_MIN1, DBSPOpcode.AGG_MIN1.toString());
        this.universalFunctions.put(DBSPOpcode.AGG_MAX1, DBSPOpcode.AGG_MAX1.toString());

        this.arithmeticFunctions.put(DBSPOpcode.ADD, "plus");
        this.arithmeticFunctions.put(DBSPOpcode.SUB, "minus");
        this.arithmeticFunctions.put(DBSPOpcode.MOD, "modulo");
        this.arithmeticFunctions.put(DBSPOpcode.MUL, "times");
        this.arithmeticFunctions.put(DBSPOpcode.DIV, "div");
        this.arithmeticFunctions.put(DBSPOpcode.BW_AND, "band");
        this.arithmeticFunctions.put(DBSPOpcode.BW_OR, "bor");
        this.arithmeticFunctions.put(DBSPOpcode.XOR, "bxor");
        this.arithmeticFunctions.put(DBSPOpcode.MUL_WEIGHT, "mul_by_ref");
        this.arithmeticFunctions.put(DBSPOpcode.DIV_NULL, DBSPOpcode.DIV_NULL.toString());
        this.arithmeticFunctions.put(DBSPOpcode.AGG_ADD, DBSPOpcode.AGG_ADD.toString());
        this.arithmeticFunctions.put(DBSPOpcode.AGG_ADD_NON_NULL, DBSPOpcode.AGG_ADD_NON_NULL.toString());
        this.arithmeticFunctions.put(DBSPOpcode.AGG_AND, DBSPOpcode.AGG_AND.toString());
        this.arithmeticFunctions.put(DBSPOpcode.AGG_OR, DBSPOpcode.AGG_OR.toString());
        this.arithmeticFunctions.put(DBSPOpcode.AGG_XOR, DBSPOpcode.AGG_XOR.toString());
        this.arithmeticFunctions.put(DBSPOpcode.CONTROLLED_FILTER_GTE, DBSPOpcode.CONTROLLED_FILTER_GTE.toString());

        this.dateFunctions.put(DBSPOpcode.INTERVAL_MUL, "times");
        this.dateFunctions.put(DBSPOpcode.INTERVAL_DIV, "div");
        this.dateFunctions.put(DBSPOpcode.CONTROLLED_FILTER_GTE, DBSPOpcode.CONTROLLED_FILTER_GTE.toString());

        this.stringFunctions.put(DBSPOpcode.CONCAT, "concat");
        this.stringFunctions.put(
                DBSPOpcode.CONTROLLED_FILTER_GTE,
                DBSPOpcode.CONTROLLED_FILTER_GTE.toString());

        this.booleanFunctions.put(DBSPOpcode.AND, "and");
        this.booleanFunctions.put(DBSPOpcode.OR, "or");
        this.booleanFunctions.put(DBSPOpcode.IS_FALSE, DBSPOpcode.IS_FALSE.toString());
        this.booleanFunctions.put(DBSPOpcode.IS_NOT_TRUE, DBSPOpcode.IS_NOT_TRUE.toString());
        this.booleanFunctions.put(DBSPOpcode.IS_TRUE, DBSPOpcode.IS_TRUE.toString());
        this.booleanFunctions.put(DBSPOpcode.IS_NOT_FALSE, DBSPOpcode.IS_NOT_FALSE.toString());
        this.booleanFunctions.put(DBSPOpcode.CONTROLLED_FILTER_GTE, DBSPOpcode.CONTROLLED_FILTER_GTE.toString());

        // These are defined for VARBIT types
        this.otherFunctions.put(DBSPOpcode.CONCAT, "concat");
        this.otherFunctions.put(DBSPOpcode.AGG_AND, DBSPOpcode.AGG_AND.toString());
        this.otherFunctions.put(DBSPOpcode.AGG_OR, DBSPOpcode.AGG_OR.toString());
        this.otherFunctions.put(DBSPOpcode.AGG_XOR, DBSPOpcode.AGG_XOR.toString());
        this.otherFunctions.put(DBSPOpcode.CONTROLLED_FILTER_GTE, DBSPOpcode.CONTROLLED_FILTER_GTE.toString());
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

    /**
     * The name of the Rust function in the sqllib which implements a specific operation
     *
     * @param node   Calcite node.
     * @param opcode Operation opcode.
     * @param ltype  Type of first argument.
     * @param rtype  Type of second argument; null if function takes only one argument.
     * @return The function name.
     */
    public String getFunctionName(CalciteObject node,
                                  DBSPOpcode opcode,
                                  DBSPType ltype, @Nullable DBSPType rtype) {
        if (ltype.is(DBSPTypeAny.class) || (rtype != null && rtype.is(DBSPTypeAny.class)))
            throw new InternalCompilerError("Unexpected type _ for operand of " + opcode, ltype);
        HashMap<DBSPOpcode, String> map = null;

        String separator = "_";
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
        String k = map.get(opcode);
        if (k != null)
            return k + separator + tsuffixl + suffixl + separator + tsuffixr + suffixr;
        throw new UnimplementedException("Rust implementation for " +
                opcode.name() + "/" + ltype + " not found", node);
    }
}
