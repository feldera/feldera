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

import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.*;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPPathSegment;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPConstItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.util.IndentStream;
import org.dbsp.util.UnsupportedException;
import org.dbsp.util.Utilities;

import java.util.Map;
import java.util.Objects;

/**
 * This visitor generate a Rust implementation of the program.
 */
public class ToRustInnerVisitor extends InnerVisitor {
    private final IndentStream builder;

    public ToRustInnerVisitor(IErrorReporter reporter, IndentStream builder) {
        super(reporter, true);
        this.builder = builder;
    }

    boolean doNullExpression(DBSPExpression expression) {
        this.builder.append("None::<");
        expression.getNonVoidType().setMayBeNull(false).accept(this);
        this.builder.append(">");
        return false;
    }

    public boolean doNull(DBSPLiteral literal) {
        if (!literal.isNull)
            throw new UnsupportedException(literal);
        return this.doNullExpression(literal);
    }

    @Override
    public boolean preorder(DBSPNullLiteral literal) {
        return this.doNull(literal);
    }

    @Override
    public boolean preorder(DBSPSortExpression expression) {
        /*
        move |(k, v): (&(), &Vec<Tuple<...>>, ), | -> Vec<Tuple<...>> {
            let comp = ...;    // comparator
            let mut ec: _ = move |a: &Tuple<...>, b: &Tuple<...>, | -> _ {
                comp.compare(a, b)
            };
            let mut v = v.clone();
            v.sort_unstable_by(ec);
            v
        }
         */
        this.builder.append("move |(k, v): (&(), &Vec<");
        expression.elementType.accept(this);
        this.builder.append(">)| -> Vec<");
        expression.elementType.accept(this);
        this.builder.append("> {").increase();
        this.builder.append("let ec = ");
        expression.comparator.accept(this);
        this.builder.append(";").newline();
        this.builder.append("let comp = move |a: &");
        expression.elementType.accept(this);
        this.builder.append(", b: &");
        expression.elementType.accept(this);
        this.builder.append("| { ec.compare(a, b) };");
        this.builder.append("let mut v = v.clone();").newline()
                .append("v.sort_unstable_by(comp);").newline()
                .append("v").newline()
                .decrease()
                .append("}");
        return false;
    }

    @Override
    public boolean preorder(DBSPSomeExpression expression) {
        this.builder.append("Some(");
        expression.expression.accept(this);
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPIndexExpression expression) {
        this.builder.append("(");
        expression.array.accept(this);
        this.builder.append("[");
        expression.index.accept(this);
        if (expression.startsAtOne)
            this.builder.append("- 1");
        this.builder.append("])");
        return false;
    }

    @Override
    public boolean preorder(DBSPFieldComparatorExpression expression) {
        expression.source.accept(this);
        boolean hasSource = expression.source.is(DBSPFieldComparatorExpression.class);
        if (hasSource)
            this.builder.append(".then(");
        this.builder.append("Extract::new(move |r: &");
        expression.tupleType().accept(this);
        this.builder.append("| r.")
                .append(expression.fieldNo)
                .append(")");
        if (!expression.ascending)
            this.builder.append(".rev()");
        if (hasSource)
            this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPTimestampLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("Timestamp::new(");
        this.builder.append(Long.toString(Objects.requireNonNull(literal.value)));
        this.builder.append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPDateLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("Date::new(");
        this.builder.append(Integer.toString(Objects.requireNonNull(literal.value)));
        this.builder.append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPIntervalMillisLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("ShortInterval::new(");
        this.builder.append(Long.toString(Objects.requireNonNull(literal.value)));
        this.builder.append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPGeoPointLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("GeoPoint::new(");
        Objects.requireNonNull(literal.left).accept(this);
        this.builder.append(", ");
        Objects.requireNonNull(literal.right).accept(this);
        this.builder.append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPBoolLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        this.builder.append(literal.wrapSome(Boolean.toString(Objects.requireNonNull(literal.value))));
        return false;
    }

    @Override
    public boolean preorder(DBSPVecLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        this.builder.append("vec!(")
                .increase();
        for (DBSPExpression exp: Objects.requireNonNull(literal.data)) {
            exp.accept(this);
            this.builder.append(", ");
        }
        this.builder.decrease().append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPZSetLiteral literal) {
        this.builder.append("zset!(")
                .increase();
        for (Map.Entry<DBSPExpression, Long> e: literal.data.data.entrySet()) {
            e.getKey().accept(this);
            this.builder.append(" => ")
                    .append(e.getValue())
                    .append(",\n");
        }
        this.builder.decrease().append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPFloatLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        float value = Objects.requireNonNull(literal.value);
        String val = Float.toString(value);
        if (Float.isNaN(value))
            val = "std::f32::NAN";
        else if (Float.isInfinite(value)) {
            if (value < 0)
                val = "std::f32::NEG_INFINITY";
            else
                val = "std::f32::INFINITY";
        }
        String out = literal.raw ? val : literal.wrapSome("F32::new(" + val + ")");
        this.builder.append(out);
        return false;
    }

    @Override
    public boolean preorder(DBSPComment comment) {
        if (comment.comment.contains("\n")) {
            this.builder.append("/* ")
                    .newline()
                    .append(comment.comment)
                    .append(" */");
        } else {
            this.builder.append("// ")
                    .append(comment.comment)
                    .newline();
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPDoubleLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        double value = Objects.requireNonNull(literal.value);
        String val = Double.toString(value);
        if (Double.isNaN(value))
            val = "std::f64::NAN";
        else if (Double.isInfinite(value)) {
            if (value < 0)
                val = "std::f64::NEG_INFINITY";
            else
                val = "std::f64::INFINITY";
        }
        String out = literal.raw ? val : literal.wrapSome("F64::new(" + val + ")");
        this.builder.append(out);
        return false;
    }

    @Override
    public boolean preorder(DBSPUSizeLiteral literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + "usize"));
        return false;
    }

    @Override
    public boolean preorder(DBSPISizeLiteral literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + "isize"));
        return false;
    }

    @Override
    public boolean preorder(DBSPI16Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Short.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return false;
    }

    @Override
    public boolean preorder(DBSPI32Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Integer.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return false;
    }

    @Override
    public boolean preorder(DBSPU32Literal literal) {
        String val = Integer.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return false;
    }

    @Override
    public boolean preorder(DBSPI64Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return false;
    }

    @Override
    public boolean preorder(DBSPU64Literal literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return false;
    }

    @Override
    public boolean preorder(DBSPStringLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        this.builder.append(literal.wrapSome(
                "String::from(" + Utilities.doubleQuote(Objects.requireNonNull(literal.value)) + ")"));
        return false;
    }

    @Override
    public boolean preorder(DBSPStrLiteral literal) {
        if (literal.isNull) {
            this.builder.append("None");
            return false;
        }
        Objects.requireNonNull(literal.value);
        if (literal.raw) {
            String contents = "r#\"" + literal.value + "\"#";
            this.builder.append(literal.wrapSome(contents));
        } else {
            this.builder.append(literal.wrapSome(Utilities.doubleQuote(literal.value)));
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPDecimalLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        DBSPTypeDecimal type = literal.getNonVoidType().to(DBSPTypeDecimal.class);
        if (type.mayBeNull)
            this.builder.append("Some(");
        this.builder.append("Decimal::from_str(\"")
                .append(Objects.requireNonNull(literal.value).toString())
                .append("\").unwrap()");
        if (type.mayBeNull)
            this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPCastExpression expression) {
        /*
         * Default implementation of cast of a source expression to the 'this' type.
         * Only defined for base types.
         * For example, to cast source which is an Option[i16] to a bool
         * the function called will be cast_to_b_i16N.
         */
        DBSPTypeBaseType baseDest = expression.getNonVoidType().as(DBSPTypeBaseType.class);
        if (baseDest == null)
            throw new UnsupportedException(this);
        DBSPType sourceType = expression.source.getNonVoidType();
        DBSPTypeBaseType baseSource = sourceType.as(DBSPTypeBaseType.class);
        if (baseSource == null)
            throw new UnsupportedException(sourceType);
        String destName = baseDest.shortName();
        String srcName = baseSource.shortName();
        String functionName = "cast_to_" + destName + baseDest.nullableSuffix() +
                "_" + srcName + sourceType.nullableSuffix();
        this.builder.append(functionName).append("(");
        expression.source.accept(this);
        DBSPTypeDecimal dec = baseDest.as(DBSPTypeDecimal.class);
        if (dec != null) {
            // pass precision and scale as arguments to cast method too
            this.builder.append(", ")
                    .append(dec.precision)
                    .append(", ")
                    .append(dec.scale);
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPBinaryExpression expression) {
        if (expression.primitive) {
            if (expression.left.getNonVoidType().mayBeNull) {
                this.builder.append("(")
                        .append("match (");
                expression.left.accept(this);
                this.builder.append(", ");
                expression.right.accept(this);
                this.builder.append(") {").increase()
                        .append("(Some(x), Some(y)) => Some(x ")
                        .append(expression.operation.toString())
                        .append(" y),\n")
                        .append("_ => None,\n")
                        .decrease()
                        .append("}")
                        .append(")");
            } else {
                this.builder.append("(");
                expression.left.accept(this);
                this.builder.append(" ")
                        .append(expression.operation.toString())
                        .append(" ");
                expression.right.accept(this);
                this.builder.append(")");
            }
        } else {
            if (expression.operation.equals(DBSPOpcode.MUL_WEIGHT)) {
                expression.left.accept(this);
                this.builder.append(".mul_by_ref(&");
                expression.right.accept(this);
                this.builder.append(")");
                return false;
            }
            RustSqlRuntimeLibrary.FunctionDescription function = RustSqlRuntimeLibrary.INSTANCE.getImplementation(
                    expression.operation,
                    expression.getNonVoidType(),
                    expression.left.getNonVoidType(),
                    expression.right.getNonVoidType());
            this.builder.append(function.function).append("(");
            expression.left.accept(this);
            this.builder.append(", ");
            expression.right.accept(this);
            this.builder.append(")");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPIsNullExpression expression) {
        if (!expression.expression.getNonVoidType().mayBeNull) {
            this.builder.append("false");
        } else {
            expression.expression.accept(this);
            this.builder.append(".is_none()");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPCloneExpression expression) {
        expression.expression.accept(this);
        this.builder.append(".clone()");
        return false;
    }

    @Override
    public boolean preorder(DBSPUnaryExpression expression) {
        if (expression.operation.equals(DBSPOpcode.WRAP_BOOL) ||
            expression.operation.equals(DBSPOpcode.INDICATOR)) {
            this.builder.append(expression.operation.toString())
                    .append("(");
            expression.source.accept(this);
            this.builder.append(")");
            return false;
        } else if (expression.operation.toString().startsWith("is_")) {
            this.builder.append(expression.operation.toString())
                    .append("_")
                    .append(expression.source.getNonVoidType().baseTypeWithSuffix())
                    .append("_(");
            expression.source.accept(this);
            this.builder.append(")");
            return false;
        }
        if (expression.source.getNonVoidType().mayBeNull) {
            this.builder.append("(")
                    .append("match ");
            expression.source.accept(this);
            this.builder.append(" {").increase()
                    .append("Some(x) => Some(")
                    .append(expression.operation.toString())
                    .append("(x)),\n")
                    .append("_ => None,\n")
                    .decrease()
                    .append("}")
                    .append(")");
        } else {
            this.builder.append("(")
                    .append(expression.operation.toString());
            expression.source.accept(this);
            this.builder.append(")");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPVariablePath expression) {
        this.builder.append(expression.variable);
        return false;
    }

    @Override
    public boolean preorder(DBSPConstItem item) {
        this.builder.append("const ")
                .append(item.name).append(": ");
        item.type.accept(this);
        if (item.expression != null) {
            this.builder.append(" = ");
            item.expression.accept(this);
        }
        this.builder.append(";");
        return false;
    }

    @Override
    public boolean preorder(DBSPExpressionStatement statement) {
        statement.expression.accept(this);
        if (!statement.expression.is(DBSPBlockExpression.class))
            this.builder.append(";");
        return false;
    }

    @Override
    public boolean preorder(DBSPLetStatement statement) {
        this.builder.append("let ")
                .append(statement.mutable ? "mut " : "")
                .append(statement.variable);
        if (!statement.type.is(DBSPTypeAny.class)) {
            this.builder.append(": ");
            statement.type.accept(this);
        }
        if (statement.initializer != null) {
            this.builder.append(" = ");
            statement.initializer.accept(this);
        }
        this.builder.append(";");
        return false;
    }

    @Override
    public boolean preorder(DBSPFunction function) {
        this.builder.intercalateS("\n", function.annotations)
                .append("pub fn ")
                .append(function.name)
                .append("(");
        boolean first = true;
        for (DBSPParameter param: function.parameters) {
            if (!first)
                this.builder.append(", ");
            first = false;
            param.accept(this);
        }
        this.builder.append(") ");
        if (function.returnType != null) {
            builder.append("-> ");
            function.returnType.accept(this);
        }
        if (function.body.is(DBSPBlockExpression.class)) {
            function.body.accept(this);
        } else {
            this.builder.append("\n{").increase();
            function.body.accept(this);
            this.builder.decrease()
                    .append("\n}");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPApplyExpression expression) {
        expression.function.accept(this);
        this.builder.append("(");
        boolean first = true;
        for (DBSPExpression arg: expression.arguments) {
            if (!first)
                this.builder.append(", ");
            first = false;
            arg.accept(this);
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPApplyMethodExpression expression) {
        expression.self.accept(this);
        this.builder.append(".");
        expression.function.accept(this);
        this.builder.append("(");
        boolean first = true;
        for (DBSPExpression arg: expression.arguments) {
            if (!first)
                this.builder.append(", ");
            first = false;
            arg.accept(this);
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPAsExpression expression) {
        this.builder.append("(");
        expression.source.accept(this);
        this.builder.append(" as ");
        expression.getNonVoidType().accept(this);
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPAssignmentExpression expression) {
        expression.left.accept(this);
        this.builder.append(" = ");
        expression.right.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPBlockExpression expression) {
        this.builder.append("{").increase();
        for (DBSPStatement stat: expression.contents) {
            stat.accept(this);
            this.builder.append("\n");
        }
        if (expression.lastExpression != null) {
            expression.lastExpression.accept(this);
            this.builder.append("\n");
        }
        this.builder.decrease().append("}");
        return false;
    }

    @Override
    public boolean preorder(DBSPBorrowExpression expression) {
        this.builder.append("&");
        if (expression.mut)
            this.builder.append("mut ");
        expression.expression.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPParameter parameter) {
        parameter.pattern.accept(this);
        this.builder.append(": ");
        parameter.type.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression expression) {
        this.builder.append("move |");
        for (DBSPParameter param: expression.parameters) {
            param.accept(this);
            this.builder.append(", ");
        }
        this.builder.append("| ");
        DBSPType resultType = expression.getResultType();
        if (resultType != null) {
            this.builder.append("-> ");
            resultType.accept(this);
            this.builder.append(" ");
        }
        if (expression.body.is(DBSPBlockExpression.class)) {
            expression.body.accept(this);
        } else {
            this.builder.append("{")
                    .increase();
            expression.body.accept(this);
            this.builder.decrease()
                    .append("\n}");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPDerefExpression expression) {
        this.builder.append("*");
        expression.expression.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPEnumValue expression) {
        this.builder.append(expression.enumName)
                .append("::")
                .append(expression.constructor);
        return false;
    }

    @Override
    public boolean preorder(DBSPFieldExpression expression) {
        expression.expression.accept(this);
        this.builder.append(".")
                .append(expression.fieldNo);
        return false;
    }

    @Override
    public boolean preorder(DBSPIfExpression expression) {
        builder.append("(if ");
        expression.condition.accept(this);
        this.builder.append(" ");
        if (!expression.positive.is(DBSPBlockExpression.class))
            this.builder.append("{")
                    .increase();
        expression.positive.accept(this);
        if (!expression.positive.is(DBSPBlockExpression.class))
            this.builder.decrease()
                    .append("\n}");
        this.builder.append(" else ");
        if (!expression.negative.is(DBSPBlockExpression.class))
            this.builder.append("{")
                    .increase();
        expression.negative.accept(this);
        if (!expression.negative.is(DBSPBlockExpression.class))
            this.builder.decrease()
                    .append("\n}");
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPMatchExpression.Case mCase) {
        mCase.against.accept(this);
        this.builder.append(" => ");
        mCase.result.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPMatchExpression expression) {
        this.builder.append("(match ");
        expression.matched.accept(this);
        this.builder.append(" {").increase();
        for (DBSPMatchExpression.Case mCase : expression.cases) {
            mCase.accept(this);
            this.builder.append(",\n");
        }
        this.builder.decrease()
                .append("})");
        return false;
    }

    @Override
    public boolean preorder(DBSPPathExpression expression) {
        expression.path.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPForExpression node) {
        this.builder.append("for ");
        node.pattern.accept(this);
        this.builder.append(" in ");
        node.iterated.accept(this);
        this.builder.append(" ");
        node.block.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPQualifyTypeExpression expression) {
        expression.expression.accept(this);
        this.builder.append("::<");
        for (DBSPType type: expression.types) {
            type.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(">");
        return false;
    }

    @Override
    public boolean preorder(DBSPRangeExpression expression) {
        if (expression.left != null)
            expression.left.accept(this);
        this.builder.append("..");
        if (expression.endInclusive)
            this.builder.append("=");
        if (expression.right != null)
            expression.right.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPRawTupleExpression expression) {
        this.builder.append("(");
        for (DBSPExpression field: expression.fields) {
            field.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleExpression expression) {
        if (expression.isNull)
            return this.doNullExpression(expression);
        if (expression.size() == 0) {
            this.builder.append("()");
        } else {
            this.builder.append("Tuple")
                    .append(expression.size())
                    .append("::new(");
            boolean first = true;
            for (DBSPExpression field: expression.fields) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                field.accept(this);
            }
            this.builder.append(")");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPStructExpression expression) {
        expression.function.accept(this);
        this.builder.append("(");
        if (expression.arguments.length > 0) {
            boolean first = true;
            for (DBSPExpression arg: expression.arguments) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                arg.accept(this);
            }
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPPath path) {
        boolean first = true;
        for (DBSPPathSegment segment: path.components) {
            if (!first)
                this.builder.append("::");
            first = false;
            segment.accept(this);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPSimplePathSegment segment) {
        builder.append(segment.identifier);
        if (segment.genericArgs.length > 0) {
            builder.append("::<");
            boolean first = true;
            for (DBSPType arg : segment.genericArgs) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                arg.accept(this);
            }
            this.builder.append(">");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPIdentifierPattern pattern) {
        this.builder.append(pattern.mutable ? "mut " : "")
                .append(pattern.identifier);
        return false;
    }

    @Override
    public boolean preorder(DBSPLiteralPattern pattern) {
        pattern.literal.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPRefPattern pattern) {
        this.builder.append("&")
                .append(pattern.mutable ? "mut " : "");
        pattern.pattern.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPTuplePattern pattern) {
        this.builder.append("(");
        boolean first = true;
        for (DBSPPattern field: pattern.fields) {
            if (!first)
                this.builder.append(", ");
            first = false;
            field.accept(this);
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleStructPattern pattern) {
        pattern.path.accept(this);
        this.builder.append("(");
        boolean first = true;
        for (DBSPPattern field: pattern.arguments) {
            if (!first)
                this.builder.append(", ");
            first = false;
            field.accept(this);
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPWildcardPattern pattern) {
        this.builder.append("_");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeWeight type) {
        this.builder.append(type.name);
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeAny type) {
        this.builder.append("_");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeFunction type) {
        this.builder.append("_");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeDouble type) {
        type.wrapOption(this.builder,"F64");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeDecimal type) {
        type.wrapOption(this.builder,"Decimal");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeFloat type) {
        type.wrapOption(this.builder,"F32");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeBool type) {
        type.wrapOption(this.builder,"bool");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeTimestamp type) {
        type.wrapOption(this.builder, type.getRustString());
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeInteger type) {
        type.wrapOption(this.builder, type.getRustString());
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeMillisInterval type) {
        type.wrapOption(this.builder, type.getRustString());
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeDate type) {
        type.wrapOption(this.builder, type.getRustString());
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeMonthsInterval type) {
        type.wrapOption(this.builder, type.getRustString());
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeISize type) {
        type.wrapOption(this.builder, "isize");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeUSize type) {
        type.wrapOption(this.builder, "usize");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeStr type) {
        type.wrapOption(this.builder, "str");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeString type) {
        type.wrapOption(this.builder, "String");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeNull type) {
        type.wrapOption(this.builder, "()");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeGeo type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append("GeoPoint");
        if (type.mayBeNull)
            this.builder.append(">");
        return true;
    }

    @Override
    public boolean preorder(DBSPTypeRawTuple type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append("(");
        for (DBSPType fType: type.tupFields) {
            fType.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(")");
        if (type.mayBeNull)
            this.builder.append(">");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeRef type) {
        this.builder.append("&")
                .append(type.mutable ? "mut " : "");
        type.type.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeStream type) {
        this.builder.append("Stream<")
                .append("_, "); // Circuit type
        type.elementType.accept(this);
        this.builder.append(">");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeStruct.Field field) {
        this.builder.append(field.name)
                .append(": ");
        field.type.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeStruct type) {
        this.builder.append(type.name)
                .append("{");
        boolean first = true;
        for (DBSPTypeStruct.Field field: type.args) {
            if (!first)
                this.builder.append(", ");
            first = false;
            field.accept(this);
        }
        this.builder.append("}");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeTuple type) {
        if (type.tupFields.length == 0) {
            this.builder.append("()");
            return false;
        }
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append("Tuple")
                .append(type.tupFields.length)
                .append("<");
        boolean first = true;
        for (DBSPType fType: type.tupFields) {
            if (!first)
                this.builder.append(", ");
            first = false;
            fType.accept(this);
        }
        this.builder.append(">");
        if (type.mayBeNull)
            this.builder.append(">");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeUser type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append(type.name);
        if (type.typeArgs.length > 0) {
            this.builder.append("<");
            boolean first = true;
            for (DBSPType fType: type.typeArgs) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                fType.accept(this);
            }
            this.builder.append(">");
        }
        if (type.mayBeNull)
            this.builder.append(">");
        return false;
    }

    @Override
    public boolean preorder(DBSPAggregate aggregate) {
        // Should only happen during debugging, we never generate code directly for aggregates.
        this.builder.append("Aggregate:").increase();
        for (DBSPAggregate.Implementation impl : aggregate.components)
            impl.accept(this);
        this.builder.decrease();
        return false;
    }

    @Override
    public boolean preorder(DBSPAggregate.Implementation implementation) {
        // Should only happen during debugging, we never generate code directly for aggregates.
        this.builder.append("zero=");
        implementation.zero.accept(this);
        this.builder.newline().append("increment=");
        implementation.increment.accept(this);
        if (implementation.postProcess != null) {
            this.builder.newline().append("postProcess=");
            implementation.postProcess.accept(this);
        }
        this.builder.newline().append("emptySetResult=");
        implementation.emptySetResult.accept(this);
        this.builder.newline().append("semigroup=");
        implementation.semigroup.accept(this);
        return false;
    }

    public static String toRustString(IErrorReporter reporter, IDBSPInnerNode node) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToRustInnerVisitor visitor = new ToRustInnerVisitor(reporter, stream);
        node.accept(visitor);
        return builder.toString();
    }
}
