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

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
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
    /**
     * If set use a more compact display, which is not necessarily compilable.
     */
    protected final boolean compact;

    public ToRustInnerVisitor(IErrorReporter reporter, IndentStream builder, boolean compact) {
        super(reporter);
        this.builder = builder;
        this.compact = compact;
    }

    @SuppressWarnings("SameReturnValue")
    VisitDecision doNullExpression(DBSPExpression expression) {
        this.builder.append("None::<");
        expression.getNonVoidType().setMayBeNull(false).accept(this);
        this.builder.append(">");
        return VisitDecision.STOP;
    }

    public VisitDecision doNull(DBSPLiteral literal) {
        if (!literal.isNull)
            throw new UnsupportedException(literal);
        return this.doNullExpression(literal);
    }

    @Override
    public VisitDecision preorder(DBSPNullLiteral literal) {
        return this.doNull(literal);
    }

    @Override
    public VisitDecision preorder(DBSPSortExpression expression) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSomeExpression expression) {
        this.builder.append("Some(");
        expression.expression.accept(this);
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIndexExpression expression) {
        this.builder.append("(");
        expression.array.accept(this);
        this.builder.append("[");
        expression.index.accept(this);
        if (expression.startsAtOne)
            this.builder.append("- 1");
        this.builder.append("])");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldComparatorExpression expression) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimestampLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("Timestamp::new(");
        this.builder.append(Long.toString(Objects.requireNonNull(literal.value)));
        this.builder.append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDateLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("Date::new(");
        this.builder.append(Integer.toString(Objects.requireNonNull(literal.value)));
        this.builder.append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntervalMillisLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        if (literal.mayBeNull())
            this.builder.append("Some(");
        this.builder.append("ShortInterval::new(");
        this.builder.append(Long.toString(Objects.requireNonNull(literal.value)));
        this.builder.append(")");
        if (literal.mayBeNull())
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPGeoPointLiteral literal) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBoolLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        this.builder.append(literal.wrapSome(Boolean.toString(Objects.requireNonNull(literal.value))));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVecLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        this.builder.append("vec!(")
                .increase();
        for (DBSPExpression exp: Objects.requireNonNull(literal.data)) {
            exp.accept(this);
            this.builder.append(", ");
        }
        this.builder.decrease().append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPZSetLiteral literal) {
        this.builder.append("zset!(")
                .increase();
        for (Map.Entry<DBSPExpression, Long> e: literal.data.data.entrySet()) {
            e.getKey().accept(this);
            this.builder.append(" => ")
                    .append(e.getValue())
                    .append(",\n");
        }
        this.builder.decrease().append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIndexedZSetLiteral literal) {
        this.builder.append("indexed_zset!(")
                .append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFloatLiteral literal) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPComment comment) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDoubleLiteral literal) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUSizeLiteral literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + "usize"));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPISizeLiteral literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + "isize"));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI16Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Short.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI32Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Integer.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU32Literal literal) {
        String val = Integer.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI64Literal literal) {
        if (literal.isNull)
            return this.doNull(literal);
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPU64Literal literal) {
        String val = Long.toString(Objects.requireNonNull(literal.value));
        this.builder.append(literal.wrapSome(val + literal.getIntegerType().getRustString()));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        this.builder.append(literal.wrapSome(
                "String::from(" + Utilities.doubleQuote(Objects.requireNonNull(literal.value)) + ")"));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStrLiteral literal) {
        if (literal.isNull) {
            this.builder.append("None");
            return VisitDecision.STOP;
        }
        Objects.requireNonNull(literal.value);
        if (literal.raw) {
            String contents = "r#\"" + literal.value + "\"#";
            this.builder.append(literal.wrapSome(contents));
        } else {
            this.builder.append(literal.wrapSome(Utilities.doubleQuote(literal.value)));
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDecimalLiteral literal) {
        if (literal.isNull)
            return this.doNull(literal);
        DBSPTypeDecimal type = literal.getNonVoidType().to(DBSPTypeDecimal.class);
        if (type.mayBeNull)
            this.builder.append("Some(");
        String value = Objects.requireNonNull(literal.value).toPlainString();
        this.builder.append("Decimal::from_str(\"")
                .append(value)
                .append("\").unwrap()");
        if (type.mayBeNull)
            this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCastExpression expression) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBinaryExpression expression) {
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
                return VisitDecision.STOP;
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIsNullExpression expression) {
        if (!expression.expression.getNonVoidType().mayBeNull) {
            this.builder.append("false");
        } else {
            expression.expression.accept(this);
            this.builder.append(".is_none()");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        expression.expression.accept(this);
        this.builder.append(".clone()");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUnaryExpression expression) {
        if (expression.operation.equals(DBSPOpcode.WRAP_BOOL) ||
            expression.operation.equals(DBSPOpcode.INDICATOR)) {
            this.builder.append(expression.operation.toString())
                    .append("(");
            expression.source.accept(this);
            this.builder.append(")");
            return VisitDecision.STOP;
        } else if (expression.operation.toString().startsWith("is_")) {
            this.builder.append(expression.operation.toString())
                    .append("_")
                    .append(expression.source.getNonVoidType().baseTypeWithSuffix())
                    .append("_(");
            expression.source.accept(this);
            this.builder.append(")");
            return VisitDecision.STOP;
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath expression) {
        this.builder.append(expression.variable);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPConstItem item) {
        this.builder.append("const ")
                .append(item.name).append(": ");
        item.type.accept(this);
        if (item.expression != null) {
            this.builder.append(" = ");
            item.expression.accept(this);
        }
        this.builder.append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPExpressionStatement statement) {
        statement.expression.accept(this);
        if (!statement.expression.is(DBSPBlockExpression.class))
            this.builder.append(";");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLetStatement statement) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFunction function) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyMethodExpression expression) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAsExpression expression) {
        this.builder.append("(");
        expression.source.accept(this);
        this.builder.append(" as ");
        expression.getNonVoidType().accept(this);
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAssignmentExpression expression) {
        expression.left.accept(this);
        this.builder.append(" = ");
        expression.right.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression expression) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBorrowExpression expression) {
        this.builder.append("&");
        if (expression.mut)
            this.builder.append("mut ");
        expression.expression.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPParameter parameter) {
        if (parameter.mutable)
            this.builder.append("mut ");
        this.builder.append(parameter.name);
        if (!this.compact) {
            this.builder.append(": ");
            parameter.type.accept(this);
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        this.builder.append("move |");
        for (DBSPParameter param: expression.parameters) {
            param.accept(this);
            this.builder.append(", ");
        }
        this.builder.append("| ");
        if (!this.compact) {
            DBSPType resultType = expression.getResultType();
            if (resultType != null) {
                this.builder.append("-> ").newline();
                resultType.accept(this);
                this.builder.append(" ");
            }
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDerefExpression expression) {
        this.builder.append("*");
        expression.expression.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPEnumValue expression) {
        this.builder.append(expression.enumName)
                .append("::")
                .append(expression.constructor);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression expression) {
        expression.expression.accept(this);
        this.builder.append(".")
                .append(expression.fieldNo);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIfExpression expression) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPMatchExpression.Case mCase) {
        mCase.against.accept(this);
        this.builder.append(" => ");
        mCase.result.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPMatchExpression expression) {
        this.builder.append("(match ");
        expression.matched.accept(this);
        this.builder.append(" {").increase();
        for (DBSPMatchExpression.Case mCase : expression.cases) {
            mCase.accept(this);
            this.builder.append(",\n");
        }
        this.builder.decrease()
                .append("})");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPathExpression expression) {
        expression.path.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPForExpression node) {
        this.builder.append("for ");
        node.pattern.accept(this);
        this.builder.append(" in ");
        node.iterated.accept(this);
        this.builder.append(" ");
        node.block.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPQualifyTypeExpression expression) {
        expression.expression.accept(this);
        this.builder.append("::<");
        for (DBSPType type: expression.types) {
            type.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRawTupleExpression expression) {
        this.builder.append("(");
        boolean newlines = this.compact && expression.fields.length > 2;
        if (newlines)
            this.builder.increase();
        for (DBSPExpression field: expression.fields) {
            field.accept(this);
            this.builder.append(", ");
            if (newlines)
                this.builder.newline();
        }
        if (newlines)
            this.builder.decrease();
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression expression) {
        if (expression.isNull)
            return this.doNullExpression(expression);
        if (expression.size() == 0) {
            this.builder.append("()");
        } else {
            boolean newlines = this.compact && expression.fields.length > 2;
            this.builder.append("Tuple")
                    .append(expression.size())
                    .append("::new(");
            if (newlines)
                this.builder.increase();
            boolean first = true;
            for (DBSPExpression field: expression.fields) {
                if (!first) {
                    this.builder.append(", ");
                    if (newlines)
                        this.builder.newline();
                }
                first = false;
                field.accept(this);
            }
            if (newlines)
                this.builder.decrease();
            this.builder.append(")");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStructExpression expression) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPPath path) {
        boolean first = true;
        for (DBSPPathSegment segment: path.components) {
            if (!first)
                this.builder.append("::");
            first = false;
            segment.accept(this);
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSimplePathSegment segment) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIdentifierPattern pattern) {
        this.builder.append(pattern.mutable ? "mut " : "")
                .append(pattern.identifier);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTuplePattern pattern) {
        this.builder.append("(");
        boolean first = true;
        for (DBSPPattern field: pattern.fields) {
            if (!first)
                this.builder.append(", ");
            first = false;
            field.accept(this);
        }
        this.builder.append(")");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleStructPattern pattern) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPWildcardPattern pattern) {
        this.builder.append("_");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeWeight type) {
        this.builder.append(type.name);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeAny type) {
        this.builder.append("_");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeFunction type) {
        this.builder.append("_");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeDouble type) {
        type.wrapOption(this.builder,"F64");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeDecimal type) {
        type.wrapOption(this.builder,"Decimal");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeFloat type) {
        type.wrapOption(this.builder,"F32");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeBool type) {
        type.wrapOption(this.builder,"bool");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeTimestamp type) {
        type.wrapOption(this.builder, type.getRustString());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeInteger type) {
        type.wrapOption(this.builder, type.getRustString());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeMillisInterval type) {
        type.wrapOption(this.builder, type.getRustString());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeDate type) {
        type.wrapOption(this.builder, type.getRustString());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeMonthsInterval type) {
        type.wrapOption(this.builder, type.getRustString());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeISize type) {
        type.wrapOption(this.builder, "isize");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeUSize type) {
        type.wrapOption(this.builder, "usize");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStr type) {
        type.wrapOption(this.builder, "str");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeString type) {
        type.wrapOption(this.builder, "String");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeNull type) {
        type.wrapOption(this.builder, "()");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeGeo type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append("GeoPoint");
        if (type.mayBeNull)
            this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRawTuple type) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeRef type) {
        this.builder.append("&")
                .append(type.mutable ? "mut " : "");
        type.type.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStream type) {
        this.builder.append("Stream<")
                .append("_, "); // Circuit type
        type.elementType.accept(this);
        this.builder.append(">");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct.Field field) {
        this.builder.append(field.name)
                .append(": ");
        field.type.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct type) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeTuple type) {
        if (type.tupFields.length == 0) {
            this.builder.append("()");
            return VisitDecision.STOP;
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTypeUser type) {
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
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregate aggregate) {
        // Should only happen during debugging, we never generate code directly for aggregates.
        this.builder.append("Aggregate:").increase();
        for (DBSPAggregate.Implementation impl : aggregate.components)
            impl.accept(this);
        this.builder.decrease();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPAggregate.Implementation implementation) {
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
        return VisitDecision.STOP;
    }

    public static String toRustString(IErrorReporter reporter, IDBSPInnerNode node, boolean compact) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToRustInnerVisitor visitor = new ToRustInnerVisitor(reporter, stream, compact);
        node.accept(visitor);
        return builder.toString();
    }
}
