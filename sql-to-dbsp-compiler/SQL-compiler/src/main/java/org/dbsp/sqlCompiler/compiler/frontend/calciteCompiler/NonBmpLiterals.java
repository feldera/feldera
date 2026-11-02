package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

import java.util.Objects;
import java.util.function.UnaryOperator;

/** Hides from Calcite the string literals that hold a character outside the Basic Multilingual Plane.
 * Calcite computes on Java strings, which count UTF-16 code units, and the Feldera runtime counts
 * code points; the two agree only on strings without surrogates.  Each non-matching literal is
 * wrapped in a call to {@link Wrapper}, which Calcite cannot fold. */
public class NonBmpLiterals extends SqlShuttle {
    /** Identity function on a string literal, typed as VARCHAR */
    public static class Wrapper extends SqlInternalOperator {
        public static final Wrapper INSTANCE = new Wrapper();

        private Wrapper() {
            super("NON_BMP_LITERAL", SqlKind.OTHER, 0, true,
                    Wrapper::varcharType, null, OperandTypes.CHARACTER);
        }

        static RelDataType varcharType(SqlOperatorBinding binding) {
            RelDataTypeFactory factory = binding.getTypeFactory();
            RelDataType literalType = binding.getOperandType(0);
            RelDataType varchar = factory.createSqlType(SqlTypeName.VARCHAR);
            return factory.createTypeWithCharsetAndCollation(varchar,
                    Objects.requireNonNull(literalType.getCharset()),
                    Objects.requireNonNull(literalType.getCollation()));
        }

        @Override
        public boolean isDeterministic() {
            // Keeps the constant folding code from evaluating the call
            return false;
        }

        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            call.operand(0).unparse(writer, leftPrec, rightPrec);
        }
    }

    final UnaryOperator<SqlParserPos> sourcePositionRemap;

    public NonBmpLiterals(UnaryOperator<SqlParserPos> sourcePositionRemap) {
        this.sourcePositionRemap = sourcePositionRemap;
    }

    /** True if the value has a surrogate pair; throws if it has an unpaired surrogate */
    boolean hasSurrogatePair(SqlCharStringLiteral literal) {
        String value = Objects.requireNonNull(literal.getValueAs(NlsString.class)).getValue();
        boolean result = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!Character.isSurrogate(c))
                continue;
            if (Character.isHighSurrogate(c) && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                result = true;
                i++;
                continue;
            }
            SqlParserPos position = this.sourcePositionRemap.apply(literal.getParserPosition());
            throw new CompilationError(new SourcePositionRange(position), "Invalid string literal",
                    "String literal contains the unpaired surrogate U+" +
                            String.format("%04X", (int) c));
        }
        return result;
    }

    @Override
    public @Nullable SqlNode visit(SqlLiteral literal) {
        if (literal instanceof SqlCharStringLiteral string && this.hasSurrogatePair(string))
            return Wrapper.INSTANCE.createCall(literal.getParserPosition(), literal);
        return literal;
    }

    @Override
    public @Nullable SqlNode visit(SqlCall call) {
        if (call.getOperator() == Wrapper.INSTANCE)
            return call;
        if (call.getKind() == SqlKind.ITEM && call.operand(1) instanceof SqlLiteral) {
            // Calcite finds a ROW field only if its name is a literal
            SqlNode collection = Objects.requireNonNull(call.operand(0).accept(this));
            if (collection == call.operand(0))
                return call;
            return call.getOperator().createCall(call.getParserPosition(), collection, call.operand(1));
        }
        return super.visit(call);
    }
}
