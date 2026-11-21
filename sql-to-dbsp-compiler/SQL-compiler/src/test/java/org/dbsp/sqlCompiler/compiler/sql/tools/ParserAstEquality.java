package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Litmus;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Calcite AST equality with explicit value comparison for sample specifications.
 * SqlSampleSpec has no equals implementation, so ordinary equalsDeep rejects
 * even two independent parses of identical TABLESAMPLE SQL. */
final class ParserAstEquality {
    private ParserAstEquality() { }

    private static final class SampleLiteral extends SqlLiteral {
        SampleLiteral(SqlLiteral literal) {
            super(literal.getValue(), literal.getTypeName(), literal.getParserPosition());
            Set<String> fields = Arrays.stream(SqlSampleSpec.SqlTableSampleSpec.class.getDeclaredFields())
                    .filter(field -> !Modifier.isStatic(field.getModifiers()))
                    .map(java.lang.reflect.Field::getName).collect(Collectors.toSet());
            if (!fields.equals(Set.of("bernoulli", "sampleRate", "repeatable", "repeatableSeed"))
                    || Arrays.stream(SqlSampleSpec.class.getDeclaredFields())
                        .anyMatch(field -> !Modifier.isStatic(field.getModifiers())))
                throw new IllegalStateException("Unreviewed SqlSampleSpec fields");
        }

        @Override public boolean equalsDeep(SqlNode node, Litmus litmus) {
            if (!(node instanceof SampleLiteral other)) return litmus.fail("Sample literal kinds differ");
            var left = (SqlSampleSpec.SqlTableSampleSpec) Objects.requireNonNull(this.getValue());
            var right = (SqlSampleSpec.SqlTableSampleSpec) Objects.requireNonNull(other.getValue());
            boolean equal = this.getTypeName() == other.getTypeName()
                    && left.isBernoulli() == right.isBernoulli()
                    && left.sampleRate.equals(right.sampleRate)
                    && left.isRepeatable() == right.isRepeatable()
                    && left.getRepeatableSeed() == right.getRepeatableSeed();
            return equal ? litmus.succeed() : litmus.fail("Sample specification fields differ");
        }
    }

    private static SqlNode sampleValues(SqlNode node) {
        return Objects.requireNonNull(node.accept(new SqlShuttle() {
            @Override public SqlNode visit(SqlLiteral literal) {
                Object value = literal.getValue();
                if (value != null && value.getClass() == SqlSampleSpec.SqlTableSampleSpec.class)
                    return new SampleLiteral(literal);
                return literal;
            }
        }));
    }

    static boolean equal(SqlNode left, SqlNode right) {
        return sampleValues(left).equalsDeep(sampleValues(right), Litmus.IGNORE);
    }
}
