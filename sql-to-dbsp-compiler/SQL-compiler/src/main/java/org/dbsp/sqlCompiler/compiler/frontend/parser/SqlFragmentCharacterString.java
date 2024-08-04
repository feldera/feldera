package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

import java.util.Objects;

/** A SqlFragment that contains an arbitrary SqlCharStringLiteral */
public class SqlFragmentCharacterString implements SqlFragment {
    final SqlCharStringLiteral node;

    public SqlFragmentCharacterString(SqlCharStringLiteral node) {
        this.node = node;
    }

    /** Returns just the content of the string, without the quotes */
    @Override
    public String getString() {
        return Objects.requireNonNull(this.node.toValue());
    }

    @Override
    public SourcePositionRange getSourcePosition() {
        return new SourcePositionRange(this.node.getParserPosition());
    }
}
