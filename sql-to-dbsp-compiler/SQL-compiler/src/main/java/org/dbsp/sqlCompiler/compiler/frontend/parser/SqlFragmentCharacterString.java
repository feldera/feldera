package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

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
    public SqlParserPos getParserPosition() {
        return this.node.getParserPosition();
    }
}
