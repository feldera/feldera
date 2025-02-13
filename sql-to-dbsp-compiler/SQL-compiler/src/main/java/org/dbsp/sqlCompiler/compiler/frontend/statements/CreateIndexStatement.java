package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateIndex;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.List;

/** The representation of a CREATE INDEX ... DDL statement. */
public class CreateIndexStatement extends RelStatement {
    public final ProgramIdentifier indexName;
    public final SqlCreateIndex createIndex;
    public final ProgramIdentifier refersTo;
    public final List<ProgramIdentifier> columns;

    public CreateIndexStatement(ParsedStatement node, ProgramIdentifier indexName,
                               SqlCreateIndex createIndex) {
        super(node);
        this.indexName = indexName;
        this.createIndex = createIndex;
        this.refersTo = Utilities.toIdentifier(createIndex.indexed);
        this.columns = Linq.map(createIndex.columns, c -> Utilities.toIdentifier((SqlIdentifier) c));
    }

    public ProgramIdentifier getName() {
        return this.indexName;
    }

    public SqlParserPos columnParserPosition(int index) {
        return this.createIndex.columns.get(index).getParserPosition();
    }
}
