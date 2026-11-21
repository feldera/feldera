package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateIndex;
import org.dbsp.util.Linq;

import java.util.List;

/** The representation of a CREATE INDEX ... DDL statement. */
public class CreateIndexStatement extends RelStatement {
    public final ProgramIdentifier indexName;
    public final ProgramIdentifier refersTo;
    public final List<ProgramIdentifier> columns;
    public final List<SqlParserPos> columnPositions;
    public final SqlParserPos namePosition;
    public final SqlParserPos targetPosition;

    public CreateIndexStatement(ParsedStatement node, ProgramIdentifier indexName,
                               SqlCreateIndex createIndex) {
        this(node, indexName, ProgramIdentifier.fromSqlId(createIndex.indexed),
                Linq.map(createIndex.columns, c -> ProgramIdentifier.fromSqlId((SqlIdentifier) c)),
                Linq.map(createIndex.columns, c -> c.getParserPosition()),
                createIndex.name.getParserPosition(), createIndex.indexed.getParserPosition());
    }

    public CreateIndexStatement(ParsedStatement node, ProgramIdentifier indexName,
                               ProgramIdentifier refersTo, List<ProgramIdentifier> columns,
                               List<SqlParserPos> columnPositions, SqlParserPos namePosition, SqlParserPos targetPosition) {
        super(node);
        if (columns.size() != columnPositions.size())
            throw new IllegalArgumentException("Index columns and positions must have equal lengths");
        this.indexName = indexName;
        this.refersTo = refersTo;
        this.columns = List.copyOf(columns);
        this.columnPositions = List.copyOf(columnPositions);
        this.namePosition = namePosition;
        this.targetPosition = targetPosition;
    }

    public ProgramIdentifier getName() {
        return this.indexName;
    }

    public SqlParserPos columnParserPosition(int index) {
        return this.columnPositions.get(index);
    }
}
