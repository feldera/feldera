/// Adapted from calcite-ddl and calcite-BABEL
/// https://github.com/apache/calcite/tree/main/server/src/main/codegen

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlNodeList ExtendedTableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    ExtendedTableElement(list)
    (
        <COMMA> ExtendedTableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void ExtendedTableElement(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final ColumnStrategy strategy;
    SqlExtendedColumnDeclaration column = null;
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
        {
            strategy = nullable ? ColumnStrategy.NULLABLE : ColumnStrategy.NOT_NULLABLE;
            column = new SqlExtendedColumnDeclaration(s.add(id).end(this), id,
                            type.withNullable(nullable), null, strategy, null, null, false, null, null);
        }
        ( column = ColumnAttribute(column) )*
        {
            list.add(column);
        }
    |
        { list.add(id); }
    )
|
    id = SimpleIdentifier() {
        list.add(id);
    }
|
    (
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    |   <FOREIGN> <KEY> ParenthesizedSimpleIdentifierList() <REFERENCES>
                 SimpleIdentifier() ParenthesizedSimpleIdentifierList() {
            // TODO: this is currently completely ignored
        }
    )
}

SqlExtendedColumnDeclaration ColumnAttribute(SqlExtendedColumnDeclaration column) :
{
    SqlIdentifier foreignKeyTable = null;
    SqlIdentifier foreignKeyColumn = null;
    SqlNode lateness = null;
    SqlNode watermark = null;
    SqlNode e;
    Span s;
}
{
        (
            <PRIMARY> { s = span(); } <KEY> { return column.setPrimaryKey(s.end(this)); }
        |
            <FOREIGN> <KEY> <REFERENCES> foreignKeyTable = SimpleIdentifier()
            <LPAREN> foreignKeyColumn = SimpleIdentifier() <RPAREN> {
               return column.setForeignKey(foreignKeyTable, foreignKeyColumn);
            }
        |
            <LATENESS> lateness = Expression(ExprContext.ACCEPT_NON_QUERY) {
               return column.setLatenes(lateness);
            }
        |
            <WATERMARK> watermark = Expression(ExprContext.ACCEPT_NON_QUERY) {
               return column.setWatermark(watermark);
            }
        |
            <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                return column.setDefault(e);
            }
        )
}

SqlNode LatenessStatement() :
{
    final SqlIdentifier view;
    final SqlIdentifier column;
    final SqlNode lateness;
    Span s;
}
{
    <LATENESS> { s = span(); } view = SimpleIdentifier()
    <DOT> column = SimpleIdentifier()
    lateness = Expression(ExprContext.ACCEPT_NON_QUERY)
    {
         return new SqlLateness(s.end(this), view, column, lateness);
    }
}

SqlNodeList NonEmptyAttributeDefList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    AttributeDef(list)
    (
        <COMMA> AttributeDef(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlNodeList AttributeDefList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    ( AttributeDef(list)
        (
            <COMMA> AttributeDef(list)
        )*
    )?
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void AttributeDef(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    SqlNode e = null;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
    )
    [ <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) ]
    {
        list.add(SqlDdlNodes.attribute(s.add(id).end(this), id,
            type.withNullable(nullable), e, null));
    }
}

/** REMOVE FROM TABLE */
SqlNode RemoveStatement() :
{
    final SqlIdentifier tableName;
    SqlNode tableRef;
    SqlNode source;
    final SqlNodeList columnList;
    final Span s;
    final Pair<SqlNodeList, SqlNodeList> p;
}
{
    <REMOVE>
    { s = span(); }
    <FROM> tableName = CompoundTableIdentifier()
    { tableRef = tableName; }
    (
            LOOKAHEAD(2)
            p = ParenthesizedCompoundIdentifierList() {
                if (p.right.size() > 0) {
                    tableRef = extend(tableRef, p.right);
                }
                if (p.left.size() > 0) {
                    columnList = p.left;
                } else {
                    columnList = null;
                }
            }
        |   { columnList = null; }
    )
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new SqlRemove(s.end(source), tableRef, source, columnList);
    }
}

SqlCreateFunctionDeclaration SqlCreateFunction(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNodeList parameters;
    final SqlDataTypeSpec type;
    final boolean nullable;
    SqlNode body = null;
}
{
    <FUNCTION> ifNotExists = IfNotExistsOpt()
    id = SimpleIdentifier()
    parameters = AttributeDefList()
    <RETURNS>
    type = DataType()
    nullable = NullableOptDefaultTrue()
    [ <AS> body = OrderedQueryOrExpr(ExprContext.ACCEPT_NON_QUERY) ]
    {
        return new SqlCreateFunctionDeclaration(s.end(this), replace, ifNotExists,
            id, parameters, type.withNullable(nullable), body);
    }
}

SqlCreate SqlCreateType(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList attributeDefList = null;
    SqlDataTypeSpec type = null;
}
{
    <TYPE>
    id = CompoundIdentifier()
    <AS>
    (
        attributeDefList = NonEmptyAttributeDefList()
    |
        type = DataType()
    )
    {
        return SqlDdlNodes.createType(s.end(this), replace, id, attributeDefList, type);
    }
}

SqlCreate SqlCreateExtendedTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode query = null;
}
{
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = ExtendedTableElementList() ]
    [ <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ]
    {
        return SqlDdlNodes.createTable(s.end(this), replace, ifNotExists, id,
            tableElementList, query);
    }
}

SqlCreate SqlCreateView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
    boolean local = false;
}
{
    [ <LOCAL> { local = true; } ]
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new SqlCreateLocalView(s.end(this), replace, local, id, columnList, query);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropTable(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropView(s.end(this), ifExists, id);
    }
}
