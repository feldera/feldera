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
    final SqlNode e;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final ColumnStrategy strategy;
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
        (
            <PRIMARY> <KEY> /* ignored, but accepted */ {}
        |
            <FOREIGN> <KEY> <REFERENCES> SimpleIdentifier() <LPAREN> SimpleIdentifier() <RPAREN> /* ignored */ {}
        |
            /* empty */ {}
        )
        {
            strategy = nullable ? ColumnStrategy.NULLABLE : ColumnStrategy.NOT_NULLABLE;
            list.add(
                SqlDdlNodes.column(s.add(id).end(this), id,
                    type.withNullable(nullable), null, strategy));
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
        <UNIQUE> { s.add(this); }
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
        }
    |
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    |   <FOREIGN> ParenthesizedSimpleIdentifierList() <KEY> <REFERENCES>
                  SimpleIdentifier() <LPAREN> ParenthesizedSimpleIdentifierList() <RPAREN> {
            // Ignored, but parsed
        }
    )
}

SqlNodeList AttributeDefList() :
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
        attributeDefList = AttributeDefList()
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
}
{
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createView(s.end(this), replace, id, columnList,
            query);
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
