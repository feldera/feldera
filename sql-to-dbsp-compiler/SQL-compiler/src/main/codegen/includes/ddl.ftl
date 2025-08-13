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
    final SqlNodeList otherColumnList;
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
                            type.withNullable(nullable), null, strategy, null, null, false, null, null, false);
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
    |   <FOREIGN> <KEY> columnList = ParenthesizedSimpleIdentifierList() <REFERENCES>
                 id = SimpleIdentifier() otherColumnList = ParenthesizedSimpleIdentifierList() {
            list.add(new SqlForeignKey(s.end(otherColumnList), columnList, id, otherColumnList));
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
            <INTERNED> {
               return column.setInterned();
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
    (
        AttributeDef(list)
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

SqlNodeList KeyValueList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    KeyValue(list)
    (
        <COMMA> KeyValue(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void KeyValue(List<SqlNode> list) :
{
    final SqlNode key;
    final SqlNode value;
}
{
    key = StringLiteral() { list.add(key); }
    <EQ>
    value = StringLiteral() { list.add(value); }
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

SqlCreateAggregate SqlCreateAggregate(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNodeList parameters;
    final SqlDataTypeSpec type;
    final boolean nullable;
    boolean linear = false;
}
{
    [ <LINEAR> { linear = true; } ]
    <AGGREGATE> ifNotExists = IfNotExistsOpt()
    id = SimpleIdentifier()
    parameters = AttributeDefList()
    <RETURNS>
    type = DataType()
    nullable = NullableOptDefaultTrue()
    {
        return new SqlCreateAggregate(s.end(this), replace, ifNotExists, linear,
            id, parameters, type.withNullable(nullable));
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
    SqlNodeList tableElementList;
    SqlNodeList properties = null;
}
{
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    tableElementList = ExtendedTableElementList()
    [ <WITH> properties = KeyValueList() ]
    {
        return new SqlCreateTable(s.end(this), replace, ifNotExists, id,
            tableElementList, properties);
    }
}

SqlDeclareView SqlDeclareView() :
{
    final SqlIdentifier id;
    List<SqlNode> columns = new ArrayList<SqlNode>();
    Span s, e;
}
{
   <DECLARE> { s = span(); } <RECURSIVE> <VIEW> { e = span(); }
   id = CompoundIdentifier() <LPAREN>
   ColumnDeclaration(columns)
   (
       <COMMA> ColumnDeclaration(columns)
   )*
   <RPAREN> {
        return new SqlDeclareView(s.end(this), id, new SqlNodeList(columns, e.end(this)));
   }
}

SqlCreateIndex SqlCreateIndex(Span s, boolean replace) :
{
    final SqlIdentifier id;
    final SqlIdentifier indexed;
    List<SqlNode> columns = new ArrayList<SqlNode>();
    SqlNodeList columnList;
}
{
   <INDEX>
   id = CompoundIdentifier() <ON>
   indexed = CompoundIdentifier()
   columnList = ParenthesizedSimpleIdentifierList()
   {
        return new SqlCreateIndex(s.end(this), id, indexed, columnList);
   }
}

void ColumnDeclaration(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    boolean interned = false;
    Span s;
}
{
    { s = span(); }
    id = SimpleIdentifier()
    type = DataType()
    nullable = NullableOptDefaultTrue()
    (
        <INTERNED> { interned = true; }
    )?
    {
        list.add(new SqlViewColumnDeclaration(s.add(id).end(this), id,
            type.withNullable(nullable), interned));
    }
}


SqlCreate SqlCreateView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
    SqlCreateView.ViewKind kind = SqlCreateView.ViewKind.STANDARD;
    SqlNodeList properties = null;
}
{
    [ <LOCAL> { kind = SqlCreateView.ViewKind.LOCAL; }
    | <MATERIALIZED> { kind = SqlCreateView.ViewKind.MATERIALIZED; } ]
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    [ <WITH> properties = KeyValueList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new SqlCreateView(s.end(this), replace, kind, id, columnList, properties, query);
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
