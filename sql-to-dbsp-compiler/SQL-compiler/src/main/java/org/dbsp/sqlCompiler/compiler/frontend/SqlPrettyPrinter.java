package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/** Formats a {@link SqlNode} tree as SQL text using an {@link IIndentStream}. */
// Calcite has an unparse method, but it has lots of options and bugs. This printer has no options.
public class SqlPrettyPrinter {
    private static final UnaryOperator<SqlWriterConfig> UNQUOTED_CONFIG =
            c -> c.withDialect(CalciteSqlDialect.DEFAULT).withQuoteAllIdentifiers(false);

    /**
     * Wraps a sorted list of {@link SqlComment}s with a cursor.
     * Given a position, {@link #before(SqlParserPos)} returns every comment
     * whose source position strictly precedes that position, then advances
     * the cursor past those comments.
     */
    private static class CommentStream {
        private final List<SqlComment> comments;
        private int cursor;

        CommentStream(List<SqlComment> comments) {
            this.comments = comments;
            this.cursor = 0;
        }

        /** Returns {@code true} if {@code a} appears strictly before {@code b} in the source. */
        private static boolean isBefore(SqlParserPos a, SqlParserPos b) {
            int aLine = a.getLineNum();
            int bLine = b.getLineNum();
            if (aLine != bLine) return aLine < bLine;
            return a.getColumnNum() < b.getColumnNum();
        }

        /**
         * Returns all comments whose source position is strictly before {@code pos},
         * advancing the cursor past them.
         */
        List<SqlComment> before(SqlParserPos pos) {
            List<SqlComment> result = new ArrayList<>();
            while (this.cursor < this.comments.size()) {
                SqlComment c = requireNonNull(this.comments.get(this.cursor));
                if (!isBefore(requireNonNull(c.pos), pos)) break;
                result.add(c);
                this.cursor++;
            }
            return result;
        }

        /** Returns all remaining comments and advances the cursor to the end. */
        List<SqlComment> remaining() {
            List<SqlComment> result = new ArrayList<>(this.comments.subList(this.cursor, this.comments.size()));
            this.cursor = this.comments.size();
            return result;
        }
    }

    private final IIndentStream stream;
    private final CommentStream commentStream;

    public SqlPrettyPrinter(IIndentStream stream, List<SqlComment> comments) {
        this.stream = stream;
        this.commentStream = new CommentStream(comments);
    }

    public SqlPrettyPrinter(IIndentStream stream) {
        this(stream, new ArrayList<>());
    }

    /**
     * Emit all comments whose source line precedes {@code node}'s start position,
     * then advance the comment cursor past them.
     */
    private void flushBefore(SqlNode node) {
        for (SqlComment comment : this.commentStream.before(requireNonNull(node.getParserPosition()))) {
            this.stream.append(requireNonNull(comment.text.stripTrailing())).newline();
        }
    }

    /** Emit all remaining comments that follow every printed node. */
    public void flushRemainingComments() {
        for (SqlComment comment : this.commentStream.remaining()) {
            this.stream.append(requireNonNull(comment.text.stripTrailing())).newline();
        }
    }

    /** Format {@code node} as a SQL string. */
    public static String toString(SqlNode node) {
        StringBuilder sb = new StringBuilder();
        IndentStream stream = new IndentStream(sb);
        new SqlPrettyPrinter(stream).print(node);
        return sb.toString();
    }

    public static String toString(SqlNodeList nodes) {
        return toString(nodes, List.of());
    }

    /**
     * Format a list of SQL statements, interleaving comments from the original source
     * at the correct positions — both between statements and inside them (e.g., between
     * columns of a CREATE TABLE).
     */
    public static String toString(SqlNodeList nodes, List<SqlComment> comments) {
        StringBuilder sb = new StringBuilder();
        IndentStream stream = new IndentStream(sb);
        SqlPrettyPrinter printer = new SqlPrettyPrinter(stream, comments);
        for (SqlNode node : nodes) {
            printer.print(requireNonNull(node));
            stream.append(";").newline();
        }
        printer.flushRemainingComments();
        return sb.toString();
    }

    /** Get the SQL using the Calcite implementation */
    private String fallbackSql(SqlNode node) {
        return node.toSqlString(UNQUOTED_CONFIG).getSql();
    }

    /** Append the SQL text for {@code node} to the this.stream. */
    public void print(@Nullable SqlNode node) {
        if (node == null) return;
        this.flushBefore(node);

        // Feldera-specific DDL nodes (check before SqlCall)
        if (node instanceof SqlCreateView) { this.printCreateView((SqlCreateView) node); return; }
        if (node instanceof SqlCreateTable) { this.printCreateTable((SqlCreateTable) node); return; }
        // Standard Calcite query nodes
        if (node instanceof SqlSelect)   { this.printSelect((SqlSelect) node); return; }
        if (node instanceof SqlOrderBy)  { this.printOrderBy((SqlOrderBy) node); return; }
        if (node instanceof SqlWith)     { this.printWith((SqlWith) node); return; }
        if (node instanceof SqlJoin)     { this.printJoin((SqlJoin) node); return; }

        if (node instanceof SqlCase)        { this.printCase((SqlCase) node); return; }
        if (node instanceof SqlBasicCall)   { this.printBasicCall((SqlBasicCall) node); return; }

        // Fallback: use Calcite's own unparse without forcing quotes on all identifiers
        this.stream.append(this.fallbackSql(node));
    }

    /**
     * Print a node that appears inside an expression context.
     * A SELECT sub-query is wrapped in parentheses and indented.
     */
    private void printExpr(@Nullable SqlNode node) {
        if (node == null) return;
        SqlKind kind = node.getKind();
        if (node instanceof SqlSelect
                || node instanceof SqlOrderBy
                || node instanceof SqlWith
                || kind == SqlKind.UNION
                || kind == SqlKind.INTERSECT
                || kind == SqlKind.EXCEPT
                || kind == SqlKind.VALUES) {
            this.printQueryParens(node);
        } else {
            this.print(node);
        }
    }

    private void printQueryParens(SqlNode query) {
        this.stream.append("(").increase();
        this.print(query);
        this.stream.decrease().newline().append(")");
    }

    /** Print the RHS of an IN / NOT IN expression.
     *  A value list {@code (1, 2, 3)} is a {@link SqlNodeList} and needs explicit parens.
     *  A subquery is handled by {@link #printExpr}. */
    private void printInList(SqlNode rhs) {
        if (rhs instanceof SqlNodeList list) {
            this.stream.append("(");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) this.stream.append(", ");
                this.printExpr(requireNonNull(list.get(i)));
            }
            this.stream.append(")");
        } else {
            this.printExpr(rhs);
        }
    }

    private void printSelect(SqlSelect select) {
        this.stream.append("SELECT");

        if (select.hasHints()) {
            this.stream.append(" /*+");
            SqlNodeList hints = requireNonNull(select.getHints());
            for (int i = 0; i < hints.size(); i++) {
                if (i > 0) this.stream.append(",");
                this.stream.append(" ").append(this.fallbackSql(requireNonNull(hints.get(i))));
            }
            this.stream.append(" */");
        }

        if (select.isDistinctOn()) {
            this.stream.append(" DISTINCT ON (");
            SqlNodeList distinctOn = requireNonNull(select.getDistinctOn());
            for (int i = 0; i < distinctOn.size(); i++) {
                if (i > 0) this.stream.append(", ");
                this.printExpr(requireNonNull(distinctOn.get(i)));
            }
            this.stream.append(")");
        } else if (select.isDistinct()) {
            this.stream.append(" DISTINCT");
        } else if (select.getModifierNode(SqlSelectKeyword.ALL) != null) {
            this.stream.append(" ALL");
        }

        // SELECT list — one item per line, indented
        SqlNodeList selectList = select.getSelectList();
        if (selectList != null && !selectList.isEmpty()) {
            this.stream.increase();
            for (int i = 0; i < selectList.size(); i++) {
                if (i > 0) this.stream.append(",").newline();
                this.printExpr(requireNonNull(selectList.get(i)));
            }
            this.stream.decrease();
        }

        SqlNode from = select.getFrom();
        if (from != null) {
            this.stream.newline().append("FROM ");
            this.printFrom(from);
        }

        SqlNode where = select.getWhere();
        if (where != null) {
            this.stream.newline().append("WHERE ");
            this.printExpr(where);
        }

        SqlNodeList groupBy = select.getGroup();
        if (groupBy != null && !groupBy.isEmpty()) {
            this.stream.newline().append("GROUP BY ");
            for (int i = 0; i < groupBy.size(); i++) {
                if (i > 0) this.stream.append(", ");
                this.printExpr(groupBy.get(i));
            }
        }

        SqlNode having = select.getHaving();
        if (having != null) {
            this.stream.newline().append("HAVING ");
            this.printExpr(having);
        }

        SqlNodeList windowDecls = select.getWindowList();
        if (windowDecls != null && !windowDecls.isEmpty()) {
            this.stream.newline().append("WINDOW ");
            for (int i = 0; i < windowDecls.size(); i++) {
                if (i > 0) this.stream.append(", ");
                this.print(windowDecls.get(i));
            }
        }

        SqlNode qualify = select.getQualify();
        if (qualify != null) {
            this.stream.newline().append("QUALIFY ");
            this.printExpr(qualify);
        }

        SqlNodeList orderBy = select.getOrderList();
        if (orderBy != null && !orderBy.isEmpty()) {
            this.stream.newline().append("ORDER BY ");
            for (int i = 0; i < orderBy.size(); i++) {
                if (i > 0) this.stream.append(", ");
                this.printExpr(orderBy.get(i));
            }
        }

        SqlNode offset = select.getOffset();
        if (offset != null) {
            this.stream.newline().append("OFFSET ");
            this.printExpr(offset);
        }

        SqlNode fetch = select.getFetch();
        if (fetch != null) {
            this.stream.newline().append("LIMIT ");
            this.printExpr(fetch);
        }
    }

    private void printFrom(SqlNode from) {
        if (from instanceof SqlJoin) {
            this.printJoin((SqlJoin) from);
        } else {
            this.printExpr(from);
        }
    }

    private void printJoin(SqlJoin join) {
        this.printFrom(join.getLeft());
        this.stream.newline();
        boolean natural = join.isNatural();
        switch (join.getJoinType()) {
            case INNER: this.stream.append(natural ? "NATURAL INNER JOIN " : "INNER JOIN "); break;
            case LEFT:  this.stream.append(natural ? "NATURAL LEFT JOIN "  : "LEFT JOIN ");  break;
            case RIGHT: this.stream.append(natural ? "NATURAL RIGHT JOIN " : "RIGHT JOIN "); break;
            case FULL:  this.stream.append(natural ? "NATURAL FULL JOIN "  : "FULL JOIN ");  break;
            case CROSS: this.stream.append(natural ? "NATURAL CROSS JOIN " : "CROSS JOIN "); break;
            case COMMA: this.stream.append(", "); break;
            default:    this.stream.append("JOIN "); break;
        }
        SqlNode rightSide = join.getRight();
        if (rightSide instanceof SqlJoin) {
            // A nested join on the right must be parenthesized; otherwise
            // "JOIN (B JOIN C ON c1) ON c2" would print as "JOIN B JOIN C ON c1 ON c2"
            // which the parser rejects (second ON appears where a join type is expected).
            this.stream.append("(").increase();
            this.printJoin((SqlJoin) rightSide);
            this.stream.decrease().newline().append(")");
        } else {
            this.printExpr(rightSide);
        }
        JoinConditionType condType = join.getConditionType();
        SqlNode cond = join.getCondition();
        if (condType == JoinConditionType.ON && cond != null) {
            this.stream.append(" ON ");
            this.printExpr(cond);
        } else if (condType == JoinConditionType.USING && cond != null) {
            this.stream.append(" USING (");
            this.printExpr(cond);
            this.stream.append(")");
        }
    }

    private void printOrderBy(SqlOrderBy orderBy) {
        this.print(requireNonNull(orderBy.query));
        if (!orderBy.orderList.isEmpty()) {
            this.stream.newline().append("ORDER BY ");
            for (int i = 0; i < orderBy.orderList.size(); i++) {
                if (i > 0) this.stream.append(", ");
                this.printExpr(orderBy.orderList.get(i));
            }
        }
        if (orderBy.offset != null) {
            this.stream.newline().append("OFFSET ");
            this.printExpr(orderBy.offset);
        }
        if (orderBy.fetch != null) {
            this.stream.newline().append("LIMIT ");
            this.printExpr(orderBy.fetch);
        }
    }

    private void printWith(SqlWith with) {
        this.stream.append("WITH").increase();
        for (int i = 0; i < with.withList.size(); i++) {
            if (i > 0) this.stream.append(",").newline();
            SqlWithItem item = (SqlWithItem) with.withList.get(i);
            this.stream.append(this.fallbackSql(requireNonNull(item.name)));
            this.stream.append(" AS ");
            this.printQueryParens(item.query);
        }
        this.stream.decrease().newline();
        this.print(with.body);
    }

    private void printCase(SqlCase caseExpr) {
        SqlNode value = caseExpr.getValueOperand();
        if (value != null) {
            this.stream.append("CASE ");
            this.printExpr(value);
        } else {
            this.stream.append("CASE");
        }
        SqlNodeList whenList = caseExpr.getWhenOperands();
        SqlNodeList thenList = caseExpr.getThenOperands();
        for (int i = 0; i < whenList.size(); i++) {
            this.stream.append(" WHEN ");
            this.printExpr(requireNonNull(whenList.get(i)));
            this.stream.append(" THEN ");
            this.printExpr(requireNonNull(thenList.get(i)));
        }
        SqlNode elseExpr = caseExpr.getElseOperand();
        if (elseExpr != null) {
            this.stream.append(" ELSE ");
            this.printExpr(elseExpr);
        }
        this.stream.append(" END");
    }

    private void printBasicCall(SqlBasicCall call) {
        SqlOperator op = call.getOperator();
        List<@Nullable SqlNode> operands = call.getOperandList();
        SqlKind kind = op.getKind();

        switch (kind) {
            case AS:
                this.printExpr(requireNonNull(operands.get(0)));
                this.stream.append(" AS ");
                // Alias is always a simple identifier — no parens needed
                this.print(operands.get(1));
                return;
            case CAST: {
                this.stream.append("CAST(");
                this.printExpr(requireNonNull(operands.get(0)));
                this.stream.append(" AS ");
                SqlNode castType = requireNonNull(operands.get(1));
                if (castType instanceof org.apache.calcite.sql.SqlIntervalQualifier) {
                    this.stream.append("INTERVAL ");
                }
                this.print(castType);
                this.stream.append(")");
                return;
            }
            case DESCENDING:
                this.printExpr(requireNonNull(operands.get(0)));
                this.stream.append(" DESC");
                return;
            case NULLS_FIRST:
                this.printExpr(requireNonNull(operands.get(0)));
                this.stream.append(" NULLS FIRST");
                return;
            case NULLS_LAST:
                this.printExpr(requireNonNull(operands.get(0)));
                this.stream.append(" NULLS LAST");
                return;
            case IN:
                this.printExpr(requireNonNull(operands.get(0)));
                this.stream.append(" IN ");
                this.printInList(requireNonNull(operands.get(1)));
                return;
            case NOT_IN:
                this.printExpr(requireNonNull(operands.get(0)));
                this.stream.append(" NOT IN ");
                this.printInList(requireNonNull(operands.get(1)));
                return;
            case LATERAL:
                this.stream.append("LATERAL ");
                this.printExpr(requireNonNull(operands.get(0)));
                return;
            case FILTER:
                this.printExpr(requireNonNull(operands.get(0)));
                this.stream.append(" FILTER (WHERE ");
                this.printExpr(requireNonNull(operands.get(1)));
                this.stream.append(")");
                return;
            case WITHIN_GROUP: {
                this.printExpr(requireNonNull(operands.get(0)));
                this.stream.append(" WITHIN GROUP (ORDER BY ");
                SqlNodeList orderList = (SqlNodeList) requireNonNull(operands.get(1));
                for (int i = 0; i < orderList.size(); i++) {
                    if (i > 0) this.stream.append(", ");
                    this.printExpr(requireNonNull(orderList.get(i)));
                }
                this.stream.append(")");
                return;
            }
            default:
                break;
        }

        if (operands.size() == 2 && op.getSyntax() == org.apache.calcite.sql.SqlSyntax.BINARY) {
            this.printExpr(operands.get(0));
            this.stream.append(" ").append(op.getName()).append(" ");
            this.printExpr(operands.get(1));
            return;
        } else if (operands.size() == 1 && op.getSyntax() == org.apache.calcite.sql.SqlSyntax.POSTFIX) {
            this.printExpr(operands.get(0));
            this.stream.append(" ").append(op.getName());
            return;
        } else if (operands.size() == 1 && op.getSyntax() == org.apache.calcite.sql.SqlSyntax.PREFIX) {
            this.stream.append(op.getName()).append(" ");
            this.printExpr(operands.get(0));
            return;
        }

        // Fallback: let Calcite's own unparse() handle everything else
        this.stream.append(this.fallbackSql(call));
    }

    private void printCreateView(SqlCreateView view) {
        if (view.getReplace()) {
            this.stream.append("CREATE OR REPLACE");
        } else {
            this.stream.append("CREATE");
        }
        switch (view.viewKind) {
            case LOCAL:        this.stream.append(" LOCAL"); break;
            case MATERIALIZED: this.stream.append(" MATERIALIZED"); break;
            default: break;
        }
        this.stream.append(" VIEW ");
        this.stream.append(this.fallbackSql(requireNonNull(view.name)));

        SqlNodeList cols = view.columnList;
        if (cols != null && !cols.isEmpty()) {
            this.stream.append("(");
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) this.stream.append(", ");
                this.print(cols.get(i));
            }
            this.stream.append(")");
        }

        this.printProperties(view.viewProperties);
        this.stream.append(" AS").newline();
        this.print(view.query);
    }

    private void printCreateTable(SqlCreateTable table) {
        this.stream.append("CREATE TABLE ");
        if (table.ifNotExists) this.stream.append("IF NOT EXISTS ");
        this.stream.append(this.fallbackSql(table.name));
        this.stream.append(" (").increase();
        for (int i = 0; i < table.columnsOrForeignKeys.size(); i++) {
            if (i > 0) this.stream.append(",").newline();
            this.print(requireNonNull(table.columnsOrForeignKeys.get(i)));
        }
        this.stream.decrease().newline().append(")");
        this.printProperties(table.tableProperties);
    }

    /** Format a WITH (key = value, ...) property list. */
    private void printProperties(@Nullable SqlNodeList properties) {
        if (properties == null || properties.isEmpty()) return;
        this.stream.append(" WITH (").increase();
        for (int i = 0; i < properties.size(); i += 2) {
            if (i > 0) this.stream.append(",").newline();
            SqlNode keyNode = requireNonNull(properties.get(i));
            SqlNode valueNode = requireNonNull(properties.get(i + 1));
            this.print(keyNode);
            this.stream.append(" = ");

            // Pretty-print the 'connectors' value as formatted JSON
            boolean emitted = false;
            if (keyNode instanceof SqlLiteral key
                    && valueNode instanceof SqlLiteral value
                    && Objects.equals(key.toValue(), "connectors")) {
                String raw = value.toValue();
                if (raw == null) {
                    this.stream.append("null");
                } else {
                    String formatted = Utilities.tryFormatAsJson(raw, this.stream.getIndentAmount());
                    if (!formatted.equals(raw)) {
                        this.stream.append("'" + formatted.replace("'", "''") + "'");
                        emitted = true;
                    }
                }
            }
            if (!emitted) {
                this.print(valueNode);
            }
        }
        this.stream.decrease().newline().append(")");
    }
}
