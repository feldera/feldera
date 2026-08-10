package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Converts the top-level common table expressions of a CREATE VIEW
 * statement into separate LOCAL VIEW statements.
 *
 * <p>Calcite has no relational operator for a CTE: the SqlToRelConverter
 * re-converts a CTE body at every reference.  A query whose CTE contains
 * correlations (e.g., UNNEST) and is referenced many times produces many
 * correlates that the decorrelator must solve together, which often fails.
 * Compiling each CTE as a LOCAL VIEW may make the plan tractable.
 *
 * <p>Only top-level WITH items are hoisted.  A WITH nested inside a subquery may
 * reference outer scopes so it stays inlined (it may be hoisted recursively later).
 * Recursive CTE items are never hoisted.
 *
 * <p>The rewrite runs on the SqlNode representation, after validation:
 * the validator's name resolution decides which FROM identifiers reference
 * a CTE, so shadowing (a CTE shadowing a table, or a nested WITH rebinding
 * a name) is decided by the same logic that compiles the query. */
public class CteToLocalViews {
    /** Thrown by {@code compileCreateView} to request recompilation
     * with CTEs converted to local views. */
    public static class Retry extends RuntimeException {}

    final SqlToRelCompiler compiler;

    public CteToLocalViews(SqlToRelCompiler compiler) {
        this.compiler = compiler;
    }

    /** Name of the local view generated for a CTE.  The '-' can only appear
     * in quoted user identifiers, so it's not a legal unquoted identifier. */
    static String localViewName(String view, String cte) {
        return view + "-cte-" + cte;
    }

    /** The query's top-level WITH, if the rewrite can hoist its items. */
    @Nullable
    static SqlWith hoistableWith(SqlNode query) {
        if (query instanceof SqlOrderBy orderBy)
            query = orderBy.query;
        if (!(query instanceof SqlWith with))
            return null;
        for (SqlNode node : with.withList) {
            SqlWithItem item = (SqlWithItem) node;
            if (item.recursive.booleanValue())
                return null;
        }
        return with;
    }

    public static boolean canHoist(SqlCreateView cv) {
        return hoistableWith(cv.query) != null;
    }

    /** Replaces references to hoisted CTEs with references to the
     * corresponding local views.  The original CTE name is preserved as a
     * relation alias, so column references in the query keep resolving. */
    static class RewriteCteReferences extends SqlShuttle {
        final SqlValidator validator;
        /** Maps each WITH statement to the view name to use instead. */
        final IdentityHashMap<SqlWithItem, SqlIdentifier> hoisted;

        RewriteCteReferences(SqlValidator validator,
                             IdentityHashMap<SqlWithItem, SqlIdentifier> hoisted) {
            this.validator = validator;
            this.hoisted = hoisted;
        }

        /** The local view name for a FROM identifier that references a
         * hoisted CTE; null for every other node. */
        @Nullable
        SqlIdentifier replacement(SqlNode node) {
            if (!(node instanceof SqlIdentifier id) || !id.isSimple())
                return null;
            SqlValidatorNamespace ns = this.validator.getNamespace(id);
            if (ns == null)
                return null;
            SqlValidatorNamespace resolved;
            try {
                resolved = ns.resolve();
            } catch (RuntimeException ignored) {
                return null;
            }
            if (resolved.getNode() instanceof SqlWithItem item)
                return this.hoisted.get(item);
            return null;
        }

        @Override
        public @Nullable SqlNode visit(SqlCall call) {
            // An aliased reference 'cte AS alias' keeps its alias.
            if (call.getKind() == SqlKind.AS && call.operandCount() >= 2) {
                SqlIdentifier replacement = this.replacement(call.operand(0));
                if (replacement != null) {
                    List<SqlNode> operands = new ArrayList<>(call.getOperandList());
                    operands.set(0, replacement);
                    return call.getOperator().createCall(call.getParserPosition(), operands);
                }
            }
            return super.visit(call);
        }

        @Override
        public @Nullable SqlNode visit(SqlIdentifier id) {
            SqlIdentifier replacement = this.replacement(id);
            if (replacement == null)
                return id;
            SqlIdentifier alias = new SqlIdentifier(id.getSimple(), id.getParserPosition());
            return SqlStdOperatorTable.AS.createCall(id.getParserPosition(), replacement, alias);
        }
    }

    static SqlNode deepCopy(SqlNode node) {
        SqlShuttle copier = new SqlShuttle() {
            @Override
            public SqlNode visit(SqlLiteral literal) {
                return SqlNode.clone(literal);
            }

            @Override
            public SqlNode visit(SqlIdentifier id) {
                return SqlNode.clone(id);
            }

            @Override
            public SqlNode visit(SqlDataTypeSpec type) {
                return SqlNode.clone(type);
            }

            @Override
            public SqlNode visit(SqlDynamicParam param) {
                return SqlNode.clone(param);
            }

            @Override
            public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
                return SqlNode.clone(intervalQualifier);
            }

            @Override
            public @Nullable SqlNode visit(SqlCall call) {
                CallCopyingArgHandler argHandler = new CallCopyingArgHandler(call, true);
                call.getOperator().acceptCall(this, call, false, argHandler);
                return argHandler.result();
            }
        };
        return Objects.requireNonNull(node.accept(copier));
    }

    /** Rewrite a CREATE VIEW statement whose query has top-level common
     * table expressions into one LOCAL VIEW per CTE, followed by the view
     * itself with CTE references replaced by local view references.
     * Returns null if the rewrite does not apply.
     * The statements returned must all be compiled, in order. */
    @Nullable
    public List<ParsedStatement> apply(ParsedStatement statement) {
        if (!(statement.statement() instanceof SqlCreateView cv))
            return null;
        if (!canHoist(cv))
            return null;
        SqlNode query = this.compiler.replaceRecursiveViews(cv.query);
        if (query instanceof SqlOrderBy orderBy) {
            // Move the ORDER BY inside the WITH, so that the WITH stays the
            // top node of the validated query.
            SqlWith with = (SqlWith) orderBy.query;
            query = new SqlWith(with.getParserPosition(), with.withList,
                    new SqlOrderBy(orderBy.getParserPosition(), with.body,
                            orderBy.orderList, orderBy.offset, orderBy.fetch));
        }

        // Validate with a throwaway compiler: validation mutates internal
        // validator state keyed by the query's nodes, and the statements
        // emitted below must look brand-new to the main validator.
        SqlToRelCompiler probe = new SqlToRelCompiler(this.compiler);
        SqlValidator validator = probe.getValidator();
        SqlNode validated = validator.validate(query);
        if (!(validated instanceof SqlWith with))
            // Cannot happen
            return null;

        IdentityHashMap<SqlWithItem, SqlIdentifier> hoisted = new IdentityHashMap<>();
        Set<String> usedNames = new HashSet<>();
        for (SqlNode node : with.withList) {
            SqlWithItem item = (SqlWithItem) node;
            String name = localViewName(cv.name.getSimple(), item.name.getSimple());
            // SQL allows duplicate CTE names (later ones shadow earlier ones)
            while (!usedNames.add(name))
                name = name + "-";
            Utilities.putNew(hoisted, item,
                    new SqlIdentifier(name, item.name.getParserPosition()));
        }

        RewriteCteReferences rewriter = new RewriteCteReferences(validator, hoisted);
        List<ParsedStatement> result = new ArrayList<>();
        for (SqlNode node : with.withList) {
            SqlWithItem item = (SqlWithItem) node;
            SqlNode body = Objects.requireNonNull(rewriter.visitNode(item.query));
            SqlCreateView local = new SqlCreateView(
                    item.getParserPosition(), false, SqlCreateView.ViewKind.LOCAL,
                    Utilities.getExists(hoisted, item), item.columnList, null, body);
            result.add(new ParsedStatement(deepCopy(local), statement.visible()));
        }
        SqlNode body = Objects.requireNonNull(rewriter.visitNode(with.body));
        SqlCreateView view = new SqlCreateView(
                cv.getParserPosition(), cv.getReplace(), cv.viewKind,
                cv.name, cv.columnList, cv.viewProperties, body);
        result.add(new ParsedStatement(deepCopy(view), statement.visible()));
        return result;
    }
}
