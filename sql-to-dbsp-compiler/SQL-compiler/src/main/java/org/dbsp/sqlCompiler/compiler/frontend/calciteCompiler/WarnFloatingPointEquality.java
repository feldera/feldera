package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.dbsp.sqlCompiler.compiler.Documentation;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

/** Warns about the constructs of a validated SQL statement that compare floating point
 * values for equality.
 *
 * <p>Equality is explicit in {@code =}, {@code <>}, {@code IS [NOT] DISTINCT FROM},
 * {@code NULLIF}, and {@code IN}; the parser rewrites {@code CASE x WHEN v} into {@code x = v}.
 * Equality is implicit in {@code GROUP BY}, {@code DISTINCT}, {@code DISTINCT}
 * aggregates, {@code MODE}, {@code UNION}, {@code INTERSECT}, {@code EXCEPT},
 * {@code PARTITION BY}, {@code NATURAL JOIN}, {@code USING}, and in the ties of the
 * {@code RANK} family of window functions.
 *
 * <p>A value is floating point when its type is {@code REAL} or {@code DOUBLE}; a
 * {@code ROW}, {@code ARRAY}, or {@code MAP} value contains floating point values when
 * one of its component types does. */
public class WarnFloatingPointEquality extends SqlBasicVisitor<Void> {
    /** Name of the warning; silenced by {@code SET FELDERA_IGNORE_WARNING_FLOATING_POINT_EQUALITY = ON} */
    public static final String WARNING = "Floating point equality";
    public static final Documentation.Link DOCUMENTATION =
            new Documentation.Link("sql/comparisons", "comparing-floating-point-values");

    /** Operators that compare their operands for equality */
    static final Set<SqlKind> EQUALITY_OPERATORS = EnumSet.of(
            SqlKind.EQUALS, SqlKind.NOT_EQUALS,
            SqlKind.IS_DISTINCT_FROM, SqlKind.IS_NOT_DISTINCT_FROM, SqlKind.NULLIF);
    /** Window functions whose result depends on which rows tie under ORDER BY */
    static final Set<SqlKind> RANK_FUNCTIONS = EnumSet.of(
            SqlKind.RANK, SqlKind.DENSE_RANK, SqlKind.PERCENT_RANK, SqlKind.CUME_DIST);
    /** GROUP BY items that group other items rather than being expressions */
    static final Set<SqlKind> GROUPING_CONSTRUCTS = EnumSet.of(
            SqlKind.GROUPING_SETS, SqlKind.ROLLUP, SqlKind.CUBE, SqlKind.GROUP_BY_DISTINCT);
    /** Wrappers of an ORDER BY key that specify its direction or NULL placement */
    static final Set<SqlKind> ORDERING_MODIFIERS = EnumSet.of(
            SqlKind.DESCENDING, SqlKind.NULLS_FIRST, SqlKind.NULLS_LAST);

    private final SqlValidator validator;
    private final IErrorReporter reporter;
    /** Maps a position in the statement to the position reported to the user */
    private final UnaryOperator<SqlParserPos> sourcePositionRemap;
    /** The SELECT statements that enclose the node being visited, innermost first;
     * a named window is declared in the WINDOW clause of one of them */
    private final Deque<SqlSelect> enclosingSelects = new ArrayDeque<>();

    public WarnFloatingPointEquality(
            SqlValidator validator, IErrorReporter reporter,
            UnaryOperator<SqlParserPos> sourcePositionRemap) {
        this.validator = validator;
        this.reporter = reporter;
        this.sourcePositionRemap = sourcePositionRemap;
    }

    /** True if values of the type are, or contain, floating point numbers */
    public static boolean containsFloatingPoint(RelDataType type) {
        if (SqlTypeName.APPROX_TYPES.contains(type.getSqlTypeName()))
            return true;
        if (type.isStruct())
            return Linq.any(type.getFieldList(), field -> containsFloatingPoint(field.getType()));
        RelDataType component = type.getComponentType();
        if (component != null)
            return containsFloatingPoint(component);
        RelDataType key = type.getKeyType();
        RelDataType value = type.getValueType();
        if (key != null && value != null)
            return containsFloatingPoint(key) || containsFloatingPoint(value);
        return false;
    }

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlSelect select) {
            this.enclosingSelects.push(select);
            this.checkDistinct(select);
            this.checkGroupBy(select);
            super.visit(call);
            this.enclosingSelects.pop();
            return null;
        }
        if (call instanceof SqlJoin join)
            this.checkJoin(join);
        else if (call instanceof SqlPivot pivot)
            this.checkPivot(pivot);
        else if (call instanceof SqlWindow window)
            this.checkPartitionBy(window);
        else if (call.getOperator() instanceof SqlSetOperator setOperator)
            this.checkSetOperation(call, setOperator);
        else if (call.getKind() == SqlKind.OVER)
            this.checkRankTies(call);
        else if (call.getOperator() instanceof SqlAggFunction)
            this.checkAggregate(call);
        else if (call.getOperator() instanceof SqlInOperator in)
            this.checkIn(call, in);
        else if (EQUALITY_OPERATORS.contains(call.getKind()))
            this.checkOperands(call, call.getOperator().getName());
        return super.visit(call);
    }

    void checkDistinct(SqlSelect select) {
        SqlNode distinct = select.getModifierNode(SqlSelectKeyword.DISTINCT);
        if (distinct == null)
            return;
        RelDataType row = this.validator.getValidatedNodeTypeIfKnown(select);
        if (row == null || !row.isStruct())
            return;
        this.checkColumns(distinct.getParserPosition(), "DISTINCT", row.getFieldList());
    }

    void checkGroupBy(SqlSelect select) {
        SqlNodeList group = select.getGroup();
        if (group == null)
            return;
        for (SqlNode item : group)
            this.checkGroupItem(item);
    }

    void checkGroupItem(SqlNode item) {
        if (item instanceof SqlNodeList list) {
            for (SqlNode element : list)
                this.checkGroupItem(element);
        } else if (item instanceof SqlCall call && GROUPING_CONSTRUCTS.contains(call.getKind())) {
            for (SqlNode operand : call.getOperandList())
                this.checkGroupItem(operand);
        } else {
            this.checkValues(item, "GROUP BY", "");
        }
    }

    /** A UNION without ALL, an INTERSECT, or an EXCEPT matches equal rows of its inputs */
    void checkSetOperation(SqlCall call, SqlSetOperator setOperator) {
        if (setOperator.getKind() == SqlKind.UNION && setOperator.isAll())
            return;
        RelDataType row = this.validator.getValidatedNodeTypeIfKnown(call);
        if (row == null || !row.isStruct())
            return;
        this.checkColumns(call.getParserPosition(), setOperator.getName(), row.getFieldList());
    }

    /** NATURAL JOIN and USING compare the columns that the two inputs share */
    void checkJoin(SqlJoin join) {
        RelDataType left = this.rowType(join.getLeft());
        RelDataType right = this.rowType(join.getRight());
        if (left == null || right == null)
            return;
        SqlNameMatcher nameMatcher = this.validator.getCatalogReader().nameMatcher();
        List<String> shared;
        String construct;
        SqlParserPos position;
        if (join.isNatural()) {
            shared = SqlValidatorUtil.deriveNaturalJoinColumnList(nameMatcher, left, right);
            construct = "NATURAL JOIN";
            position = join.getParserPosition();
        } else if (join.getConditionType() == JoinConditionType.USING) {
            SqlNodeList columns = (SqlNodeList) join.getCondition();
            shared = Linq.map(columns, column -> ((SqlIdentifier) column).getSimple());
            construct = "USING";
            position = columns.getParserPosition();
        } else {
            return;
        }
        // A shared column is coerced to a common type, so either side may be the floating point one
        List<RelDataTypeField> fields = new ArrayList<>();
        for (String name : shared) {
            RelDataTypeField leftField = nameMatcher.field(left, name);
            RelDataTypeField rightField = nameMatcher.field(right, name);
            if (leftField != null && containsFloatingPoint(leftField.getType()))
                fields.add(leftField);
            else if (rightField != null && containsFloatingPoint(rightField.getType()))
                fields.add(rightField);
        }
        this.checkColumns(position, construct, fields);
    }

    /** PIVOT compares the FOR columns with each of the pivot values */
    void checkPivot(SqlPivot pivot) {
        for (SqlNode axis : pivot.axisList)
            this.checkValues(axis, "PIVOT", "");
    }

    void checkPartitionBy(SqlWindow window) {
        for (SqlNode key : window.getPartitionList())
            this.checkValues(key, "PARTITION BY", "");
    }

    /** RANK and its relatives give the same result to rows whose ORDER BY keys are equal */
    void checkRankTies(SqlCall over) {
        SqlCall function = over.operand(0);
        if (!RANK_FUNCTIONS.contains(function.getKind()))
            return;
        for (SqlNode key : this.orderKeys(over.operand(1))) {
            SqlNode expression = key;
            while (expression instanceof SqlCall call && ORDERING_MODIFIERS.contains(call.getKind()))
                expression = call.operand(0);
            this.checkValues(expression, function.getOperator().getName(), " to detect ties");
        }
    }

    /** The ORDER BY keys of a window, or of the named window it refers to */
    SqlNodeList orderKeys(SqlNode windowOrName) {
        SqlWindow window = windowOrName instanceof SqlWindow inline ?
                inline : this.declaredWindow((SqlIdentifier) windowOrName);
        while (window != null) {
            if (!window.getOrderList().isEmpty())
                return window.getOrderList();
            SqlIdentifier reference = window.getRefName();
            window = reference == null ? null : this.declaredWindow(reference);
        }
        return SqlNodeList.EMPTY;
    }

    /** The window that a WINDOW clause of an enclosing SELECT declares under {@code name} */
    @Nullable
    SqlWindow declaredWindow(SqlIdentifier name) {
        for (SqlSelect select : this.enclosingSelects) {
            for (SqlNode declaration : select.getWindowList()) {
                SqlWindow window = (SqlWindow) declaration;
                SqlIdentifier declared = window.getDeclName();
                if (declared != null && declared.getSimple().equalsIgnoreCase(name.getSimple()))
                    return window;
            }
        }
        return null;
    }

    /** An aggregate with DISTINCT deduplicates its arguments; MODE counts equal arguments */
    void checkAggregate(SqlCall call) {
        SqlLiteral quantifier = call.getFunctionQuantifier();
        String name = call.getOperator().getName();
        if (quantifier != null && quantifier.symbolValue(SqlSelectKeyword.class) == SqlSelectKeyword.DISTINCT)
            this.checkOperands(call, name + "(DISTINCT)");
        else if (call.getKind() == SqlKind.MODE)
            this.checkOperands(call, name);
    }

    /** IN and NOT IN test membership by equality; a quantified comparison
     * such as {@code = SOME} does so only when its comparison is an equality */
    void checkIn(SqlCall call, SqlInOperator in) {
        if (in instanceof SqlQuantifyOperator quantified
                && quantified.comparisonKind != SqlKind.EQUALS
                && quantified.comparisonKind != SqlKind.NOT_EQUALS)
            return;
        this.checkOperands(call, in.getName());
    }

    /** Warn if any operand of the call is, or contains, a floating point value */
    void checkOperands(SqlCall call, String construct) {
        for (SqlNode operand : call.getOperandList()) {
            RelDataType type = this.floatingPointType(operand);
            if (type != null) {
                this.warn(call.getParserPosition(), construct, describeValues(type), "");
                return;
            }
        }
    }

    /** Warn if the expression is, or contains, a floating point value */
    void checkValues(SqlNode expression, String construct, String suffix) {
        RelDataType type = this.floatingPointType(expression);
        if (type != null)
            this.warn(expression.getParserPosition(), construct, describeValues(type), suffix);
    }

    /** Warn if any of the columns is, or contains, a floating point value */
    void checkColumns(SqlParserPos position, String construct, List<RelDataTypeField> columns) {
        List<RelDataTypeField> floating = Linq.where(columns, field -> containsFloatingPoint(field.getType()));
        if (!floating.isEmpty())
            this.warn(position, construct, describeColumns(floating), "");
    }

    void warn(SqlParserPos position, String construct, String values, String suffix) {
        SourcePositionRange range = new SourcePositionRange(this.sourcePositionRemap.apply(position));
        this.reporter.reportWarning(range, WARNING, message(construct, values, suffix));
    }

    /** Warn if the values that {@code construct} compares for equality, whose type is
     * {@code type}, are or contain floating point values.  For the functions compiled by
     * {@link org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler}, which compare
     * their arguments or the elements of their arguments. */
    public static void checkType(IErrorReporter reporter, SourcePositionRange position,
                                 String construct, @Nullable RelDataType type) {
        if (type == null || !containsFloatingPoint(type))
            return;
        reporter.reportWarning(position, WARNING, message(construct, describeValues(type), ""));
    }

    static String message(String construct, String values, String suffix) {
        return Utilities.singleQuote(construct) + " compares " + values +
                " for equality" + suffix + ".\n" + DOCUMENTATION.citation();
    }

    /** The type of the node when it is, or contains, floating point values; for a
     * list of nodes, the type of the first such element; null otherwise. */
    @Nullable
    RelDataType floatingPointType(@Nullable SqlNode node) {
        if (node == null)
            return null;
        if (node instanceof SqlNodeList list) {
            for (SqlNode element : list) {
                RelDataType type = this.floatingPointType(element);
                if (type != null)
                    return type;
            }
            return null;
        }
        RelDataType type = this.typeOf(node);
        if (type == null || !containsFloatingPoint(type))
            return null;
        return type;
    }

    /** The type of an expression, or null if the analysis cannot determine it */
    @Nullable
    RelDataType typeOf(SqlNode expression) {
        RelDataType type = this.validator.getValidatedNodeTypeIfKnown(expression);
        if (type != null || this.enclosingSelects.isEmpty() || SqlKind.QUERY.contains(expression.getKind()))
            return type;
        // The validator resolves a plain column reference in PARTITION BY or in the ORDER BY
        // of a window without recording its type, so derive the type in the scope that
        // resolved the reference.  The analysis only warns, so a failure to derive the type
        // must not fail the compilation.
        try {
            SqlValidatorScope scope = this.validator.getSelectScope(this.enclosingSelects.peek());
            return this.validator.deriveType(scope, expression);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** The row type of a FROM item, or null if the validator did not resolve the item */
    @Nullable
    RelDataType rowType(SqlNode fromItem) {
        SqlValidatorNamespace namespace = this.validator.getNamespace(fromItem);
        return namespace == null ? null : namespace.getRowType();
    }

    static String describeValues(RelDataType type) {
        SqlTypeName name = type.getSqlTypeName();
        if (SqlTypeName.APPROX_TYPES.contains(name))
            return "floating point values of type " + name;
        return name + " values containing floating point values";
    }

    static String describeColumns(List<RelDataTypeField> columns) {
        List<String> names = Linq.map(columns, field -> Utilities.singleQuote(field.getName()));
        if (names.size() == 1)
            return "the floating point values in column " + names.get(0);
        String last = names.remove(names.size() - 1);
        String separator = names.size() == 1 ? " and " : ", and ";
        return "the floating point values in columns " + String.join(", ", names) + separator + last;
    }
}
