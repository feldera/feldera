package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;

import java.util.ArrayList;
import java.util.List;

/** Handle the loading of the functions from the Calcite library.
 * We create a custom SqlOperatorTable rather than loading all existing functions in each library,
 * to control which functions are visible among the many overloads available.
 * We support a mix of functions from various libraries. */
public class CalciteFunctions implements FunctionDocumentation.FunctionRegistry {
    @Override
    public List<FunctionDocumentation.FunctionDescription> getDescriptions() {
        return List.of(toLoad);
    }

    /** Describes information about a Calcite function.
     *
     * @param function      Reference to function
     * @param functionName  Function name
     * @param library       Calcite library containing the function
     * @param documentation Files where function is documented (comma-separated).
     *                      May not exist if the function is internal to Calcite.
     * @param aggregate     True if this is an aggregate function */
    record Func(SqlOperator function, String functionName, SqlLibrary library,
                String documentation, boolean aggregate) implements FunctionDocumentation.FunctionDescription { }

    final Func[] toLoad = {
            // Standard operators from SqlStdOperatorTable
            new Func(SqlStdOperatorTable.UNION, "UNION", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.UNION_ALL, "UNION ALL", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.EXCEPT, "EXCEPT", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.EXCEPT_ALL, "EXCEPT", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.INTERSECT, "INTERSECT", SqlLibrary.STANDARD, "grammar", false),

            new Func(SqlStdOperatorTable.AND, "AND", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.AS, "AS", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.DEFAULT, "DEFAULT", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.FILTER, "FILTER", SqlLibrary.STANDARD, "aggregates", false),
            new Func(SqlStdOperatorTable.CUBE, "CUBE", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.ROLLUP, "ROLLUP", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.GROUPING_SETS, "GROUPING SETS", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.GROUPING, "GROUPING", SqlLibrary.STANDARD, "grammar", false),
            // new Func(SqlStdOperatorTable.GROUP_ID, "GROUP ID", SqlLibrary.STANDARD, "grammar", false),
            // new Func(SqlStdOperatorTable.GROUPING_ID, "GROUPING ID", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.CONCAT, "||", SqlLibrary.STANDARD, "string,binary", false),
            new Func(SqlStdOperatorTable.DIVIDE, "/", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.PERCENT_REMAINDER, "%", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.DIVIDE_INTEGER, "/INT", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.DOT, ".", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.EQUALS, "=", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.GREATER_THAN, ">", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.IS_DISTINCT_FROM, "IS DISTINCT FROM", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, "IS NOT DISTINCT FROM", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ">=", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.IN, "IN", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.NOT_IN, "NOT IN", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.SEARCH, "", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.LESS_THAN, "<", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "<=", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.MINUS, "-", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.MULTIPLY, "*", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.NOT_EQUALS, "<>", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.OR, "OR", SqlLibrary.STANDARD, "boolean", false),
            new Func(SqlStdOperatorTable.PLUS, "+", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.DATETIME_PLUS, "+", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.INTERVAL, "INTERVAL", SqlLibrary.STANDARD, "datetime", false),

            new Func(SqlStdOperatorTable.DESC, "DESC", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.NULLS_FIRST, "NULLS FIRST", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.NULLS_LAST, "NULLS LAST", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.IS_NOT_NULL, "IS NOT NULL", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.IS_NULL, "IS NULL", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.IS_NOT_TRUE, "IS NOT TRUE", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.IS_TRUE, "IS TRUE", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.IS_NOT_FALSE, "IS NOT FALSE", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.IS_FALSE, "IS FALSE", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.IS_NOT_UNKNOWN, "IS NOT UNKNOWN", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.IS_UNKNOWN, "IS UNKNOWN", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.IS_EMPTY, "IS EMPTY", SqlLibrary.STANDARD, "", false),

            new Func(SqlStdOperatorTable.EXISTS, "EXISTS", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.UNIQUE, "UNIQUE", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.NOT, "NOT", SqlLibrary.STANDARD, "boolean", false),
            new Func(SqlStdOperatorTable.UNARY_MINUS, "-", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.UNARY_PLUS, "+", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.EXPLICIT_TABLE, "TABLE", SqlLibrary.STANDARD, "table", false),

            // aggregates
            new Func(SqlStdOperatorTable.AVG, "AVG", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.ARG_MAX, "ARG_MAX", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.ARG_MIN, "ARG_MIN", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.BIT_AND, "BIT_AND", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.BIT_OR, "BIT_OR", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.BIT_XOR, "BIT_XOR", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.COUNT, "COUNT", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.EVERY, "EVERY", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.MAX, "MAX", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.MIN, "MIN", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.SINGLE_VALUE, "SINGLE", SqlLibrary.STANDARD, "", true),
            new Func(SqlStdOperatorTable.SOME, "SOME", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.SUM, "SUM", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.SUM0, "SUM", SqlLibrary.STANDARD, "", true),
            new Func(SqlStdOperatorTable.STDDEV, "STDDEV", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.STDDEV_POP, "STDDEV_POP", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.STDDEV_SAMP, "STDDEV_SAMP", SqlLibrary.STANDARD, "aggregates", true),
            // window
            new Func(SqlStdOperatorTable.DENSE_RANK, "DENSE_RANK", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.LAG, "LAG", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.LEAD, "LEAD", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.RANK, "RANK", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.ROW_NUMBER, "ROW_NUMBER", SqlLibrary.STANDARD, "aggregates", true),
            // constructors from subqueries
            new Func(SqlStdOperatorTable.ARRAY_QUERY, "ARRAY", SqlLibrary.STANDARD, "aggregates", true),
            new Func(SqlStdOperatorTable.MAP_QUERY, "ROW_NUMBER", SqlLibrary.STANDARD, "aggregates", true),
            // Constructors
            new Func(SqlStdOperatorTable.ROW, "ROW", SqlLibrary.STANDARD, "types", false),
            new Func(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, "ARRAY", SqlLibrary.STANDARD, "array", false),
            new Func(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, "MAP", SqlLibrary.STANDARD, "map", false),

            new Func(SqlStdOperatorTable.IGNORE_NULLS, "IGNORE NULLS", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.RESPECT_NULLS, "RESPECT NULLS", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.MINUS_DATE, "-", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.UNNEST, "UNNEST", SqlLibrary.STANDARD, "array", false),
            new Func(SqlStdOperatorTable.UNNEST_WITH_ORDINALITY, "UNNEST WITH ORDINALITY", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.LATERAL, "LATERAL", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.COLLECTION_TABLE, "TABLE", SqlLibrary.STANDARD, "grammar", false),

            new Func(SqlStdOperatorTable.OVERLAPS, "OVERLAPS", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.BETWEEN, "BETWEEN", SqlLibrary.STANDARD, "comparisons,datetime,operators", false),
            new Func(SqlStdOperatorTable.NOT_BETWEEN, "NOT BETWEEN", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.SYMMETRIC_BETWEEN, "BETWEEN", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN, "NOT BETWEEN", SqlLibrary.STANDARD, "operators", false),
            new Func(SqlStdOperatorTable.VALUES, "VALUES", SqlLibrary.STANDARD, "grammar", false),

            new Func(SqlStdOperatorTable.NOT_LIKE, "NOT LIKE", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.LIKE, "LIKE", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.SIMILAR_TO, "SIMILAR TO", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.NOT_SIMILAR_TO, "NOT SIMILAR TO", SqlLibrary.STANDARD, "", false),

            new Func(SqlStdOperatorTable.ESCAPE, "ESCAPE", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.CASE, "CASE", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.OVER, "OVER", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.REINTERPRET, "", SqlLibrary.STANDARD, "", false),

            // Functions
            new Func(SqlStdOperatorTable.SUBSTRING, "SUBSTRING", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.REPLACE, "REPLACE", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.CONVERT, "CONVERT", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.TRANSLATE, "TRANSLATE", SqlLibrary.STANDARD, "", false),

            new Func(SqlStdOperatorTable.OVERLAY, "OVERLAY", SqlLibrary.STANDARD, "string,binary", false),
            new Func(SqlStdOperatorTable.TRIM, "TRIM", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.POSITION, "POSITION", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.CHAR_LENGTH, "CHAR_LENGTH", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.OCTET_LENGTH, "OCTET_LENGTH", SqlLibrary.STANDARD, "binary", false),
            new Func(SqlStdOperatorTable.UPPER, "UPPER", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.LOWER, "LOWER", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.INITCAP, "INITCAP", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.ASCII, "ASCII", SqlLibrary.STANDARD, "string", false),
            new Func(SqlStdOperatorTable.POWER, "POWER", SqlLibrary.STANDARD, "decimal,float", false),
            new Func(SqlStdOperatorTable.SQRT, "SQRT", SqlLibrary.STANDARD, "decimal,float", false),

            new Func(SqlStdOperatorTable.MOD, "MOD", SqlLibrary.STANDARD, "integer", false),
            new Func(SqlStdOperatorTable.LN, "LN", SqlLibrary.STANDARD, "decimal,float", false),
            new Func(SqlStdOperatorTable.LOG10, "LOG10", SqlLibrary.STANDARD, "decimal,float", false),
            new Func(SqlStdOperatorTable.ABS, "ABS", SqlLibrary.STANDARD, "decimal,float,integer", false),

            new Func(SqlStdOperatorTable.ACOS, "ACOS", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.ASIN, "ASIN", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.ATAN, "ATAN", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.ATAN2, "ATAN2", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.CBRT, "CBRT", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.COS, "COS", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.COT, "COT", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.DEGREES, "DEGREES", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.EXP, "EXP", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.RADIANS, "RADIANS", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.ROUND, "ROUND", SqlLibrary.STANDARD, "float,decimal", false),
            new Func(SqlStdOperatorTable.SIGN, "SIGN", SqlLibrary.STANDARD, "decimal", false),
            new Func(SqlStdOperatorTable.SIN, "SIN", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.TAN, "TAN", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.TRUNCATE, "TRUNCATE", SqlLibrary.STANDARD, "decimal,float", false),
            new Func(SqlStdOperatorTable.PI, "PI", SqlLibrary.STANDARD, "float", false),
            new Func(SqlStdOperatorTable.NULLIF, "NULLIF", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.COALESCE, "COALESCE", SqlLibrary.STANDARD, "comparisons", false),
            new Func(SqlStdOperatorTable.FLOOR, "FLOOR", SqlLibrary.STANDARD, "decimal,float", false),
            new Func(SqlStdOperatorTable.CEIL, "CEIL", SqlLibrary.STANDARD, "decimal,float", false),
            new Func(SqlStdOperatorTable.TIMESTAMP_ADD, "TIMESTAMPADD", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.TIMESTAMP_DIFF, "TIMESTAMPDIFF", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.CAST, "CAST", SqlLibrary.STANDARD, "casts", false),
            new Func(SqlStdOperatorTable.EXTRACT, "EXTRACT", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.YEAR, "YEAR", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.QUARTER, "QUARTER", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.MONTH, "MONTH", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.WEEK, "WEEK", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.DAYOFYEAR, "DOY", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.DAYOFMONTH, "DAY", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.DAYOFWEEK, "DOW", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.HOUR, "HOUR", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.MINUTE, "MINUTE", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.SECOND, "SECOND", SqlLibrary.STANDARD, "datetime", false),

            new Func(SqlStdOperatorTable.ELEMENT, "ELEMENT", SqlLibrary.STANDARD, "array", false),
            new Func(SqlStdOperatorTable.ITEM, "[]", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.SLICE, "$SLICE", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.ELEMENT_SLICE, "$ELEMENT_SLICE", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.SCALAR_QUERY, "$SCALAR_QUERY", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.STRUCT_ACCESS, "$STRUCT_ACCESS", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.CARDINALITY, "CARDINALITY", SqlLibrary.STANDARD, "array,map", false),

            new Func(SqlStdOperatorTable.TUMBLE, "TUMBLE", SqlLibrary.STANDARD, "table", false),
            new Func(SqlStdOperatorTable.HOP, "HOP", SqlLibrary.STANDARD, "table", false),

            // SqlLibraryOperators operators
            new Func(SqlLibraryOperators.DATEADD, "DATEADD", SqlLibrary.POSTGRESQL, "datetime", false),
            new Func(SqlLibraryOperators.DATEDIFF, "DATEDIFF", SqlLibrary.POSTGRESQL, "datetime", false),
            new Func(SqlLibraryOperators.MSSQL_CONVERT, "CONVERT", SqlLibrary.POSTGRESQL, "casts", false),
            new Func(SqlLibraryOperators.DATE_ADD, "DATE_ADD", SqlLibrary.BIG_QUERY, "datetime", false),
            new Func(SqlLibraryOperators.DATE_SUB, "DATE_SUB", SqlLibrary.BIG_QUERY, "datetime", false),
            new Func(SqlLibraryOperators.DATE_PART, "DATE_PART", SqlLibrary.POSTGRESQL, "datetime", false),
            new Func(SqlLibraryOperators.LEN, "LEN", SqlLibrary.SPARK, "string", false),
            new Func(SqlLibraryOperators.LENGTH, "LENGTH", SqlLibrary.POSTGRESQL, "string", false),
            new Func(SqlLibraryOperators.SUBSTR_BIG_QUERY, "SUBSTR", SqlLibrary.BIG_QUERY, "string", false),
            new Func(SqlLibraryOperators.SPLIT, "SPLIT", SqlLibrary.BIG_QUERY, "string", false),
            new Func(SqlLibraryOperators.SPLIT_PART, "SPLIT_PART", SqlLibrary.POSTGRESQL, "string", false),
            new Func(SqlLibraryOperators.GREATEST, "GREATEST", SqlLibrary.BIG_QUERY, "comparisons", false),
            new Func(SqlLibraryOperators.LEAST, "LEAST", SqlLibrary.BIG_QUERY, "comparisons", false),

            new Func(SqlLibraryOperators.REGEXP_REPLACE_2, "REGEXP_REPLACE", SqlLibrary.REDSHIFT, "string", false),
            new Func(SqlLibraryOperators.REGEXP_REPLACE_3, "REGEXP_REPLACE", SqlLibrary.REDSHIFT, "string", false),

            // More aggregates
            new Func(SqlLibraryOperators.BOOL_AND, "BOOL_AND", SqlLibrary.POSTGRESQL, "aggregates", true),
            new Func(SqlLibraryOperators.BOOL_OR, "BOOL_OR", SqlLibrary.POSTGRESQL, "aggregates", true),
            new Func(SqlLibraryOperators.LOGICAL_OR, "LOGICAL_OR", SqlLibrary.BIG_QUERY, "aggregates", true),
            new Func(SqlLibraryOperators.LOGICAL_AND, "COUNT", SqlLibrary.BIG_QUERY, "aggregates", true),
            new Func(SqlLibraryOperators.COUNTIF, "COUNTIF", SqlLibrary.BIG_QUERY, "aggregates", true),
            new Func(SqlLibraryOperators.ARRAY_AGG, "ARRAY_AGG", SqlLibrary.BIG_QUERY, "aggregates", true),

            new Func(SqlLibraryOperators.DATE, "DATE", SqlLibrary.BIG_QUERY, "datetime", false),
            new Func(SqlLibraryOperators.TIME, "TIME", SqlLibrary.BIG_QUERY, "datetime", false),
            new Func(SqlLibraryOperators.TIMESTAMP, "TIMESTAMP", SqlLibrary.BIG_QUERY, "datetime", false),
            new Func(SqlLibraryOperators.LEFT, "LEFT", SqlLibrary.POSTGRESQL, "string", false),
            new Func(SqlLibraryOperators.REPEAT, "REPEAT", SqlLibrary.POSTGRESQL, "string", false),
            // new Func(SqlLibraryOperators.RIGHT, "RIGHT", SqlLibrary.POSTGRESQL, "string", false),
            new Func(SqlLibraryOperators.ILIKE, "ILIKE", SqlLibrary.POSTGRESQL, "string", false),
            new Func(SqlLibraryOperators.NOT_ILIKE, "NOT ILIKE", SqlLibrary.POSTGRESQL, "string", false),
            new Func(SqlLibraryOperators.RLIKE, "RLIKE", SqlLibrary.MYSQL, "string", false),
            new Func(SqlLibraryOperators.NOT_RLIKE, "NOT RLIKE", SqlLibrary.MYSQL, "string", false),
            new Func(SqlLibraryOperators.CONCAT_FUNCTION, "CONCAT", SqlLibrary.MYSQL, "string", false),
            new Func(SqlLibraryOperators.CONCAT_WS, "CONCAT_WS", SqlLibrary.MYSQL, "string", false),
            new Func(SqlLibraryOperators.ARRAY, "ARRAY", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.MAP, "MAP", SqlLibrary.SPARK, "map", false),
            new Func(SqlLibraryOperators.ARRAY_APPEND, "ARRAY_APPEND", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_COMPACT, "ARRAY_COMPACT", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_CONCAT, "ARRAY_CONCAT", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_CONTAINS, "ARRAY_CONTAINS", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_DISTINCT, "ARRAY_DISTINCT", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_INSERT, "ARRAY_INSERT", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_JOIN, "ARRAY_JOIN", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_LENGTH, "ARRAY_LENGTH", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_MAX, "ARRAY_MAX", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_MIN, "ARRAY_MIN", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_POSITION, "ARRAY_POSITION", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_PREPEND, "ARRAY_PREPEND", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_REMOVE, "ARRAY_REMOVE", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_REPEAT, "ARRAY_REPEAT", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_REVERSE, "ARRAY_REVERSE", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_SIZE, "ARRAY_SIZE", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAY_TO_STRING, "ARRAY_TO_STRING", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.ARRAYS_OVERLAP, "ARRAYS_OVERLAP", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.SORT_ARRAY, "SORT_ARRAY", SqlLibrary.SPARK, "array", false),
            new Func(SqlLibraryOperators.TO_HEX, "TO_HEX", SqlLibrary.BIG_QUERY, "binary", false),
            new Func(SqlLibraryOperators.DATE_TRUNC, "DATE_TRUNC", SqlLibrary.BIG_QUERY, "datetime", false),
            new Func(SqlLibraryOperators.TIME_TRUNC, "TIME_TRUNC", SqlLibrary.BIG_QUERY, "datetime", false),
            new Func(SqlLibraryOperators.TIMESTAMP_TRUNC, "TIMESTAMP_TRUNC", SqlLibrary.BIG_QUERY, "datetime", false),
            new Func(SqlLibraryOperators.CHR, "CHR", SqlLibrary.POSTGRESQL, "string", false),
            new Func(SqlLibraryOperators.TANH, "TANH", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.COTH, "COTH", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.COSH, "COSH", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.ACOSH, "ACOSH", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.ASINH, "ASINH", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.ATANH, "ATANH", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.SECH, "SECH", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.CSCH, "CSCH", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.SINH, "SINH", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.CSC, "CSC", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.SEC, "SEC", SqlLibrary.ALL, "float", false),
            new Func(SqlLibraryOperators.IS_INF, "IS_INF", SqlLibrary.BIG_QUERY, "float", false),
            new Func(SqlLibraryOperators.IS_NAN, "IS_NAN", SqlLibrary.BIG_QUERY, "float", false),
            new Func(SqlLibraryOperators.LOG, "LOG", SqlLibrary.BIG_QUERY, "float", false),
            new Func(SqlLibraryOperators.INFIX_CAST, "::", SqlLibrary.POSTGRESQL, "casts", false),
            // new Func(SqlLibraryOperators.OFFSET, "OFFSET", SqlLibrary.BIG_QUERY, "", false),
            new Func(SqlLibraryOperators.SAFE_OFFSET, "SAFE_OFFSET", SqlLibrary.BIG_QUERY, "", false),
            // new Func(SqlLibraryOperators.ORDINAL, "ORDINAL", SqlLibrary.BIG_QUERY, "array", false),
            new Func(SqlLibraryOperators.TRUNC_BIG_QUERY, "TRUNC", SqlLibrary.BIG_QUERY, "decimal,float", false),
            new Func(SqlLibraryOperators.MAP_CONTAINS_KEY, "MAP_CONTAINS_KEY", SqlLibrary.SPARK, "map", false),
            // new Func(SqlLibraryOperators.SAFE_ORDINAL, "SAFE_ORDINAL", SqlLibrary.BIG_QUERY, "array", false),
    };

    private CalciteFunctions() {}

    public static final CalciteFunctions INSTANCE = new CalciteFunctions();

    /** Return all the functions supported from Calcite's libraries */
    public SqlOperatorTable getFunctions() {
        List<SqlOperator> operators = new ArrayList<>();
        for (Func func: this.toLoad) {
            if (func.library != SqlLibrary.STANDARD)
                operators.add(func.function);
        }
        return SqlOperatorTables.chain(
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                        // Always load all standard functions
                        SqlLibrary.STANDARD,
                        // There is no easy way to skip spatial functions
                        SqlLibrary.SPATIAL),
                SqlOperatorTables.of(operators));
    }
}
