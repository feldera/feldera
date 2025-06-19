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
            new Func(SqlStdOperatorTable.UNION, "UNION", SqlLibrary.STANDARD, "grammar#setop", false),
            new Func(SqlStdOperatorTable.UNION_ALL, "UNION ALL", SqlLibrary.STANDARD, "grammar#setop", false),
            new Func(SqlStdOperatorTable.EXCEPT, "EXCEPT", SqlLibrary.STANDARD, "grammar#setop", false),
            new Func(SqlStdOperatorTable.EXCEPT_ALL, "EXCEPT ALL", SqlLibrary.STANDARD, "grammar#setop", false),
            new Func(SqlStdOperatorTable.INTERSECT, "INTERSECT", SqlLibrary.STANDARD, "grammar#setop", false),

            new Func(SqlStdOperatorTable.AND, "AND", SqlLibrary.STANDARD, "boolean#and", false),
            new Func(SqlStdOperatorTable.AS, "AS", SqlLibrary.STANDARD, "grammar#as", false),
            new Func(SqlStdOperatorTable.DEFAULT, "DEFAULT", SqlLibrary.STANDARD, "grammar#default", false),
            new Func(SqlStdOperatorTable.FILTER, "FILTER", SqlLibrary.STANDARD, "aggregates#filter", false),
            new Func(SqlStdOperatorTable.CUBE, "CUBE", SqlLibrary.STANDARD, "grammar#cube", false),
            new Func(SqlStdOperatorTable.ROLLUP, "ROLLUP", SqlLibrary.STANDARD, "grammar#cube", false),
            new Func(SqlStdOperatorTable.GROUPING_SETS, "GROUPING SETS", SqlLibrary.STANDARD, "grammar#grouping-functions", false),
            new Func(SqlStdOperatorTable.GROUPING, "GROUPING", SqlLibrary.STANDARD, "grammar#grouping", false),
            // new Func(SqlStdOperatorTable.GROUP_ID, "GROUP ID", SqlLibrary.STANDARD, "grammar", false),
            // new Func(SqlStdOperatorTable.GROUPING_ID, "GROUPING ID", SqlLibrary.STANDARD, "grammar", false),
            new Func(SqlStdOperatorTable.CONCAT, "||", SqlLibrary.STANDARD, "string#concat,binary#concat", false),
            new Func(SqlStdOperatorTable.DIVIDE, "/", SqlLibrary.STANDARD, "operators#muldiv", false),
            new Func(SqlStdOperatorTable.PERCENT_REMAINDER, "%", SqlLibrary.STANDARD, "operators#muldiv", false),
            new Func(SqlStdOperatorTable.DIVIDE_INTEGER, "/", SqlLibrary.STANDARD, "operators#muldiv", false),
            new Func(SqlStdOperatorTable.DOT, ".", SqlLibrary.STANDARD, "grammar#as", false),
            new Func(SqlStdOperatorTable.EQUALS, "=", SqlLibrary.STANDARD, "comparisons#eq", false),
            new Func(SqlStdOperatorTable.GREATER_THAN, ">", SqlLibrary.STANDARD, "comparisons#gt", false),
            new Func(SqlStdOperatorTable.IS_DISTINCT_FROM, "IS DISTINCT FROM", SqlLibrary.STANDARD, "comparisons#distinct", false),
            new Func(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, "IS NOT DISTINCT FROM", SqlLibrary.STANDARD, "comparisons#notdistinct", false),
            new Func(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ">=", SqlLibrary.STANDARD, "comparisons#gte", false),
            new Func(SqlStdOperatorTable.IN, "IN", SqlLibrary.STANDARD, "comparisons#in", false),
            new Func(SqlStdOperatorTable.NOT_IN, "NOT IN", SqlLibrary.STANDARD, "comparisons#in", false),
            new Func(SqlStdOperatorTable.SEARCH, "", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.LESS_THAN, "<", SqlLibrary.STANDARD, "comparisons#lt", false),
            new Func(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "<=", SqlLibrary.STANDARD, "comparisons#lte", false),
            new Func(SqlStdOperatorTable.MINUS, "-", SqlLibrary.STANDARD, "operators#plusminus", false),
            new Func(SqlStdOperatorTable.MULTIPLY, "*", SqlLibrary.STANDARD, "operators#muldiv", false),
            new Func(SqlStdOperatorTable.NOT_EQUALS, "<>", SqlLibrary.STANDARD, "comparisons#ne", false),
            new Func(SqlStdOperatorTable.OR, "OR", SqlLibrary.STANDARD, "boolean#or", false),
            new Func(SqlStdOperatorTable.PLUS, "+", SqlLibrary.STANDARD, "operators#plusminus", false),
            new Func(SqlStdOperatorTable.DATETIME_PLUS, "+", SqlLibrary.STANDARD, "datetime#Other-datetimetimestamptime-interval-operations", false),
            new Func(SqlStdOperatorTable.INTERVAL, "INTERVAL", SqlLibrary.STANDARD, "datetime#time-intervals", false),

            new Func(SqlStdOperatorTable.DESC, "DESC", SqlLibrary.STANDARD, "grammar#order", false),
            new Func(SqlStdOperatorTable.NULLS_FIRST, "NULLS FIRST", SqlLibrary.STANDARD, "grammar#order", false),
            new Func(SqlStdOperatorTable.NULLS_LAST, "NULLS LAST", SqlLibrary.STANDARD, "grammar#order", false),
            new Func(SqlStdOperatorTable.IS_NOT_NULL, "IS NOT NULL", SqlLibrary.STANDARD, "comparisons#isnotnull,operators#isnull", false),
            new Func(SqlStdOperatorTable.IS_NULL, "IS NULL", SqlLibrary.STANDARD, "comparisons#isnull,operators#isnull", false),
            new Func(SqlStdOperatorTable.IS_NOT_TRUE, "IS NOT TRUE", SqlLibrary.STANDARD, "operators#isnull", false),
            new Func(SqlStdOperatorTable.IS_TRUE, "IS TRUE", SqlLibrary.STANDARD, "operators#isnull", false),
            new Func(SqlStdOperatorTable.IS_NOT_FALSE, "IS NOT FALSE", SqlLibrary.STANDARD, "operators#isnull", false),
            new Func(SqlStdOperatorTable.IS_FALSE, "IS FALSE", SqlLibrary.STANDARD, "operators#isnull", false),
            new Func(SqlStdOperatorTable.IS_NOT_UNKNOWN, "IS NOT UNKNOWN", SqlLibrary.STANDARD, "operators#isnull", false),
            new Func(SqlStdOperatorTable.IS_UNKNOWN, "IS UNKNOWN", SqlLibrary.STANDARD, "operators#isnull", false),
            new Func(SqlStdOperatorTable.IS_EMPTY, "IS EMPTY", SqlLibrary.STANDARD, "", false),

            new Func(SqlStdOperatorTable.EXISTS, "EXISTS", SqlLibrary.STANDARD, "comparisons#exists", false),
            new Func(SqlStdOperatorTable.UNIQUE, "UNIQUE", SqlLibrary.STANDARD, "comparisons#unique", false),
            new Func(SqlStdOperatorTable.NOT, "NOT", SqlLibrary.STANDARD, "boolean#not", false),
            new Func(SqlStdOperatorTable.UNARY_MINUS, "-", SqlLibrary.STANDARD, "operators#plusminus", false),
            new Func(SqlStdOperatorTable.UNARY_PLUS, "+", SqlLibrary.STANDARD, "operators#plusminus", false),
            new Func(SqlStdOperatorTable.EXPLICIT_TABLE, "TABLE", SqlLibrary.STANDARD, "table#syntax", false),

            // aggregates
            new Func(SqlStdOperatorTable.AVG, "AVG", SqlLibrary.STANDARD, "aggregates#avg,aggregates#window-avg", true),
            new Func(SqlStdOperatorTable.ARG_MAX, "ARG_MAX", SqlLibrary.STANDARD, "aggregates#arg_max", true),
            new Func(SqlStdOperatorTable.ARG_MIN, "ARG_MIN", SqlLibrary.STANDARD, "aggregates#arg_min", true),
            new Func(SqlStdOperatorTable.BIT_AND, "BIT_AND", SqlLibrary.STANDARD, "aggregates#bit_and", true),
            new Func(SqlStdOperatorTable.BIT_OR, "BIT_OR", SqlLibrary.STANDARD, "aggregates#bit_or", true),
            new Func(SqlStdOperatorTable.BIT_XOR, "BIT_XOR", SqlLibrary.STANDARD, "aggregates#bit_xor", true),
            new Func(SqlStdOperatorTable.COUNT, "COUNT", SqlLibrary.STANDARD,
                    "aggregates#count,aggregates#countstar,aggregates#window-count,aggregates#window-countstar", true),
            new Func(SqlStdOperatorTable.EVERY, "EVERY", SqlLibrary.STANDARD, "aggregates#every", true),
            new Func(SqlStdOperatorTable.MAX, "MAX", SqlLibrary.STANDARD, "aggregates#max,aggregates#window-max", true),
            new Func(SqlStdOperatorTable.MIN, "MIN", SqlLibrary.STANDARD, "aggregates#min,aggregates#window-min", true),
            new Func(SqlStdOperatorTable.SINGLE_VALUE, "SINGLE", SqlLibrary.STANDARD, "", true),
            new Func(SqlStdOperatorTable.SOME, "SOME", SqlLibrary.STANDARD, "aggregates#some", true),
            new Func(SqlStdOperatorTable.SUM, "SUM", SqlLibrary.STANDARD, "aggregates#sum,aggregates#window-sum", true),
            new Func(SqlStdOperatorTable.SUM0, "SUM", SqlLibrary.STANDARD, "", true),
            new Func(SqlStdOperatorTable.STDDEV, "STDDEV", SqlLibrary.STANDARD, "aggregates#stddev", true),
            new Func(SqlStdOperatorTable.STDDEV_POP, "STDDEV_POP", SqlLibrary.STANDARD, "aggregates#stddev_pop", true),
            new Func(SqlStdOperatorTable.STDDEV_SAMP, "STDDEV_SAMP", SqlLibrary.STANDARD, "aggregates#stddev_samp", true),
            // window
            new Func(SqlStdOperatorTable.DENSE_RANK, "DENSE_RANK", SqlLibrary.STANDARD, "aggregates#dense_rank", true),
            new Func(SqlStdOperatorTable.LAG, "LAG", SqlLibrary.STANDARD, "aggregates#lag", true),
            new Func(SqlStdOperatorTable.LEAD, "LEAD", SqlLibrary.STANDARD, "aggregates#lead", true),
            new Func(SqlStdOperatorTable.RANK, "RANK", SqlLibrary.STANDARD, "aggregates#rank", true),
            new Func(SqlStdOperatorTable.ROW_NUMBER, "ROW_NUMBER", SqlLibrary.STANDARD, "aggregates#row_number", true),
            // constructors from subqueries
            new Func(SqlStdOperatorTable.ARRAY_QUERY, "ARRAY", SqlLibrary.STANDARD, "aggregates#array", true),
            new Func(SqlStdOperatorTable.MAP_QUERY, "MAP", SqlLibrary.STANDARD, "aggregates#map", true),
            // Constructors
            new Func(SqlStdOperatorTable.ROW, "ROW", SqlLibrary.STANDARD, "types#row_constructor", false),
            new Func(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, "ARRAY", SqlLibrary.STANDARD, "array", false),
            new Func(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, "MAP", SqlLibrary.STANDARD, "map", false),

            new Func(SqlStdOperatorTable.IGNORE_NULLS, "IGNORE NULLS", SqlLibrary.STANDARD, "grammar#window-aggregates", false),
            new Func(SqlStdOperatorTable.RESPECT_NULLS, "RESPECT NULLS", SqlLibrary.STANDARD, "grammar#window-aggregates", false),
            new Func(SqlStdOperatorTable.MINUS_DATE, "-", SqlLibrary.STANDARD, "datetime", false),
            new Func(SqlStdOperatorTable.UNNEST, "UNNEST", SqlLibrary.STANDARD, "array#the-unnest-sql-operator", false),
            new Func(SqlStdOperatorTable.UNNEST_WITH_ORDINALITY, "UNNEST WITH ORDINALITY", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.LATERAL, "LATERAL", SqlLibrary.STANDARD, "grammar#lateral", false),
            new Func(SqlStdOperatorTable.COLLECTION_TABLE, "TABLE", SqlLibrary.STANDARD, "grammar", false),

            new Func(SqlStdOperatorTable.OVERLAPS, "OVERLAPS", SqlLibrary.STANDARD, "operators#between", false),
            new Func(SqlStdOperatorTable.BETWEEN, "BETWEEN", SqlLibrary.STANDARD, "comparisons#between,operators#between", false),
            new Func(SqlStdOperatorTable.NOT_BETWEEN, "NOT BETWEEN", SqlLibrary.STANDARD, "comparisons#notbetween", false),
            new Func(SqlStdOperatorTable.SYMMETRIC_BETWEEN, "BETWEEN", SqlLibrary.STANDARD, "operators#between", false),
            new Func(SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN, "NOT BETWEEN", SqlLibrary.STANDARD, "operators#between", false),
            new Func(SqlStdOperatorTable.VALUES, "VALUES", SqlLibrary.STANDARD, "grammar#values", false),

            new Func(SqlStdOperatorTable.NOT_LIKE, "NOT LIKE", SqlLibrary.STANDARD, "string#like", false),
            new Func(SqlStdOperatorTable.LIKE, "LIKE", SqlLibrary.STANDARD, "string#like", false),
            new Func(SqlStdOperatorTable.ESCAPE, "ESCAPE", SqlLibrary.STANDARD, "string#like", false),
            new Func(SqlStdOperatorTable.CASE, "CASE", SqlLibrary.STANDARD, "comparisons#case", false),
            new Func(SqlStdOperatorTable.OVER, "OVER", SqlLibrary.STANDARD, "grammar#window-aggregates", false),
            new Func(SqlStdOperatorTable.REINTERPRET, "", SqlLibrary.STANDARD, "", false),

            // Functions
            new Func(SqlStdOperatorTable.SUBSTRING, "SUBSTRING", SqlLibrary.STANDARD, "string#substring", false),
            new Func(SqlStdOperatorTable.REPLACE, "REPLACE", SqlLibrary.STANDARD, "string#replace", false),
            new Func(SqlStdOperatorTable.CONVERT, "CONVERT", SqlLibrary.STANDARD, "casts#casts-and-data-type-conversions", false),
            new Func(SqlStdOperatorTable.TRANSLATE, "TRANSLATE", SqlLibrary.STANDARD, "", false),

            new Func(SqlStdOperatorTable.OVERLAY, "OVERLAY", SqlLibrary.STANDARD, "string#overlay,binary#overlay", false),
            new Func(SqlStdOperatorTable.TRIM, "TRIM", SqlLibrary.STANDARD, "string#trim", false),
            new Func(SqlStdOperatorTable.POSITION, "POSITION", SqlLibrary.STANDARD, "string#position", false),
            new Func(SqlStdOperatorTable.CHAR_LENGTH, "CHAR_LENGTH", SqlLibrary.STANDARD, "string#char_length", false),
            new Func(SqlStdOperatorTable.OCTET_LENGTH, "OCTET_LENGTH", SqlLibrary.STANDARD, "binary#octet_length", false),
            new Func(SqlStdOperatorTable.UPPER, "UPPER", SqlLibrary.STANDARD, "string#upper", false),
            new Func(SqlStdOperatorTable.LOWER, "LOWER", SqlLibrary.STANDARD, "string#lower", false),
            new Func(SqlStdOperatorTable.INITCAP, "INITCAP", SqlLibrary.STANDARD, "string#initcap", false),
            new Func(SqlStdOperatorTable.ASCII, "ASCII", SqlLibrary.STANDARD, "string#ascii", false),
            new Func(SqlStdOperatorTable.POWER, "POWER", SqlLibrary.STANDARD, "decimal#power,float#power", false),
            new Func(SqlStdOperatorTable.SQRT, "SQRT", SqlLibrary.STANDARD, "decimal#sqrt,float#sqrt", false),

            new Func(SqlStdOperatorTable.MOD, "MOD", SqlLibrary.STANDARD, "integer#mod", false),
            new Func(SqlStdOperatorTable.LN, "LN", SqlLibrary.STANDARD, "decimal#ln,float#ln", false),
            new Func(SqlStdOperatorTable.LOG10, "LOG10", SqlLibrary.STANDARD, "decimal#log10,float#log10", false),
            new Func(SqlStdOperatorTable.ABS, "ABS", SqlLibrary.STANDARD, "decimal#abs,float#abs,integer#abs,datetime#abs", false),

            new Func(SqlStdOperatorTable.ACOS, "ACOS", SqlLibrary.STANDARD, "float#acos", false),
            new Func(SqlStdOperatorTable.ASIN, "ASIN", SqlLibrary.STANDARD, "float#asin", false),
            new Func(SqlStdOperatorTable.ATAN, "ATAN", SqlLibrary.STANDARD, "float#atan", false),
            new Func(SqlStdOperatorTable.ATAN2, "ATAN2", SqlLibrary.STANDARD, "float#atan2", false),
            new Func(SqlStdOperatorTable.CBRT, "CBRT", SqlLibrary.STANDARD, "float#cbrt", false),
            new Func(SqlStdOperatorTable.COS, "COS", SqlLibrary.STANDARD, "float#cos", false),
            new Func(SqlStdOperatorTable.COT, "COT", SqlLibrary.STANDARD, "float#cot", false),
            new Func(SqlStdOperatorTable.DEGREES, "DEGREES", SqlLibrary.STANDARD, "float#degrees", false),
            new Func(SqlStdOperatorTable.EXP, "EXP", SqlLibrary.STANDARD, "float#exp", false),
            new Func(SqlStdOperatorTable.RADIANS, "RADIANS", SqlLibrary.STANDARD, "float#radians", false),
            new Func(SqlStdOperatorTable.ROUND, "ROUND", SqlLibrary.STANDARD, "float#round,float#round2,decimal#round,decimal#round2", false),
            new Func(SqlStdOperatorTable.SIGN, "SIGN", SqlLibrary.STANDARD, "decimal#sign", false),
            new Func(SqlStdOperatorTable.SIN, "SIN", SqlLibrary.STANDARD, "float#sin", false),
            new Func(SqlStdOperatorTable.TAN, "TAN", SqlLibrary.STANDARD, "float#tan", false),
            new Func(SqlStdOperatorTable.TRUNCATE, "TRUNCATE", SqlLibrary.STANDARD, "decimal#truncate,float#truncate", false),
            new Func(SqlStdOperatorTable.PI, "PI", SqlLibrary.STANDARD, "float#pi", false),
            new Func(SqlStdOperatorTable.NULLIF, "NULLIF", SqlLibrary.STANDARD, "comparisons#nullif", false),
            new Func(SqlStdOperatorTable.COALESCE, "COALESCE", SqlLibrary.STANDARD, "comparisons#coalesce", false),
            new Func(SqlStdOperatorTable.FLOOR, "FLOOR", SqlLibrary.STANDARD,
                    "decimal#floor,float#floor,datetime#date_floor,datetime#timestamp_floor", false),
            new Func(SqlStdOperatorTable.CEIL, "CEIL", SqlLibrary.STANDARD,
                    "decimal#ceil,float#ceil,datetime#date_ceil,datetime#timestamp_ceil", false),
            new Func(SqlStdOperatorTable.TIMESTAMP_ADD, "TIMESTAMPADD", SqlLibrary.STANDARD,
                    "datetime#timestampadd", false),
            new Func(SqlStdOperatorTable.TIMESTAMP_DIFF, "TIMESTAMPDIFF", SqlLibrary.STANDARD,
                    "datetime#date_timestampdiff,datetime#timestamp_timestampdiff", false),
            new Func(SqlStdOperatorTable.CAST, "CAST", SqlLibrary.STANDARD, "casts#casts-and-data-type-conversions", false),
            new Func(SqlStdOperatorTable.EXTRACT, "EXTRACT", SqlLibrary.STANDARD, "datetime#time_extract,datetime#date_extract,datetime#timestamp_extract", false),
            new Func(SqlStdOperatorTable.YEAR, "YEAR", SqlLibrary.STANDARD,
                    "datetime#year", false),
            new Func(SqlStdOperatorTable.QUARTER, "QUARTER", SqlLibrary.STANDARD,
                    "datetime#quarter", false),
            new Func(SqlStdOperatorTable.MONTH, "MONTH", SqlLibrary.STANDARD,
                    "datetime#month", false),
            new Func(SqlStdOperatorTable.WEEK, "WEEK", SqlLibrary.STANDARD,
                    "datetime#week", false),
            new Func(SqlStdOperatorTable.DAYOFYEAR, "DOY", SqlLibrary.STANDARD,
                    "datetime#doy", false),
            new Func(SqlStdOperatorTable.DAYOFMONTH, "DAYOFMONTH", SqlLibrary.STANDARD,
                    "datetime#date_dayofmonth,datetime#timestamp_dayofmonth", false),
            new Func(SqlStdOperatorTable.DAYOFWEEK, "DOW", SqlLibrary.STANDARD,
                    "datetime#date_dayofweek,datetime#timestamp_dayofweek", false),
            new Func(SqlStdOperatorTable.HOUR, "HOUR", SqlLibrary.STANDARD,
                    "datetime#date_hour,datetime#time_hour,datetime#timestamp_hour", false),
            new Func(SqlStdOperatorTable.MINUTE, "MINUTE", SqlLibrary.STANDARD,
                    "datetime#date_minute,datetime#time_minute,datetime#timestamp_minute", false),
            new Func(SqlStdOperatorTable.SECOND, "SECOND", SqlLibrary.STANDARD,
                    "datetime#date_second,datetime#time_second,datetime#timestamp_second", false),

            new Func(SqlStdOperatorTable.ELEMENT, "ELEMENT", SqlLibrary.STANDARD, "array#element", false),
            new Func(SqlStdOperatorTable.ITEM, "[]", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.SLICE, "$SLICE", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.ELEMENT_SLICE, "$ELEMENT_SLICE", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.SCALAR_QUERY, "$SCALAR_QUERY", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.STRUCT_ACCESS, "$STRUCT_ACCESS", SqlLibrary.STANDARD, "", false),
            new Func(SqlStdOperatorTable.CARDINALITY, "CARDINALITY", SqlLibrary.STANDARD, "array#cardinality,map#cardinality", false),

            new Func(SqlStdOperatorTable.TUMBLE, "TUMBLE", SqlLibrary.STANDARD, "table#tumble", false),
            new Func(SqlStdOperatorTable.HOP, "HOP", SqlLibrary.STANDARD, "table#hop", false),

            // SqlLibraryOperators operators
            new Func(SqlLibraryOperators.DATEADD, "DATEADD", SqlLibrary.POSTGRESQL, "datetime#dateadd", false),
            new Func(SqlLibraryOperators.DATEDIFF, "DATEDIFF", SqlLibrary.POSTGRESQL,
                    "datetime#date_timestampdiff,datetime#timestamp_timestampdiff", false),
            new Func(SqlLibraryOperators.MSSQL_CONVERT, "CONVERT", SqlLibrary.POSTGRESQL, "casts", false),
            new Func(SqlLibraryOperators.DATE_ADD, "DATE_ADD", SqlLibrary.BIG_QUERY, "datetime#date_add", false),
            new Func(SqlLibraryOperators.DATE_SUB, "DATE_SUB", SqlLibrary.BIG_QUERY, "datetime#date_sub", false),
            new Func(SqlLibraryOperators.DATE_PART, "DATE_PART", SqlLibrary.POSTGRESQL,
                    "datetime#date_extract,datetime#time_extract,datetime#timestamp_extract", false),
            new Func(SqlLibraryOperators.IF, "IF", SqlLibrary.BIG_QUERY, "comparisons#if", false),
            new Func(SqlLibraryOperators.LEN, "LEN", SqlLibrary.SPARK, "string#char_length", false),
            new Func(SqlLibraryOperators.LENGTH, "LENGTH", SqlLibrary.POSTGRESQL, "string#char_length", false),
            new Func(SqlLibraryOperators.SUBSTR_BIG_QUERY, "SUBSTR", SqlLibrary.BIG_QUERY, "string#substr", false),
            new Func(SqlLibraryOperators.SPLIT, "SPLIT", SqlLibrary.BIG_QUERY, "string#split", false),
            new Func(SqlLibraryOperators.SPLIT_PART, "SPLIT_PART", SqlLibrary.POSTGRESQL, "string#split_part", false),
            new Func(SqlLibraryOperators.GREATEST, "GREATEST", SqlLibrary.BIG_QUERY, "comparisons#greatest", false),
            new Func(SqlLibraryOperators.LEAST, "LEAST", SqlLibrary.BIG_QUERY, "comparisons#least", false),
            new Func(SqlLibraryOperators.SAFE_CAST, "SAFE_CAST", SqlLibrary.BIG_QUERY, "casts#safe-casts", false),

            new Func(SqlLibraryOperators.REGEXP_REPLACE_2, "REGEXP_REPLACE", SqlLibrary.REDSHIFT, "string#regexp_replace", false),
            new Func(SqlLibraryOperators.REGEXP_REPLACE_3, "REGEXP_REPLACE", SqlLibrary.REDSHIFT, "string#regexp_replace", false),

            // More aggregates
            new Func(SqlLibraryOperators.BOOL_AND, "BOOL_AND", SqlLibrary.POSTGRESQL, "aggregates#logical_and", true),
            new Func(SqlLibraryOperators.BOOL_OR, "BOOL_OR", SqlLibrary.POSTGRESQL, "aggregates#logical_or", true),
            new Func(SqlLibraryOperators.LOGICAL_OR, "LOGICAL_OR", SqlLibrary.BIG_QUERY, "aggregates#logical_or", true),
            new Func(SqlLibraryOperators.LOGICAL_AND, "LOGICAL_AND", SqlLibrary.BIG_QUERY, "aggregates#logical_and", true),
            new Func(SqlLibraryOperators.COUNTIF, "COUNTIF", SqlLibrary.BIG_QUERY, "aggregates#countif", true),
            new Func(SqlLibraryOperators.ARRAY_AGG, "ARRAY_AGG", SqlLibrary.BIG_QUERY, "aggregates#array_agg", true),

            new Func(SqlLibraryOperators.DATE, "DATE", SqlLibrary.BIG_QUERY, "datetime#date-literals", false),
            new Func(SqlLibraryOperators.TIME, "TIME", SqlLibrary.BIG_QUERY, "datetime#time-literals", false),
            new Func(SqlLibraryOperators.TIMESTAMP, "TIMESTAMP", SqlLibrary.BIG_QUERY, "datetime#timestamp-literals", false),
            new Func(SqlLibraryOperators.LEFT, "LEFT", SqlLibrary.POSTGRESQL, "string#left,binary#left", false),
            new Func(SqlLibraryOperators.REPEAT, "REPEAT", SqlLibrary.POSTGRESQL, "string#repeat", false),
            new Func(SqlLibraryOperators.RIGHT, "RIGHT", SqlLibrary.POSTGRESQL, "string#right,binary#right", false),
            new Func(SqlLibraryOperators.ILIKE, "ILIKE", SqlLibrary.POSTGRESQL, "string#ilike", false),
            new Func(SqlLibraryOperators.NOT_ILIKE, "NOT ILIKE", SqlLibrary.POSTGRESQL, "string#ilike", false),
            new Func(SqlLibraryOperators.RLIKE, "RLIKE", SqlLibrary.MYSQL, "string#rlike", false),
            new Func(SqlLibraryOperators.NOT_RLIKE, "NOT RLIKE", SqlLibrary.MYSQL, "string#rlike", false),
            new Func(SqlLibraryOperators.CONCAT_FUNCTION, "CONCAT", SqlLibrary.MYSQL, "string#concat", false),
            new Func(SqlLibraryOperators.CONCAT_WS, "CONCAT_WS", SqlLibrary.MYSQL, "string#concat_ws", false),
            new Func(SqlLibraryOperators.ARRAY, "ARRAY", SqlLibrary.SPARK, "array#array", false),
            new Func(SqlLibraryOperators.MAP, "MAP", SqlLibrary.SPARK, "map", false),
            new Func(SqlLibraryOperators.ARRAY_APPEND, "ARRAY_APPEND", SqlLibrary.SPARK, "array#append", false),
            new Func(SqlLibraryOperators.ARRAY_COMPACT, "ARRAY_COMPACT", SqlLibrary.SPARK, "array#compact", false),
            new Func(SqlLibraryOperators.ARRAY_CONCAT, "ARRAY_CONCAT", SqlLibrary.SPARK, "array#concat", false),
            new Func(SqlLibraryOperators.ARRAY_DISTINCT, "ARRAY_DISTINCT", SqlLibrary.SPARK, "array#distinct", false),
            new Func(SqlLibraryOperators.ARRAY_JOIN, "ARRAY_JOIN", SqlLibrary.SPARK, "array#join", false),
            new Func(SqlLibraryOperators.ARRAY_LENGTH, "ARRAY_LENGTH", SqlLibrary.SPARK, "array#length", false),
            new Func(SqlLibraryOperators.ARRAY_MAX, "ARRAY_MAX", SqlLibrary.SPARK, "array#max", false),
            new Func(SqlLibraryOperators.ARRAY_MIN, "ARRAY_MIN", SqlLibrary.SPARK, "array#min", false),
            new Func(SqlLibraryOperators.ARRAY_PREPEND, "ARRAY_PREPEND", SqlLibrary.SPARK, "array#prepend", false),
            new Func(SqlLibraryOperators.ARRAY_REPEAT, "ARRAY_REPEAT", SqlLibrary.SPARK, "array#repeat", false),
            new Func(SqlLibraryOperators.ARRAY_REVERSE, "ARRAY_REVERSE", SqlLibrary.SPARK, "array#reverse", false),
            new Func(SqlLibraryOperators.ARRAY_SIZE, "ARRAY_SIZE", SqlLibrary.SPARK, "array#size", false),
            new Func(SqlLibraryOperators.ARRAY_TO_STRING, "ARRAY_TO_STRING", SqlLibrary.SPARK, "array#to_string", false),
            new Func(SqlLibraryOperators.SORT_ARRAY, "SORT_ARRAY", SqlLibrary.SPARK, "array#sort", false),
            new Func(SqlLibraryOperators.TO_HEX, "TO_HEX", SqlLibrary.BIG_QUERY, "binary#to_hex", false),
            new Func(SqlLibraryOperators.DATE_TRUNC, "DATE_TRUNC", SqlLibrary.BIG_QUERY, "datetime#date_trunc", false),
            new Func(SqlLibraryOperators.TIME_TRUNC, "TIME_TRUNC", SqlLibrary.BIG_QUERY, "datetime#time_trunc", false),
            new Func(SqlLibraryOperators.TIMESTAMP_TRUNC, "TIMESTAMP_TRUNC", SqlLibrary.BIG_QUERY, "datetime#timestamp_trunc", false),
            new Func(SqlLibraryOperators.CHR, "CHR", SqlLibrary.POSTGRESQL, "string#chr", false),
            new Func(SqlLibraryOperators.TANH, "TANH", SqlLibrary.ALL, "float#tanh", false),
            new Func(SqlLibraryOperators.COTH, "COTH", SqlLibrary.ALL, "float#coth", false),
            new Func(SqlLibraryOperators.COSH, "COSH", SqlLibrary.ALL, "float#cosh", false),
            new Func(SqlLibraryOperators.ACOSH, "ACOSH", SqlLibrary.ALL, "float#acosh", false),
            new Func(SqlLibraryOperators.ASINH, "ASINH", SqlLibrary.ALL, "float#asinh", false),
            new Func(SqlLibraryOperators.ATANH, "ATANH", SqlLibrary.ALL, "float#atanh", false),
            new Func(SqlLibraryOperators.SECH, "SECH", SqlLibrary.ALL, "float#sech", false),
            new Func(SqlLibraryOperators.CSCH, "CSCH", SqlLibrary.ALL, "float#csch", false),
            new Func(SqlLibraryOperators.SINH, "SINH", SqlLibrary.ALL, "float#sinh", false),
            new Func(SqlLibraryOperators.CSC, "CSC", SqlLibrary.ALL, "float#csc", false),
            new Func(SqlLibraryOperators.SEC, "SEC", SqlLibrary.ALL, "float#sec", false),
            new Func(SqlLibraryOperators.IS_INF, "IS_INF", SqlLibrary.BIG_QUERY, "float#is_inf", false),
            new Func(SqlLibraryOperators.IS_NAN, "IS_NAN", SqlLibrary.BIG_QUERY, "float#is_nan", false),
            new Func(SqlLibraryOperators.LOG, "LOG", SqlLibrary.BIG_QUERY, "float#log", false),
            new Func(SqlLibraryOperators.INFIX_CAST, "::", SqlLibrary.POSTGRESQL, "casts#coloncolon", false),
            // new Func(SqlLibraryOperators.OFFSET, "OFFSET", SqlLibrary.BIG_QUERY, "", false),
            new Func(SqlLibraryOperators.SAFE_OFFSET, "SAFE_OFFSET", SqlLibrary.BIG_QUERY, "", false),
            // new Func(SqlLibraryOperators.ORDINAL, "ORDINAL", SqlLibrary.BIG_QUERY, "array", false),
            new Func(SqlLibraryOperators.TRUNC_BIG_QUERY, "TRUNC", SqlLibrary.BIG_QUERY,
                    "decimal#trunc,decimal#trunc2,float#trunc", false),
            new Func(SqlLibraryOperators.MAP_CONTAINS_KEY, "MAP_CONTAINS_KEY", SqlLibrary.SPARK,
                    "map#map_contains_key", false),
            new Func(SqlLibraryOperators.MD5, "MD5", SqlLibrary.SPARK,
                    "string#md5,binary#md5", false),
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
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.STANDARD),
                new SqlToRelCompiler.CaseInsensitiveOperatorTable(
                        SqlOperatorTables.spatialInstance().getOperatorList()),
                new SqlToRelCompiler.CaseInsensitiveOperatorTable(operators));
    }
}
