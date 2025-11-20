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
     * @param testedBy      Python files where function is tested (comma-separated).
     *                      May not exist if the function is internal.
     * @param aggregate     True if this is an aggregate function */
    record Func(SqlOperator function, String functionName, SqlLibrary library,
                String documentation, String testedBy, boolean aggregate)
            implements FunctionDocumentation.FunctionDescription { }

    final Func[] toLoad = {
            // Standard operators from SqlStdOperatorTable
            new Func(SqlStdOperatorTable.UNION, "UNION", SqlLibrary.STANDARD, "grammar#setop", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.UNION_ALL, "UNION ALL", SqlLibrary.STANDARD, "grammar#setop", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.EXCEPT, "EXCEPT", SqlLibrary.STANDARD, "grammar#setop", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.EXCEPT_ALL, "EXCEPT ALL", SqlLibrary.STANDARD, "grammar#setop", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.INTERSECT, "INTERSECT", SqlLibrary.STANDARD, "grammar#setop", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.AND, "AND", SqlLibrary.STANDARD, "boolean#and", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.AS, "AS", SqlLibrary.STANDARD, "grammar#as", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.DEFAULT, "DEFAULT", SqlLibrary.STANDARD, "grammar#default", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.FILTER, "FILTER", SqlLibrary.STANDARD, "aggregates#filter", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.CUBE, "CUBE", SqlLibrary.STANDARD, "grammar#cube", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ROLLUP, "ROLLUP", SqlLibrary.STANDARD, "grammar#cube", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.GROUPING_SETS, "GROUPING SETS", SqlLibrary.STANDARD, "grammar#grouping-functions", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.GROUPING, "GROUPING", SqlLibrary.STANDARD, "grammar#grouping", FunctionDocumentation.NO_FILE, false),
            // new Func(SqlStdOperatorTable.GROUP_ID, "GROUP ID", SqlLibrary.STANDARD, "grammar", FunctionDocumentation.NO_FILE, false),
            // new Func(SqlStdOperatorTable.GROUPING_ID, "GROUPING ID", SqlLibrary.STANDARD, "grammar", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.CONCAT, "||", SqlLibrary.STANDARD, "string#concat,binary#concat", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.DIVIDE, "/", SqlLibrary.STANDARD, "operators#muldiv", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.PERCENT_REMAINDER, "%", SqlLibrary.STANDARD, "operators#muldiv", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.DIVIDE_INTEGER, "/", SqlLibrary.STANDARD, "operators#muldiv", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.DOT, ".", SqlLibrary.STANDARD, "grammar#as", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.EQUALS, "=", SqlLibrary.STANDARD, "comparisons#eq", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.GREATER_THAN, ">", SqlLibrary.STANDARD, "comparisons#gt", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_DISTINCT_FROM, "IS DISTINCT FROM", SqlLibrary.STANDARD, "comparisons#distinct", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, "IS NOT DISTINCT FROM", SqlLibrary.STANDARD, "comparisons#notdistinct", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ">=", SqlLibrary.STANDARD, "comparisons#gte", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IN, "IN", SqlLibrary.STANDARD, "comparisons#in", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.NOT_IN, "NOT IN", SqlLibrary.STANDARD, "comparisons#in", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.SEARCH, "", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.LESS_THAN, "<", SqlLibrary.STANDARD, "comparisons#lt", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "<=", SqlLibrary.STANDARD, "comparisons#lte", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.MINUS, "-", SqlLibrary.STANDARD, "operators#plusminus", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.MULTIPLY, "*", SqlLibrary.STANDARD, "operators#muldiv", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.NOT_EQUALS, "<>", SqlLibrary.STANDARD, "comparisons#ne", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.OR, "OR", SqlLibrary.STANDARD, "boolean#or", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.PLUS, "+", SqlLibrary.STANDARD, "operators#plusminus", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.DATETIME_PLUS, "+", SqlLibrary.STANDARD, "datetime#Other-datetimetimestamptime-interval-operations", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.INTERVAL, "INTERVAL", SqlLibrary.STANDARD, "datetime#time-intervals", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.DESC, "DESC", SqlLibrary.STANDARD, "grammar#order", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.NULLS_FIRST, "NULLS FIRST", SqlLibrary.STANDARD, "grammar#order", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.NULLS_LAST, "NULLS LAST", SqlLibrary.STANDARD, "grammar#order", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_NOT_NULL, "IS NOT NULL", SqlLibrary.STANDARD, "comparisons#isnotnull,operators#isnull", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_NULL, "IS NULL", SqlLibrary.STANDARD, "comparisons#isnull,operators#isnull", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_NOT_TRUE, "IS NOT TRUE", SqlLibrary.STANDARD, "operators#isnull", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_TRUE, "IS TRUE", SqlLibrary.STANDARD, "operators#isnull", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_NOT_FALSE, "IS NOT FALSE", SqlLibrary.STANDARD, "operators#isnull", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_FALSE, "IS FALSE", SqlLibrary.STANDARD, "operators#isnull", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_NOT_UNKNOWN, "IS NOT UNKNOWN", SqlLibrary.STANDARD, "operators#isnull", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_UNKNOWN, "IS UNKNOWN", SqlLibrary.STANDARD, "operators#isnull", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.IS_EMPTY, "IS EMPTY", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.EXISTS, "EXISTS", SqlLibrary.STANDARD, "comparisons#exists", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.NOT, "NOT", SqlLibrary.STANDARD, "boolean#not", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.UNARY_MINUS, "-", SqlLibrary.STANDARD, "operators#plusminus", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.UNARY_PLUS, "+", SqlLibrary.STANDARD, "operators#plusminus", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.EXPLICIT_TABLE, "TABLE", SqlLibrary.STANDARD, "table#syntax", FunctionDocumentation.NO_FILE, false),

            // aggregates
            new Func(SqlStdOperatorTable.AVG, "AVG", SqlLibrary.STANDARD, "aggregates#avg,aggregates#window-avg",
                    "runtime_aggtest/aggregate_tests/test_{avg,decimal_avg,empty_set}.py|runtime_aggtest/aggregate_tests3/test_un_int_avg.py", true),
            new Func(SqlStdOperatorTable.ARG_MAX, "ARG_MAX", SqlLibrary.STANDARD, "aggregates#arg_max",
                    """
                    runtime_aggtest/aggregate_tests/test_{arg_max,decimal_arg_max,empty_set,row_arg_max}.py|
                    runtime_aggtest/aggregate_tests2/test_{charn_argmax,date_arg_max,interval_arg_max,time_arg_max,timestamp_arg_max}.py|
                    runtime_aggtest/aggregate_tests3/test_{binary_arg_max,empty_set,un_int_arg_max,varbinary_arg_max}.py|
                    runtime_aggtest/aggregate_tests4/test_{array_arg_max,map_arg_max,uuid,varchar_argmax,varcharn_argmax}.py|
                    runtime_aggtest/aggregate_tests6/test_interval_mths_argmax.py
                    """, true),
            new Func(SqlStdOperatorTable.ARG_MIN, "ARG_MIN", SqlLibrary.STANDARD, "aggregates#arg_min",
                    """
                    runtime_aggtest/aggregate_tests/test_{arg_min,decimal_arg_min,empty_set,row_arg_min}.py|
                    runtime_aggtest/aggregate_tests2/test_{charn_argmin,date_arg_min,interval_arg_min,time_arg_min,timestamp_arg_min}.py|
                    runtime_aggtest/aggregate_tests3/test_{binary_arg_min,empty_set,un_int_arg_min,varbinary_arg_min}.py|
                    runtime_aggtest/aggregate_tests4/test_{array_arg_min,map_arg_min,uuid,varchar_argmin,varcharn_argmin}.py|
                    runtime_aggtest/aggregate_tests6/test_interval_mths_argmin.py
                    """, true),
            new Func(SqlStdOperatorTable.BIT_AND, "BIT_AND", SqlLibrary.STANDARD, "aggregates#bit_and",
                    "runtime_aggtest/aggregate_tests/test_bit_and.py", true),
            new Func(SqlStdOperatorTable.BIT_OR, "BIT_OR", SqlLibrary.STANDARD, "aggregates#bit_or",
                    "runtime_aggtest/aggregate_tests/test_bit_or.py", true),
            new Func(SqlStdOperatorTable.BIT_XOR, "BIT_XOR", SqlLibrary.STANDARD, "aggregates#bit_xor",
                    "runtime_aggtest/aggregate_tests/test_bit_xor.py", true),
            new Func(SqlStdOperatorTable.COUNT, "COUNT", SqlLibrary.STANDARD,
                    "aggregates#count,aggregates#countstar,aggregates#window-count,aggregates#window-countstar",
                    """
                    runtime_aggtest/aggregate_tests/test_{count,count_col,decimal_count,decimal_count_col,row_count_col,empty_set}.py|
                    runtime_aggtest/aggregate_tests2/test_{charn_count,charn_count_col,date_count,date_count_col,interval_count,interval_count_col,time_count,time_count_col,timestamp_count,timestamp_count_col}.py|
                    runtime_aggtest/aggregate_tests3/test_{binary_count,binary_count_col,un_int_count,un_int_count_col,un_int_countif,varbinary_count,varbinary_count_col,empty_set}.py|
                    runtime_aggtest/aggregate_tests4/test_{array_count,array_count_col,map_count,map_count_col,varchar_count,varchar_count_col,varcharn_count,varcharn_count_col}.py|
                    runtime_aggtest/aggregate_tests6/test_{interval_count_mths,interval_count_col_mths}.py
                    """, true),
            new Func(SqlStdOperatorTable.EVERY, "EVERY", SqlLibrary.STANDARD, "aggregates#every",
                    """
                    runtime_aggtest/aggregate_tests/test_{every,decimal_every,row_every,empty_set}.py|
                    runtime_aggtest/aggregate_tests2/test_{charn_every,date_every,interval_every,time_every,timestamp_every}.py|
                    runtime_aggtest/aggregate_tests3/test_{binary_every,un_int_every,varbinary_every,empty_set}.py|
                    runtime_aggtest/aggregate_tests4/test_{array_every,map_every,varchar_every,varcharn_every}.py|
                    runtime_aggtest/aggregate_tests6/test_{interval_mths_every}.py
                    """, true),
            new Func(SqlStdOperatorTable.MAX, "MAX", SqlLibrary.STANDARD, "aggregates#max,aggregates#window-max", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.MIN, "MIN", SqlLibrary.STANDARD, "aggregates#min,aggregates#window-min", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.SINGLE_VALUE, "SINGLE", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.SOME, "SOME", SqlLibrary.STANDARD, "aggregates#some", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.SUM, "SUM", SqlLibrary.STANDARD, "aggregates#sum,aggregates#window-sum", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.SUM0, "SUM", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.STDDEV, "STDDEV", SqlLibrary.STANDARD, "aggregates#stddev", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.STDDEV_POP, "STDDEV_POP", SqlLibrary.STANDARD, "aggregates#stddev_pop", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.STDDEV_SAMP, "STDDEV_SAMP", SqlLibrary.STANDARD, "aggregates#stddev_samp", FunctionDocumentation.NO_FILE, true),
            // window
            new Func(SqlStdOperatorTable.DENSE_RANK, "DENSE_RANK", SqlLibrary.STANDARD, "aggregates#dense_rank", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.LAG, "LAG", SqlLibrary.STANDARD, "aggregates#lag", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.LEAD, "LEAD", SqlLibrary.STANDARD, "aggregates#lead", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.RANK, "RANK", SqlLibrary.STANDARD, "aggregates#rank", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.ROW_NUMBER, "ROW_NUMBER", SqlLibrary.STANDARD, "aggregates#row_number", FunctionDocumentation.NO_FILE, true),
            // constructors from subqueries
            new Func(SqlStdOperatorTable.ARRAY_QUERY, "ARRAY", SqlLibrary.STANDARD, "aggregates#array", FunctionDocumentation.NO_FILE, true),
            new Func(SqlStdOperatorTable.MAP_QUERY, "MAP", SqlLibrary.STANDARD, "aggregates#map", FunctionDocumentation.NO_FILE, true),
            // Constructors
            new Func(SqlStdOperatorTable.ROW, "ROW", SqlLibrary.STANDARD, "types#row_constructor", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, "ARRAY", SqlLibrary.STANDARD, "array#constructor", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, "MAP", SqlLibrary.STANDARD, "map#map-literals", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.IGNORE_NULLS, "IGNORE NULLS", SqlLibrary.STANDARD, "grammar#window-aggregates", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.RESPECT_NULLS, "RESPECT NULLS", SqlLibrary.STANDARD, "grammar#window-aggregates", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.MINUS_DATE, "-", SqlLibrary.STANDARD, "datetime", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.UNNEST, "UNNEST", SqlLibrary.STANDARD, "array#the-unnest-sql-operator,map#the-unnest-operator", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.UNNEST_WITH_ORDINALITY, "UNNEST WITH ORDINALITY", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.LATERAL, "LATERAL", SqlLibrary.STANDARD, "grammar#lateral", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.COLLECTION_TABLE, "TABLE", SqlLibrary.STANDARD, "grammar", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.OVERLAPS, "OVERLAPS", SqlLibrary.STANDARD, "operators#between", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.BETWEEN, "BETWEEN", SqlLibrary.STANDARD, "comparisons#between,operators#between", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.NOT_BETWEEN, "NOT BETWEEN", SqlLibrary.STANDARD, "comparisons#notbetween", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.SYMMETRIC_BETWEEN, "BETWEEN", SqlLibrary.STANDARD, "operators#between", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN, "NOT BETWEEN", SqlLibrary.STANDARD, "operators#between", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.VALUES, "VALUES", SqlLibrary.STANDARD, "grammar#values", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.NOT_LIKE, "NOT LIKE", SqlLibrary.STANDARD, "string#like", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.LIKE, "LIKE", SqlLibrary.STANDARD, "string#like", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ESCAPE, "ESCAPE", SqlLibrary.STANDARD, "string#like", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.CASE, "CASE", SqlLibrary.STANDARD, "comparisons#case", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.OVER, "OVER", SqlLibrary.STANDARD, "grammar#window-aggregates", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.REINTERPRET, "", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, false),

            // Functions
            new Func(SqlStdOperatorTable.SUBSTRING, "SUBSTRING", SqlLibrary.STANDARD, "string#substring", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.REPLACE, "REPLACE", SqlLibrary.STANDARD, "string#replace", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.CONVERT, "CONVERT", SqlLibrary.STANDARD, "casts#casts-and-data-type-conversions", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.TRANSLATE, "TRANSLATE", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.OVERLAY, "OVERLAY", SqlLibrary.STANDARD, "string#overlay,binary#overlay", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.TRIM, "TRIM", SqlLibrary.STANDARD, "string#trim", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.POSITION, "POSITION", SqlLibrary.STANDARD, "string#position", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.CHAR_LENGTH, "CHAR_LENGTH", SqlLibrary.STANDARD, "string#char_length", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.OCTET_LENGTH, "OCTET_LENGTH", SqlLibrary.STANDARD, "binary#octet_length", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.UPPER, "UPPER", SqlLibrary.STANDARD, "string#upper", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.LOWER, "LOWER", SqlLibrary.STANDARD, "string#lower", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.INITCAP, "INITCAP", SqlLibrary.STANDARD, "string#initcap", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ASCII, "ASCII", SqlLibrary.STANDARD, "string#ascii", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.POWER, "POWER", SqlLibrary.STANDARD, "float#power", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.SQRT, "SQRT", SqlLibrary.STANDARD, "float#sqrt", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.MOD, "MOD", SqlLibrary.STANDARD, "integer#mod", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.LN, "LN", SqlLibrary.STANDARD, "float#ln", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.LOG10, "LOG10", SqlLibrary.STANDARD, "float#log10", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ABS, "ABS", SqlLibrary.STANDARD, "decimal#abs,float#abs,integer#abs,datetime#abs", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.ACOS, "ACOS", SqlLibrary.STANDARD, "float#acos", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ASIN, "ASIN", SqlLibrary.STANDARD, "float#asin", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ATAN, "ATAN", SqlLibrary.STANDARD, "float#atan", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ATAN2, "ATAN2", SqlLibrary.STANDARD, "float#atan2", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.CBRT, "CBRT", SqlLibrary.STANDARD, "float#cbrt", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.COS, "COS", SqlLibrary.STANDARD, "float#cos", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.COT, "COT", SqlLibrary.STANDARD, "float#cot", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.DEGREES, "DEGREES", SqlLibrary.STANDARD, "float#degrees", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.EXP, "EXP", SqlLibrary.STANDARD, "float#exp", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.RADIANS, "RADIANS", SqlLibrary.STANDARD, "float#radians", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ROUND, "ROUND", SqlLibrary.STANDARD, "float#round,float#round2,decimal#round,decimal#round2", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.SIGN, "SIGN", SqlLibrary.STANDARD, "decimal#sign", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.SIN, "SIN", SqlLibrary.STANDARD, "float#sin", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.TAN, "TAN", SqlLibrary.STANDARD, "float#tan", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.TRUNCATE, "TRUNCATE", SqlLibrary.STANDARD, "decimal#truncate,decimal#truncate2,float#truncate,float#truncate2", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.PI, "PI", SqlLibrary.STANDARD, "float#pi", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.NULLIF, "NULLIF", SqlLibrary.STANDARD, "comparisons#nullif", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.COALESCE, "COALESCE", SqlLibrary.STANDARD, "comparisons#coalesce", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.FLOOR, "FLOOR", SqlLibrary.STANDARD,
                    "decimal#floor,float#floor,datetime#date_floor,datetime#timestamp_floor,datetime#time_floor", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.CEIL, "CEIL", SqlLibrary.STANDARD,
                    "decimal#ceil,float#ceil,datetime#date_ceil,datetime#timestamp_ceil,datetime#time_ceil", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.TIMESTAMP_ADD, "TIMESTAMPADD", SqlLibrary.STANDARD,
                    "datetime#timestampadd", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.TIMESTAMP_DIFF, "TIMESTAMPDIFF", SqlLibrary.STANDARD,
                    "datetime#date_timestampdiff,datetime#timestamp_timestampdiff", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.CAST, "CAST", SqlLibrary.STANDARD, "casts#casts-and-data-type-conversions", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.EXTRACT, "EXTRACT", SqlLibrary.STANDARD, "datetime#time_extract,datetime#date_extract,datetime#timestamp_extract", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.YEAR, "YEAR", SqlLibrary.STANDARD,
                    "datetime#year", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.QUARTER, "QUARTER", SqlLibrary.STANDARD,
                    "datetime#quarter", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.MONTH, "MONTH", SqlLibrary.STANDARD,
                    "datetime#month", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.WEEK, "WEEK", SqlLibrary.STANDARD,
                    "datetime#week", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.DAYOFYEAR, "DOY", SqlLibrary.STANDARD,
                    "datetime#doy", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.DAYOFMONTH, "DAYOFMONTH", SqlLibrary.STANDARD,
                    "datetime#date_dayofmonth,datetime#timestamp_dayofmonth", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.DAYOFWEEK, "DOW", SqlLibrary.STANDARD,
                    "datetime#date_dayofweek,datetime#timestamp_dayofweek", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.HOUR, "HOUR", SqlLibrary.STANDARD,
                    "datetime#date_hour,datetime#time_hour,datetime#timestamp_hour", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.MINUTE, "MINUTE", SqlLibrary.STANDARD,
                    "datetime#date_minute,datetime#time_minute,datetime#timestamp_minute", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.SECOND, "SECOND", SqlLibrary.STANDARD,
                    "datetime#date_second,datetime#time_second,datetime#timestamp_second", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.ELEMENT, "ELEMENT", SqlLibrary.STANDARD, "array#element", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ITEM, "SAFE_OFFSET", SqlLibrary.STANDARD, "array#safe_offset", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.SLICE, "$SLICE", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.ELEMENT_SLICE, "$ELEMENT_SLICE", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.SCALAR_QUERY, "$SCALAR_QUERY", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.STRUCT_ACCESS, "$STRUCT_ACCESS", SqlLibrary.STANDARD, "", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.CARDINALITY, "CARDINALITY", SqlLibrary.STANDARD, "array#cardinality,map#cardinality", FunctionDocumentation.NO_FILE, false),

            new Func(SqlStdOperatorTable.TUMBLE, "TUMBLE", SqlLibrary.STANDARD, "table#tumble", FunctionDocumentation.NO_FILE, false),
            new Func(SqlStdOperatorTable.HOP, "HOP", SqlLibrary.STANDARD, "table#hop", FunctionDocumentation.NO_FILE, false),

            // SqlLibraryOperators operators
            // DATEADD is not implemented, but give a better error message
            new Func(SqlLibraryOperators.DATEADD, "DATEADD", SqlLibrary.POSTGRESQL, "", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.DATEDIFF, "DATEDIFF", SqlLibrary.POSTGRESQL,
                    "datetime#date_timestampdiff,datetime#timestamp_timestampdiff", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.MSSQL_CONVERT, "CONVERT", SqlLibrary.POSTGRESQL, "casts", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.DATE_ADD, "DATE_ADD", SqlLibrary.BIG_QUERY, "datetime#date_add", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.DATE_SUB, "DATE_SUB", SqlLibrary.BIG_QUERY, "datetime#date_sub", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.DATE_PART, "DATE_PART", SqlLibrary.POSTGRESQL,
                    "datetime#date_extract,datetime#time_extract,datetime#timestamp_extract", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.IF, "IF", SqlLibrary.BIG_QUERY, "comparisons#if", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.LEN, "LEN", SqlLibrary.SPARK, "string#char_length", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.LENGTH, "LENGTH", SqlLibrary.POSTGRESQL, "string#char_length", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.SUBSTR_BIG_QUERY, "SUBSTR", SqlLibrary.BIG_QUERY, "string#substr", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.SPLIT, "SPLIT", SqlLibrary.BIG_QUERY, "string#split", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.SPLIT_PART, "SPLIT_PART", SqlLibrary.POSTGRESQL, "string#split_part", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.GREATEST, "GREATEST", SqlLibrary.BIG_QUERY, "comparisons#greatest", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.LEAST, "LEAST", SqlLibrary.BIG_QUERY, "comparisons#least", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.SAFE_CAST, "SAFE_CAST", SqlLibrary.BIG_QUERY, "casts#safe-casts", FunctionDocumentation.NO_FILE, false),

            new Func(SqlLibraryOperators.REGEXP_REPLACE_2, "REGEXP_REPLACE", SqlLibrary.REDSHIFT, "string#regexp_replace", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.REGEXP_REPLACE_3, "REGEXP_REPLACE", SqlLibrary.REDSHIFT, "string#regexp_replace", FunctionDocumentation.NO_FILE, false),

            // More aggregates
            new Func(SqlLibraryOperators.BOOL_AND, "BOOL_AND", SqlLibrary.POSTGRESQL, "aggregates#logical_and", FunctionDocumentation.NO_FILE, true),
            new Func(SqlLibraryOperators.BOOL_OR, "BOOL_OR", SqlLibrary.POSTGRESQL, "aggregates#logical_or", FunctionDocumentation.NO_FILE, true),
            new Func(SqlLibraryOperators.LOGICAL_OR, "LOGICAL_OR", SqlLibrary.BIG_QUERY, "aggregates#logical_or", FunctionDocumentation.NO_FILE, true),
            new Func(SqlLibraryOperators.LOGICAL_AND, "LOGICAL_AND", SqlLibrary.BIG_QUERY, "aggregates#logical_and", FunctionDocumentation.NO_FILE, true),
            new Func(SqlLibraryOperators.COUNTIF, "COUNTIF", SqlLibrary.BIG_QUERY, "aggregates#countif", FunctionDocumentation.NO_FILE, true),
            new Func(SqlLibraryOperators.ARRAY_AGG, "ARRAY_AGG", SqlLibrary.BIG_QUERY, "aggregates#array_agg", FunctionDocumentation.NO_FILE, true),

            new Func(SqlLibraryOperators.DATE, "DATE", SqlLibrary.BIG_QUERY, "datetime#date-literals", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.TIME, "TIME", SqlLibrary.BIG_QUERY, "datetime#time-literals", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.TIMESTAMP, "TIMESTAMP", SqlLibrary.BIG_QUERY, "datetime#timestamp-literals", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.LEFT, "LEFT", SqlLibrary.POSTGRESQL, "string#left,binary#left", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.REPEAT, "REPEAT", SqlLibrary.POSTGRESQL, "string#repeat", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.RIGHT, "RIGHT", SqlLibrary.POSTGRESQL, "string#right,binary#right", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ILIKE, "ILIKE", SqlLibrary.POSTGRESQL, "string#ilike", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.NOT_ILIKE, "NOT ILIKE", SqlLibrary.POSTGRESQL, "string#ilike", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.RLIKE, "RLIKE", SqlLibrary.MYSQL, "string#rlike", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.NOT_RLIKE, "NOT RLIKE", SqlLibrary.MYSQL, "string#rlike", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.CONCAT_FUNCTION, "CONCAT", SqlLibrary.MYSQL, "string#concat", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.CONCAT_WS, "CONCAT_WS", SqlLibrary.MYSQL, "string#concat_ws", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY, "ARRAY", SqlLibrary.SPARK, "array#array", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.MAP, "MAP", SqlLibrary.SPARK, "map", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_APPEND, "ARRAY_APPEND", SqlLibrary.SPARK, "array#append", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_COMPACT, "ARRAY_COMPACT", SqlLibrary.SPARK, "array#compact", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_CONCAT, "ARRAY_CONCAT", SqlLibrary.SPARK, "array#concat", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_DISTINCT, "ARRAY_DISTINCT", SqlLibrary.SPARK, "array#distinct", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_JOIN, "ARRAY_JOIN", SqlLibrary.SPARK, "array#join", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_LENGTH, "ARRAY_LENGTH", SqlLibrary.SPARK, "array#length", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_MAX, "ARRAY_MAX", SqlLibrary.SPARK, "array#max", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_MIN, "ARRAY_MIN", SqlLibrary.SPARK, "array#min", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_PREPEND, "ARRAY_PREPEND", SqlLibrary.SPARK, "array#prepend", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_REPEAT, "ARRAY_REPEAT", SqlLibrary.SPARK, "array#repeat", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_REVERSE, "ARRAY_REVERSE", SqlLibrary.SPARK, "array#reverse", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_SIZE, "ARRAY_SIZE", SqlLibrary.SPARK, "array#size", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ARRAY_TO_STRING, "ARRAY_TO_STRING", SqlLibrary.SPARK, "array#to_string", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.SORT_ARRAY, "SORT_ARRAY", SqlLibrary.SPARK, "array#sort", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.TO_HEX, "TO_HEX", SqlLibrary.BIG_QUERY, "binary#to_hex", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.DATE_TRUNC, "DATE_TRUNC", SqlLibrary.BIG_QUERY, "datetime#date_trunc", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.TIME_TRUNC, "TIME_TRUNC", SqlLibrary.BIG_QUERY, "datetime#time_trunc", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.TIMESTAMP_TRUNC, "TIMESTAMP_TRUNC", SqlLibrary.BIG_QUERY, "datetime#timestamp_trunc", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.CHR, "CHR", SqlLibrary.POSTGRESQL, "string#chr", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.TANH, "TANH", SqlLibrary.ALL, "float#tanh", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.COTH, "COTH", SqlLibrary.ALL, "float#coth", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.COSH, "COSH", SqlLibrary.ALL, "float#cosh", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ACOSH, "ACOSH", SqlLibrary.ALL, "float#acosh", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ASINH, "ASINH", SqlLibrary.ALL, "float#asinh", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.ATANH, "ATANH", SqlLibrary.ALL, "float#atanh", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.SECH, "SECH", SqlLibrary.ALL, "float#sech", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.CSCH, "CSCH", SqlLibrary.ALL, "float#csch", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.SINH, "SINH", SqlLibrary.ALL, "float#sinh", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.CSC, "CSC", SqlLibrary.ALL, "float#csc", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.SEC, "SEC", SqlLibrary.ALL, "float#sec", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.IS_INF, "IS_INF", SqlLibrary.BIG_QUERY, "float#is_inf", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.IS_NAN, "IS_NAN", SqlLibrary.BIG_QUERY, "float#is_nan", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.LOG, "LOG", SqlLibrary.BIG_QUERY, "float#log", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.INFIX_CAST, "::", SqlLibrary.POSTGRESQL, "casts#coloncolon", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.NULL_SAFE_EQUAL, "<=>", SqlLibrary.MYSQL, "operators#comparisons", FunctionDocumentation.NO_FILE, false),
            // new Func(SqlLibraryOperators.OFFSET, "OFFSET", SqlLibrary.BIG_QUERY, "", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.SAFE_OFFSET, "SAFE_OFFSET", SqlLibrary.BIG_QUERY, "", FunctionDocumentation.NO_FILE, false),
            // new Func(SqlLibraryOperators.ORDINAL, "ORDINAL", SqlLibrary.BIG_QUERY, "array", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.TRUNC_BIG_QUERY, "TRUNC", SqlLibrary.BIG_QUERY,
                    "decimal#trunc,float#trunc", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.MAP_CONTAINS_KEY, "MAP_CONTAINS_KEY", SqlLibrary.SPARK,
                    "map#map_contains_key", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.MD5, "MD5", SqlLibrary.SPARK,
                    "string#md5,binary#md5", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.IFNULL, "IFNULL", SqlLibrary.BIG_QUERY,
                    "comparisons#ifnull", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.MAP_KEYS, "MAP_KEYS", SqlLibrary.SPARK,
                    "map#map_keys", FunctionDocumentation.NO_FILE, false),
            new Func(SqlLibraryOperators.MAP_VALUES, "MAP_VALUES", SqlLibrary.SPARK,
                    "map#map_values", FunctionDocumentation.NO_FILE, false)
            // new Func(SqlLibraryOperators.SAFE_ORDINAL, "SAFE_ORDINAL", SqlLibrary.BIG_QUERY, "array", FunctionDocumentation.NO_FILE, false),
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
