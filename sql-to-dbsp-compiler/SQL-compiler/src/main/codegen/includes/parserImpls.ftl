<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

// This file is adapted from Calcite:
// https://github.com/apache/calcite/tree/main/babel/src/main/codegen/includes/parserImpls.ftl

JoinType LeftSemiJoin() :
{
}
{
    <LEFT> <SEMI> <JOIN> { return JoinType.LEFT_SEMI_JOIN; }
}

SqlNode DateaddFunctionCall() :
{
    final Span s;
    final SqlOperator op;
    final SqlIntervalQualifier unit;
    final List<SqlNode> args;
    SqlNode e;
}
{
    (   <DATE_PART>  { op = SqlLibraryOperators.DATE_PART; }
    |   <DATEADD> { op = SqlLibraryOperators.DATEADD; }
    |   <DATEDIFF> { op = SqlStdOperatorTable.TIMESTAMP_DIFF; }
    |   <DATEPART>  { op = SqlLibraryOperators.DATEPART; }
    )
    { s = span(); }
    <LPAREN> unit = TimeUnitOrName() {
        args = startList(unit);
    }
    (
        <COMMA> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args.add(e);
        }
    )*
    <RPAREN> {
        return op.createCall(s.end(this), args);
    }
}

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

/* Extra operators */

<DEFAULT, DQID, BTID> TOKEN :
{
    < DATE_PART: "DATE_PART" >
|   < DATEADD: "DATEADD" >
|   < DATEDIFF: "DATEDIFF" >
|   < DATEPART: "DATEPART" >
|   < NEGATE: "!" >
|   < TILDE: "~" >
}

/** Parses the infix "::" cast operator used in PostgreSQL. */
void InfixCast(List<Object> list, ExprContext exprContext, Span s) :
{
    final SqlDataTypeSpec dt;
}
{
    <INFIX_CAST> {
        checkNonQueryExpression(exprContext);
    }
    dt = DataType() {
        list.add(
            new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.INFIX_CAST,
                s.pos()));
        list.add(dt);
    }
}

/** Parses the NULL-safe "<=>" equal operator used in MySQL. */
void NullSafeEqual(List<Object> list, ExprContext exprContext, Span s) :
{
}
{
    <NULL_SAFE_EQUAL> {
        checkNonQueryExpression(exprContext);
        list.add(new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.NULL_SAFE_EQUAL, getPos()));
    }
    AddExpression2b(list, ExprContext.ACCEPT_SUB_QUERY)
}
