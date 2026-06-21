package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChangeStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.util.Linq;
import org.junit.Test;

import java.nio.charset.Charset;

public class MapTests extends SqlIoTest {
    public DBSPCompiler compileQuery(String statements, String query) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.optimizationLevel = 0;
        compiler.options.ioOptions.quiet = false;
        compiler.submitStatementsForCompilation(statements);
        compiler.submitStatementForCompilation(query);
        return compiler;
    }

    void testQuery(String query, InputOutputChangeStream streams) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery("", query);
        this.getCCS(compiler, streams);
    }

    private void testQuery(String query, DBSPZSetExpression expression) {
        this.testQuery(query,
                new InputOutputChangeStream().addChange(
                        new InputOutputChange(new Change(), new Change("V", expression))));
    }

    @Test
    public void testDuplicateKeys() {
        var compiler = this.compileQuery("", "CREATE VIEW V AS SELECT MAP['hi', 1, 'hi', 2]");
        TestUtil.assertMessagesContain(compiler, "warning: Duplicate MAP key");
    }

    @Test
    public void mapLiteralTest() {
        this.qst("""
                 SELECT MAP['hi',2];
                  r
                 ---
                  { hi: 2 }
                 (1 row)""");
    }

    @Test
    public void mapIndexTest() {
        this.qst(""" 
                 SELECT MAP['hi',2]['hi'], MAP['hi',2]['x'];
                  e0 | e1
                 ---------
                  2  |
                 (1 row)""");
    }

    @Test
    public void mapBlackboxTest() {
        String query = "SELECT blackbox(MAP['hi',2])";
        DBSPType str = DBSPTypeString.varchar(false);
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPMapExpression(
                        new DBSPTypeMap(
                                str,
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false),
                                false),
                        Linq.list(new DBSPStringLiteral(CalciteObject.EMPTY, str, "hi", Charset.defaultCharset()),
                                new DBSPI32Literal(2))))));
    }

    @Test
    public void mapCardinalityTest() {
        this.qst("""
                SELECT CARDINALITY(MAP['hi',2]);
                 r
                ---
                 1
                (1 row)""");
    }

    @Test
    public void testMapSubquery() {
        var ccs = this.getCCS("""
               CREATE TABLE T(v varchar, x int);
               CREATE VIEW V AS SELECT MAP(SELECT * FROM T);""");
        ccs.stepWeightOne("INSERT INTO T VALUES('hello', 10), ('there', 5)", """
          map
        -------
         { hello: 10, there: 5 }""");
    }

    @Test
    public void testUnnestMap() {
        this.qst("""
                 select * from UNNEST(map['a', 12]);
                  k | v
                 -------
                  a | 12
                 (1 row)
                 
                 select * from UNNEST(map['a', 12, 'b', 15, 'c', NULL]);
                  k | v
                 -------
                  a | 12
                  b | 15
                  c |
                 (3 rows)
                 
                 WITH T(i, m) as (VALUES(1, MAP[1, 2, 3, 4]), (2, MAP[5, NULL]))
                 SELECT T.i, k, v FROM T CROSS JOIN UNNEST(T.m) AS pair(k, v);
                  i | k | v
                 -----------
                  1 | 1 | 2
                  1 | 3 | 4
                  2 | 5 |
                 (3 rows)""");
    }

    @Test
    public void nullMapKey() {
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT MAP[NULL, NULL]",
                "MAP key type cannot be NULL");
    }

    @Test
    public void testMapKeys() {
        this.qst("""
                SELECT map_keys(map['foo', 1, 'bar', 2]);
                 keys
                --------------
                 { bar, foo }
                (1 row)""");
    }

    @Test
    public void mapKeyVariant() {
        var ccs = this.getCCS("""
                create table j(j VARCHAR);
                
                create LOCAL view user_props AS
                SELECT PARSE_JSON(j) AS contacts FROM j;
                
                create view abc as
                WITH ref_profile AS (
                SELECT cast(contacts as MAP<varchar, variant>) contacts
                    FROM user_props
                ) SELECT key
                FROM ref_profile profile_0, UNNEST(MAP_KEYS(profile_0.contacts)) AS t(key)""");
        ccs.stepWeightOne("""
                INSERT INTO j VALUES('{ "a": "1", "b": 2, "c": [1, 2, 3], "d": null, "e": { "f": 1 } }');""", """
                 key
                -----
                 a
                 b
                 c
                 d
                 e""");
    }

    @Test
    public void testMapValues() {
        this.qst("""
                 SELECT map_values(map['foo', 1, 'bar', 2]);
                  r
                 --------
                  { 2, 1 }
                 (1 row)""");
    }

    @Test
    public void mapValuesVariant() {
        var ccs = this.getCCS("""
                create table j(j VARCHAR);
                
                create LOCAL view user_props AS
                SELECT PARSE_JSON(j) AS contacts FROM j;
                
                create view abc as
                WITH ref_profile AS (
                SELECT cast(contacts as MAP<varchar, variant>) contacts
                    FROM user_props
                ) SELECT TO_JSON(value)
                FROM ref_profile profile_0, UNNEST(MAP_VALUES(profile_0.contacts)) AS t(value)""");
        ccs.stepWeightOne("""
                INSERT INTO j VALUES('{ "a": "1", "b": 2, "c": [1, 2, 3], "d": null, "e": { "f": 1 } }');""", """
                 key
                -----
                 "1"
                 2
                 [1,2,3]
                 null
                 {"f":1}""");
    }

    @Test
    public void testMapConcat() {
        this.qst("""
                SELECT MAP_CONCAT(
                  MAP[ 1, 'a' ],
                  MAP[ 2, 'b' ]
                );
                +--------------+
                | EXPR$0       |
                +--------------+
                | {1: a, 2: b} |
                +--------------+
                (1 row)
                
                SELECT MAP_CONCAT(CAST(NULL AS MAP<INT, INT>), MAP[1, 2]);
                 r
                ----
                NULL
                (1 row)
                
                SELECT MAP_CONCAT(MAP[1, 'a'], MAP[1, 'b']);
                 r
                ----
                 {1: b}
                (1 row)
                
                SELECT MAP_CONCAT(MAP[1, 2], MAP[1.0, NULL]);
                 r
                ---
                 {1.0: NULL}
                (1 row)
                
                SELECT MAP_CONCAT(MAP[1, 2]);
                 r
                ---
                 {1: 2}
                (1 row)
                
                SELECT MAP_CONCAT(MAP[1, 2], MAP[1, 3], MAP[1, 4]);
                 r
                ---
                 {1: 4}
                (1 row)""");
    }
}
