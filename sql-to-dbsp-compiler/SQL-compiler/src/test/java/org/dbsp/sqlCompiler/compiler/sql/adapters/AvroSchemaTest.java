package org.dbsp.sqlCompiler.compiler.sql.adapters;

import org.apache.avro.Schema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.AvroSchemaWrapper;
import org.junit.Assert;
import org.junit.Test;

public class AvroSchemaTest {
    @Test
    public void avroSchemaTest() {
        String[] schemas = new String[] {
                """
                {
                  "type":"record",
                  "name":"FooBar",
                  "namespace":"com.foo.bar",
                  "fields":[{
                     "name":"foo",
                     "type":["null","double"],
                     "default":null
                  }, {
                     "name":"bar",
                     "type":["null","double"],
                     "default":null
                  }]
                }
                """,
                """
                {
                   "type" : "record",
                   "namespace" : "Tutorialspoint",
                   "name" : "Employee",
                   "fields" : [
                      { "name" : "Name" , "type" : "string" },
                      { "name" : "Age" , "type" : "int" }
                   ]
                }""",
                """
                {
                	"type": "record",
                	"namespace": "com.example",
                	"name": "User",
                	"fields": [{
                			"name": "first_name",
                			"type": "string",
                			"doc": "First Name of the User"
                		},
                		{
                			"name": "last_name",
                			"type": "string",
                			"doc": "Last Name of the User"
                		},
                		{
                			"name": "age",
                			"type": "int",
                			"doc": "Age of the User"
                		},
                		{
                			"name": "automated_email",
                			"type": "boolean",
                			"default": true,
                			"doc": "Indicaton if the user is subscribe to the newsletter"
                		}
                	]
                }""",
                """
                {
                    "type": "record",
                    "namespace": "co.uk.dalelane",
                    "name": "Type2",
                    "fields": [
                        {
                            "name": "myString",
                            "type": "string"
                        },
                        {
                            "name": "myBoolean",
                            "type": "boolean"
                        },
                        {
                            "name": "myInt",
                            "type": "int"
                        },
                        {
                            "name": "myLong",
                            "type": "long"
                        },
                        {
                            "name": "myFloat",
                            "type": "float"
                        },
                        {
                            "name": "myDouble",
                            "type": "double"
                        },
                        {
                            "name": "myBytes",
                            "type": "bytes"
                        }
                    ]
                }"""
        };

        String[] expected = new String[] {
                """
               {
                 "name" : "FooBar",
                 "case_sensitive" : true,
                 "fields" : [ {
                   "name" : "foo",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : true,
                     "type" : "DOUBLE"
                   },
                   "unused" : false
                 }, {
                   "name" : "bar",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : true,
                     "type" : "DOUBLE"
                   },
                   "unused" : false
                 } ]
               }""",
               """
               {
                 "name" : "Employee",
                 "case_sensitive" : true,
                 "fields" : [ {
                   "name" : "Name",
                   "case_sensitive" : true,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : -1,
                     "type" : "VARCHAR"
                   },
                   "unused" : false
                 }, {
                   "name" : "Age",
                   "case_sensitive" : true,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "INTEGER"
                   },
                   "unused" : false
                 } ]
               }""",
               """
               {
                 "name" : "User",
                 "case_sensitive" : true,
                 "fields" : [ {
                   "name" : "first_name",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : -1,
                     "type" : "VARCHAR"
                   },
                   "unused" : false
                 }, {
                   "name" : "last_name",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : -1,
                     "type" : "VARCHAR"
                   },
                   "unused" : false
                 }, {
                   "name" : "age",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "INTEGER"
                   },
                   "unused" : false
                 }, {
                   "name" : "automated_email",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "BOOLEAN"
                   },
                   "unused" : false
                 } ]
               }""",
               """
               {
                 "name" : "Type2",
                 "case_sensitive" : true,
                 "fields" : [ {
                   "name" : "myString",
                   "case_sensitive" : true,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : -1,
                     "type" : "VARCHAR"
                   },
                   "unused" : false
                 }, {
                   "name" : "myBoolean",
                   "case_sensitive" : true,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "BOOLEAN"
                   },
                   "unused" : false
                 }, {
                   "name" : "myInt",
                   "case_sensitive" : true,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "INTEGER"
                   },
                   "unused" : false
                 }, {
                   "name" : "myLong",
                   "case_sensitive" : true,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "BIGINT"
                   },
                   "unused" : false
                 }, {
                   "name" : "myFloat",
                   "case_sensitive" : true,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "REAL"
                   },
                   "unused" : false
                 }, {
                   "name" : "myDouble",
                   "case_sensitive" : true,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "DOUBLE"
                   },
                   "unused" : false
                 }, {
                   "name" : "myBytes",
                   "case_sensitive" : true,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : 1,
                     "type" : "BINARY"
                   },
                   "unused" : false
                 } ]
               }"""
        };

        Schema.Parser schemaParser = new Schema.Parser();
        for (int i = 0; i < schemas.length; i++) {
            String avro = schemas[i];
            String json = expected[i];
            Schema schema = schemaParser.parse(avro);
            AvroSchemaWrapper wrapper = new AvroSchemaWrapper(new JavaTypeFactoryImpl(), schema);
            Assert.assertEquals(json, wrapper.asJson(true).toPrettyString());
        }
    }
}
