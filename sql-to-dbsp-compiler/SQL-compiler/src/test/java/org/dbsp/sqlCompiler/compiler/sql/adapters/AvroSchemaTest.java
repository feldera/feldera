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
                 "case_sensitive" : false,
                 "fields" : [ {
                   "name" : "foo",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : true,
                     "type" : "DOUBLE"
                   }
                 }, {
                   "name" : "bar",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : true,
                     "type" : "DOUBLE"
                   }
                 } ]
               }""",
               """
               {
                 "name" : "Employee",
                 "case_sensitive" : false,
                 "fields" : [ {
                   "name" : "Name",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : -1,
                     "type" : "VARCHAR"
                   }
                 }, {
                   "name" : "Age",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "INTEGER"
                   }
                 } ]
               }""",
               """
               {
                 "name" : "User",
                 "case_sensitive" : false,
                 "fields" : [ {
                   "name" : "first_name",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : -1,
                     "type" : "VARCHAR"
                   }
                 }, {
                   "name" : "last_name",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : -1,
                     "type" : "VARCHAR"
                   }
                 }, {
                   "name" : "age",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "INTEGER"
                   }
                 }, {
                   "name" : "automated_email",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "BOOLEAN"
                   }
                 } ]
               }""",
               """
               {
                 "name" : "Type2",
                 "case_sensitive" : false,
                 "fields" : [ {
                   "name" : "myString",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : -1,
                     "type" : "VARCHAR"
                   }
                 }, {
                   "name" : "myBoolean",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "BOOLEAN"
                   }
                 }, {
                   "name" : "myInt",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "INTEGER"
                   }
                 }, {
                   "name" : "myLong",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "BIGINT"
                   }
                 }, {
                   "name" : "myFloat",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "REAL"
                   }
                 }, {
                   "name" : "myDouble",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "type" : "DOUBLE"
                   }
                 }, {
                   "name" : "myBytes",
                   "case_sensitive" : false,
                   "columntype" : {
                     "nullable" : false,
                     "precision" : 1,
                     "type" : "BINARY"
                   }
                 } ]
               }"""
        };

        Schema.Parser schemaParser = new Schema.Parser();
        for (int i = 0; i < schemas.length; i++) {
            String avro = schemas[i];
            String json = expected[i];
            Schema schema = schemaParser.parse(avro);
            AvroSchemaWrapper wrapper = new AvroSchemaWrapper(new JavaTypeFactoryImpl(), schema);
            Assert.assertEquals(json, wrapper.asJson().toPrettyString());
        }
    }
}
