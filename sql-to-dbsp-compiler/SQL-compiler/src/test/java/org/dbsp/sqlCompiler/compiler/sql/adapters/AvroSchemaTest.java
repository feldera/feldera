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
                     "type" : "DOUBLE",
                     "nullable" : true
                   }
                 }, {
                   "name" : "bar",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "DOUBLE",
                     "nullable" : true
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
                     "type" : "VARCHAR",
                     "nullable" : false,
                     "precision" : -1
                   }
                 }, {
                   "name" : "Age",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "INTEGER",
                     "nullable" : false
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
                     "type" : "VARCHAR",
                     "nullable" : false,
                     "precision" : -1
                   }
                 }, {
                   "name" : "last_name",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "VARCHAR",
                     "nullable" : false,
                     "precision" : -1
                   }
                 }, {
                   "name" : "age",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "INTEGER",
                     "nullable" : false
                   }
                 }, {
                   "name" : "automated_email",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "BOOLEAN",
                     "nullable" : false
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
                     "type" : "VARCHAR",
                     "nullable" : false,
                     "precision" : -1
                   }
                 }, {
                   "name" : "myBoolean",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "BOOLEAN",
                     "nullable" : false
                   }
                 }, {
                   "name" : "myInt",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "INTEGER",
                     "nullable" : false
                   }
                 }, {
                   "name" : "myLong",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "BIGINT",
                     "nullable" : false
                   }
                 }, {
                   "name" : "myFloat",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "REAL",
                     "nullable" : false
                   }
                 }, {
                   "name" : "myDouble",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "DOUBLE",
                     "nullable" : false
                   }
                 }, {
                   "name" : "myBytes",
                   "case_sensitive" : false,
                   "columntype" : {
                     "type" : "BINARY",
                     "nullable" : false,
                     "precision" : 1
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
