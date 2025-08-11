"""Data ingestion and querying tests converted from Rust integration tests."""
import unittest
import json
import time
from tests.shared_test_pipeline import SharedTestPipeline
from tests import TEST_CLIENT, enterprise_only


class TestDataIngestionAndQuery(SharedTestPipeline):
    """Test data ingestion, querying, and various data formats."""

    def test_basic_ingestion_table(self):
        """
        CREATE TABLE t1(c1 integer, c2 bool, c3 varchar) WITH ('materialized' = 'true');
        CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;
        CREATE TABLE map_table(c1 integer, c2 bool, c3 MAP<varchar, varchar>) WITH ('materialized' = 'true');
        CREATE TABLE datetime_table(t TIME, ts TIMESTAMP, d DATE) WITH ('materialized' = 'true');
        CREATE TABLE quoted_table("c1" integer not null, "C2" bool not null, "😁❤" varchar not null, "αβγ" boolean not null, ΔΘ boolean not null) WITH ('materialized' = 'true');
        CREATE TABLE pk_table(id bigint not null, s varchar not null, primary key (id)) WITH ('materialized' = 'true');
        CREATE TABLE "TaBle1"(id bigint not null);
        CREATE TABLE table1(id bigint);
        CREATE MATERIALIZED VIEW "V1" AS SELECT * FROM "TaBle1";
        CREATE MATERIALIZED VIEW "v1" AS SELECT * FROM table1;
        """
        pass

    def test_json_ingress(self):
        """Test JSON data ingestion with various formats."""
        self.pipeline.start()
        
        # Push JSON data with default format
        data = '''{"c1": 10, "c2": true}
{"c1": 20, "c3": "foo"}'''
        
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/T1?format=json&update_format=raw",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        # Wait a moment for data to be processed
        time.sleep(0.5)
        
        # Query the data
        query = "select * from t1 order by c1, c2, c3;"
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        self.assertEqual(response.status_code, 200)
        result = response.json()
        expected = [
            {"c1": 10, "c2": True, "c3": None},
            {"c1": 20, "c2": None, "c3": "foo"}
        ]
        self.assertEqual(result, expected)
        
        # Push data using insert/delete format
        data = '''{"delete": {"c1": 10, "c2": true}}
{"insert": {"c1": 30, "c3": "bar"}}'''
        
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/t1?format=json&update_format=insert_delete",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        # Query again
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [
            {"c1": 20, "c2": None, "c3": "foo"},
            {"c1": 30, "c2": None, "c3": "bar"}
        ]
        self.assertEqual(result, expected)

    def test_json_array_format(self):
        """Test JSON array format ingestion."""
        self.pipeline.start()
        
        # Format data as json array
        data = '{"insert": [40, true, "buzz"]}'
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/T1?format=json&update_format=insert_delete",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        # Use array of updates instead of newline-delimited JSON
        data = '[{"delete": [40, true, "buzz"]}, {"insert": [50, true, ""]}]'
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/t1?format=json&update_format=insert_delete&array=true",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        query = "select * from T1 order by c1, c2, c3;"
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [{"c1": 50, "c2": True, "c3": ""}]
        self.assertEqual(result, expected)

    def test_json_parse_errors(self):
        """Test JSON parsing error handling."""
        self.pipeline.start()
        
        # Trigger parse errors with array format
        data = '[{"insert": [35, true, ""]}, {"delete": [40, "foo", "buzz"]}, {"insert": [true, true, ""]}]'
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/T1?format=json&update_format=insert_delete&array=true",
            data=data
        )
        self.assertEqual(response.status_code, 400)
        
        error_data = response.json()
        self.assertEqual(error_data["error_code"], "ParseErrors")
        self.assertEqual(error_data["details"]["num_errors"], 2)

    def test_csv_ingress(self):
        """Test CSV data ingestion."""
        self.pipeline.start()
        
        # Push CSV data (one record is invalid)
        data = '''15,true,foo
not_a_number,true,ΑαΒβΓγΔδ
16,false,unicode🚲'''
        
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/t1?format=csv",
            data=data
        )
        self.assertEqual(response.status_code, 400)  # Should have parse errors
        
        error_data = response.json()
        self.assertEqual(error_data["error_code"], "ParseErrors")
        
        time.sleep(0.5)
        
        # Valid records should still be ingested
        query = "select * from t1 order by c1;"
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        
        # Should contain the valid records
        found_15 = any(r["c1"] == 15 and r["c2"] == True and r["c3"] == "foo" for r in result)
        found_16 = any(r["c1"] == 16 and r["c2"] == False and r["c3"] == "unicode🚲" for r in result)
        self.assertTrue(found_15)
        self.assertTrue(found_16)

    def test_debezium_format(self):
        """Test Debezium CDC format ingestion."""
        self.pipeline.start()
        
        # First insert some data
        data = '{"c1": 50, "c2": true, "c3": ""}'
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/T1?format=json&update_format=raw",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        # Now use Debezium format to update
        data = '{"payload": {"op": "u", "before": [50, true, ""], "after": [60, true, "hello"]}}'
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/T1?format=json&update_format=debezium",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        # Check the result
        query = "select * from t1 where c1 = 60;"
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [{"c1": 60, "c2": True, "c3": "hello"}]
        self.assertEqual(result, expected)

    def test_map_column(self):
        """Test table with MAP column type."""
        self.pipeline.start()
        
        # Push data with MAP column
        data = '''{"c1": 10, "c2": true, "c3": {"foo": "1", "bar": "2"}}
{"c1": 20}'''
        
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/map_table?format=json&update_format=raw",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        query = "select * from map_table order by c1;"
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [
            {"c1": 10, "c2": True, "c3": {"bar": "2", "foo": "1"}},
            {"c1": 20, "c2": None, "c3": None}
        ]
        self.assertEqual(result, expected)

    def test_parse_datetime(self):
        """Test parsing of date/time values with whitespace."""
        self.pipeline.start()
        
        # Test datetime parsing with leading/trailing whitespace
        data = '''{"t":"13:22:00","ts": "2021-05-20 12:12:33","d": "2021-05-20"}
{"t":" 11:12:33.483221092 ","ts": " 2024-02-25 12:12:33 ","d": " 2024-02-25 "}'''
        
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/datetime_table?format=json&update_format=raw",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        query = "select * from datetime_table order by t, ts, d;"
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [
            {"d": "2024-02-25", "t": "11:12:33.483221092", "ts": "2024-02-25T12:12:33"},
            {"d": "2021-05-20", "t": "13:22:00", "ts": "2021-05-20T12:12:33"}
        ]
        self.assertEqual(result, expected)

    def test_quoted_columns(self):
        """Test tables with quoted column names."""
        self.pipeline.start()
        
        # Push data to table with quoted columns
        data = '{"c1": 10, "C2": true, "😁❤": "foo", "αβγ": true, "δθ": false}'
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/quoted_table?format=json&update_format=raw",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        query = 'select * from quoted_table order by "c1";'
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [{"C2": True, "c1": 10, "αβγ": True, "δθ": False, "😁❤": "foo"}]
        self.assertEqual(result, expected)

    def test_primary_keys(self):
        """Test table with primary keys."""
        self.pipeline.start()
        
        # Insert initial data
        data = '''{"insert":{"id":1, "s": "1"}}
{"insert":{"id":2, "s": "2"}}'''
        
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/pk_table?format=json&update_format=insert_delete",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        # Check initial data
        query = "select * from pk_table order by id;"
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [{"id": 1, "s": "1"}, {"id": 2, "s": "2"}]
        self.assertEqual(result, expected)
        
        # Make changes (insert overwrites, update modifies)
        data = '''{"insert":{"id":1, "s": "1-modified"}}
{"update":{"id":2, "s": "2-modified"}}'''
        
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/pk_table?format=json&update_format=insert_delete",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        # Check updated data
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [{"id": 1, "s": "1-modified"}, {"id": 2, "s": "2-modified"}]
        self.assertEqual(result, expected)
        
        # Delete a record
        data = '{"delete":{"id":2}}'
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/pk_table?format=json&update_format=insert_delete",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        # Check deletion
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [{"id": 1, "s": "1-modified"}]
        self.assertEqual(result, expected)

    def test_case_sensitive_tables(self):
        """Test case-sensitive table ingress/egress behavior."""
        self.pipeline.start()
        
        # Push data to case-sensitive table
        data = '{"insert":{"id":1}}'
        response = TEST_CLIENT.post(
            f'/v0/pipelines/{self.pipeline.name}/ingress/"TaBle1"?format=json&update_format=insert_delete',
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        # Push data to regular table
        data = '{"insert":{"id":2}}'
        response = TEST_CLIENT.post(
            f"/v0/pipelines/{self.pipeline.name}/ingress/table1?format=json&update_format=insert_delete",
            data=data
        )
        self.assertTrue(response.status_code < 300)
        
        time.sleep(0.5)
        
        # Query case-sensitive view
        query = 'select * from "V1";'
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [{"id": 1}]
        self.assertEqual(result, expected)
        
        # Query lowercase view
        query = 'select * from "v1";'
        response = TEST_CLIENT.get(f"/v0/pipelines/{self.pipeline.name}/query?sql={query}")
        result = response.json()
        expected = [{"id": 2}]
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()