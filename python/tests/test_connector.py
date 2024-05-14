import unittest
from tests import TEST_CLIENT
from feldera.rest.connector import Connector


class TestConnector(unittest.TestCase):
    def test_create_connector(self, delete=True):
        config = {
            "transport": {
                "name": "file_input",
                "config": {
                    "path": "blah.txt"
                }
            },
            "format": {
                "name": "csv",
                "config": {}
            }
        }
        connector = Connector(name='test_connector', description='test_description', config=config)
        TEST_CLIENT.create_connector(connector)

        assert connector.id is not None

        if delete:
            TEST_CLIENT.delete_connector('test_connector')

    def test_get_connector(self):
        self.test_create_connector(False)

        connector = TEST_CLIENT.get_connector('test_connector')
        assert connector is not None

        TEST_CLIENT.delete_connector('test_connector')

    def test_list_connectors(self):
        self.test_create_connector(False)

        connectors = TEST_CLIENT.connectors()
        assert connectors is not None
        assert len(connectors) > 0

        TEST_CLIENT.delete_connector('test_connector')

    def test_delete_connector(self):
        self.test_create_connector(False)

        TEST_CLIENT.delete_connector('test_connector')


if __name__ == '__main__':
    unittest.main()
