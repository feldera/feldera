import unittest
import threading
from feldera import PipelineBuilder
from tests import TEST_CLIENT


class MassDeleteTest(unittest.TestCase):
    def test_mass_delete(self):
        pipelines = []
        for i in range(500):
            name = f"mass_pipeline_{i}"
            p = PipelineBuilder(TEST_CLIENT, name, "").create_or_replace()
            pipelines.append(p)

        threads = []
        for p in pipelines:
            t = threading.Thread(target=p.delete)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
