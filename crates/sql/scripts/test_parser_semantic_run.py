import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from parser_semantic_run import load_shards
from parser_s1_run import verify_transport


class SemanticTransportTests(unittest.TestCase):
    def test_manifest_requires_frozen_bytes_and_unique_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / 'shard-000.jsonl'
            shard.write_text('{"id":"case"}\n')
            item = {'file': shard.name, 'cases': 1, 'sha256': hashlib.sha256(shard.read_bytes()).hexdigest()}
            marker = {'complete': True, 'accepted_cases': 1, 'shards': [item]}
            (root / 'complete.json').write_text(json.dumps(marker))
            self.assertEqual(load_shards(root)[1], [(shard, ['case'])])
            shard.write_text('{"id":"other"}\n')
            with self.assertRaisesRegex(ValueError, 'digest'):
                load_shards(root)
            shard.write_text('{"id":"case"}\n')
            marker['shards'].append(item)
            (root / 'complete.json').write_text(json.dumps(marker))
            with self.assertRaisesRegex(ValueError, 'identity'):
                load_shards(root)

    def test_transport_preserves_values_identity_order_and_length(self):
        with tempfile.TemporaryDirectory() as directory:
            source, result = Path(directory) / 'source', Path(directory) / 'result'
            records = [{'id': 'a', 'value': 1}, {'id': 'b', 'value': 2}]
            source.write_text(''.join(json.dumps(r) + '\n' for r in records))
            result.write_text(source.read_text())
            verify_transport(source, result, ['a', 'b'])
            for changed in [records[::-1], records[:1], [dict(records[0], value=2), records[1]]]:
                result.write_text(''.join(json.dumps(r) + '\n' for r in changed))
                with self.assertRaises(ValueError):
                    verify_transport(source, result, ['a', 'b'])
            result.write_text(source.read_text())
            with self.assertRaisesRegex(ValueError, 'identities'):
                verify_transport(source, result, ['b', 'a'])


if __name__ == '__main__':
    unittest.main()
