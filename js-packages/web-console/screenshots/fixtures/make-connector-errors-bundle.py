"""Writes connector-errors-support-bundle.zip: the last snapshot of
fraud-detection-support-bundle.zip, with parse errors added to the `customer` input connector.
The real pipeline has no connector errors, and the docs need a screenshot of them.

Run in this directory: python3 make-connector-errors-bundle.py
"""

import json
import zipfile

SOURCE = "fraud-detection-support-bundle.zip"
TARGET = "connector-errors-support-bundle.zip"
SNAPSHOT = "2026-10-02T09-23-50.186200122Z"

ERRORS = [
    (
        "2026-10-02T09:23:41.512Z",
        "failed to deserialize JSON record: error parsing field 'lat': invalid type: "
        'string "n/a", expected f64 at line 1 column 47\n'
        'Invalid fragment: \'{"cc_num": 4532015112830366, "name": "Ann Lee", "lat": "n/a", '
        '"long": -122.42}\'',
    ),
    (
        "2026-10-02T09:23:44.087Z",
        "failed to deserialize JSON record: missing field `cc_num` at line 1 column 52\n"
        'Invalid fragment: \'{"name": "Raj Patel", "lat": 40.71, "long": -74.0}\'',
    ),
    (
        "2026-10-02T09:23:47.930Z",
        "failed to deserialize JSON record: error parsing field 'cc_num': invalid type: "
        'string "4716-1234", expected i64 at line 1 column 21\n'
        'Invalid fragment: \'{"cc_num": "4716-1234", "name": "Mia Wong", "lat": 51.5, '
        '"long": -0.12}\'',
    ),
]


def add_errors(stats: dict) -> dict:
    customer = next(i for i in stats["inputs"] if i["endpoint_name"] == "customer.customer")
    customer["parse_errors"] = [
        {"index": index, "message": message, "tag": None, "timestamp": timestamp}
        for index, (timestamp, message) in enumerate(ERRORS)
    ]
    customer["metrics"]["num_parse_errors"] = len(ERRORS)
    return stats


def keep_snapshot(metadata: dict) -> dict:
    metadata["collections"] = [c for c in metadata["collections"] if c["directory"] == SNAPSHOT]
    return metadata


with zipfile.ZipFile(SOURCE) as source, zipfile.ZipFile(TARGET, "w", zipfile.ZIP_DEFLATED) as target:
    for info in source.infolist():
        name = info.filename
        if name == "metadata.json":
            data = json.dumps(keep_snapshot(json.loads(source.read(name))), indent=2).encode()
        elif name == f"{SNAPSHOT}/stats.json":
            data = json.dumps(add_errors(json.loads(source.read(name))), indent=2).encode()
        elif name.startswith(f"{SNAPSHOT}/") or name == "metadata.txt":
            data = source.read(name)
        else:
            continue
        # A fixed date keeps the zip the same from run to run.
        target.writestr(zipfile.ZipInfo(name, date_time=(2026, 10, 2, 9, 23, 50)), data,
                        zipfile.ZIP_DEFLATED)
