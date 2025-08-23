from tests.runtime_aggtest.aggtst_base import TstTable, TstView


class aggtst_uuid_tbl(TstTable):
    """Define the table used by the UUID tests"""

    def __init__(self):
        self.sql = """CREATE TABLE uuid_tbl(
                      id INT,
                      c1 UUID,
                      c2 UUID)"""
        self.data = [
            {
                "id": 0,
                "c1": "724b11b7-5a71-4d18-b241-299f82d9b403",
                "c2": "859d5430-c3e1-4544-b107-b888582902e2",
            },
            {
                "id": 0,
                "c1": "ba15a45f-e479-4fa7-a45e-b73e3d03436b",
                "c2": "cd88e41a-6ffb-4aaf-826e-efffdedf3c31",
            },
            {
                "id": 1,
                "c1": "2976a23e-788c-419e-be56-4fb1f1013761",
                "c2": "9a6b78db-668a-4507-b5b5-1f164e6f1eba",
            },
            {
                "id": 1,
                "c1": "8bad7a16-7c4d-490a-9f97-50c51e7990ec",
                "c2": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            },
        ]


class aggtst_uuid_to_char(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1_char": "724b11b7", "c2_char": "8"},
            {"id": 0, "c1_char": "ba15a45f", "c2_char": "c"},
            {"id": 1, "c1_char": "2976a23e", "c2_char": "9"},
            {"id": 1, "c1_char": "8bad7a16", "c2_char": "4"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_to_char AS SELECT
                      id,
                      CAST(c1 AS CHAR(8)) AS c1_char,
                      CAST(c2 AS CHAR) AS c2_char
                      FROM uuid_tbl"""


class aggtst_uuid_to_vchar(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1_vchar": "724b11b7-5a71-4d18-b241-299f82d9b403",
                "c2_vchar": "859d5430-c3e1-4544-b107-b888582902e2",
            },
            {
                "id": 0,
                "c1_vchar": "ba15a45f-e479-4fa7-a45e-b73e3d03436b",
                "c2_vchar": "cd88e41a-6ffb-4aaf-826e-efffdedf3c31",
            },
            {
                "id": 1,
                "c1_vchar": "2976a23e-788c-419e-be56-4fb1f1013761",
                "c2_vchar": "9a6b78db-668a-4507-b5b5-1f164e6f1eba",
            },
            {
                "id": 1,
                "c1_vchar": "8bad7a16-7c4d-490a-9f97-50c51e7990ec",
                "c2_vchar": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_to_vchar AS SELECT
                      id,
                      CAST(c1 AS VARCHAR) AS c1_vchar,
                      CAST(c2 AS VARCHAR) AS c2_vchar
                      FROM uuid_tbl"""


class aggtst_uuid_to_binary(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1_bin": "72", "c2_bin": "859d5430c3e14544b107b888582902e2"},
            {"id": 0, "c1_bin": "ba", "c2_bin": "cd88e41a6ffb4aaf826eefffdedf3c31"},
            {"id": 1, "c1_bin": "29", "c2_bin": "9a6b78db668a4507b5b51f164e6f1eba"},
            {"id": 1, "c1_bin": "8b", "c2_bin": "42b8fec7c7a3453196114bde80f9cb4c"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_to_binary AS SELECT
                      id,
                      CAST(c1 AS BINARY) AS c1_bin,
                      CAST(c2 AS BINARY(16)) AS c2_bin
                      FROM uuid_tbl"""


class aggtst_uuid_to_varbinary(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1_vbin": "724b11b75a714d18b241299f82d9b403",
                "c2_vbin": "859d5430c3e14544b107b888582902e2",
            },
            {
                "id": 0,
                "c1_vbin": "ba15a45fe4794fa7a45eb73e3d03436b",
                "c2_vbin": "cd88e41a6ffb4aaf826eefffdedf3c31",
            },
            {
                "id": 1,
                "c1_vbin": "2976a23e788c419ebe564fb1f1013761",
                "c2_vbin": "9a6b78db668a4507b5b51f164e6f1eba",
            },
            {
                "id": 1,
                "c1_vbin": "8bad7a167c4d490a9f9750c51e7990ec",
                "c2_vbin": "42b8fec7c7a3453196114bde80f9cb4c",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_to_varbinary AS SELECT
                      id,
                      CAST(c1 AS VARBINARY) AS c1_vbin,
                      CAST(c2 AS VARBINARY) AS c2_vbin
                      FROM uuid_tbl"""


# MAX Tests
class aggtst_uuid_max(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "max1": "ba15a45f-e479-4fa7-a45e-b73e3d03436b",
                "max2": "cd88e41a-6ffb-4aaf-826e-efffdedf3c31",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_max AS SELECT
                      MAX(c1) AS max1,
                      MAX(c2) AS max2
                      FROM uuid_tbl"""


class aggtst_uuid_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "max1": "ba15a45f-e479-4fa7-a45e-b73e3d03436b",
                "max2": "cd88e41a-6ffb-4aaf-826e-efffdedf3c31",
            },
            {
                "id": 1,
                "max1": "8bad7a16-7c4d-490a-9f97-50c51e7990ec",
                "max2": "9a6b78db-668a-4507-b5b5-1f164e6f1eba",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_max_gby AS SELECT
                      id,
                      MAX(c1) AS max1,
                      MAX(c2) AS max2
                      FROM uuid_tbl
                      GROUP BY id"""


class aggtst_uuid_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "max1": "8bad7a16-7c4d-490a-9f97-50c51e7990ec",
                "max2": "9a6b78db-668a-4507-b5b5-1f164e6f1eba",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_max_where AS SELECT
                      MAX(c1) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS max1,
                      MAX(c2) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS max2
                      FROM uuid_tbl"""


class aggtst_uuid_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "max1": "724b11b7-5a71-4d18-b241-299f82d9b403",
                "max2": "859d5430-c3e1-4544-b107-b888582902e2",
            },
            {
                "id": 1,
                "max1": "8bad7a16-7c4d-490a-9f97-50c51e7990ec",
                "max2": "9a6b78db-668a-4507-b5b5-1f164e6f1eba",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_max_where_gby AS SELECT
                      id,
                      MAX(c1) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS max1,
                      MAX(c2) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS max2
                      FROM uuid_tbl
                      GROUP BY id"""


# ARG_MAX Tests
class aggtst_uuid_arg_max(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "arg_max1": "ba15a45f-e479-4fa7-a45e-b73e3d03436b",
                "arg_max2": "cd88e41a-6ffb-4aaf-826e-efffdedf3c31",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_arg_max AS SELECT
                      ARG_MAX(c1, c2) AS arg_max1,
                      ARG_MAX(c2, c1) AS arg_max2
                      FROM uuid_tbl"""


class aggtst_uuid_arg_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arg_max1": "ba15a45f-e479-4fa7-a45e-b73e3d03436b",
                "arg_max2": "cd88e41a-6ffb-4aaf-826e-efffdedf3c31",
            },
            {
                "id": 1,
                "arg_max1": "2976a23e-788c-419e-be56-4fb1f1013761",
                "arg_max2": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_arg_max_gby AS SELECT
                      id,
                      ARG_MAX(c1, c2) AS arg_max1,
                      ARG_MAX(c2, c1) AS arg_max2
                      FROM uuid_tbl
                      GROUP BY id"""


class aggtst_uuid_arg_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "arg_max1": "2976a23e-788c-419e-be56-4fb1f1013761",
                "arg_max2": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_arg_max_where AS SELECT
                      ARG_MAX(c1, c2) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS arg_max1,
                      ARG_MAX(c2, c1) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS arg_max2
                      FROM uuid_tbl"""


class aggtst_uuid_arg_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arg_max1": "724b11b7-5a71-4d18-b241-299f82d9b403",
                "arg_max2": "859d5430-c3e1-4544-b107-b888582902e2",
            },
            {
                "id": 1,
                "arg_max1": "2976a23e-788c-419e-be56-4fb1f1013761",
                "arg_max2": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_arg_max_where_gby AS SELECT
                      id,
                      ARG_MAX(c1, c2) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS arg_max1,
                      ARG_MAX(c2, c1) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS arg_max2
                      FROM uuid_tbl
                      GROUP BY id"""


# MIN tests
class aggtst_uuid_min(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "min1": "2976a23e-788c-419e-be56-4fb1f1013761",
                "min2": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_min AS SELECT
                      MIN(c1) AS min1,
                      MIN(c2) AS min2
                      FROM uuid_tbl"""


class aggtst_uuid_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "min1": "724b11b7-5a71-4d18-b241-299f82d9b403",
                "min2": "859d5430-c3e1-4544-b107-b888582902e2",
            },
            {
                "id": 1,
                "min1": "2976a23e-788c-419e-be56-4fb1f1013761",
                "min2": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_min_gby AS SELECT
                      id,
                      MIN(c1) AS min1,
                      MIN(c2) AS min2
                      FROM uuid_tbl
                      GROUP BY id"""


class aggtst_uuid_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "min1": "2976a23e-788c-419e-be56-4fb1f1013761",
                "min2": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_min_where AS SELECT
                      MIN(c1) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS min1,
                      MIN(c2) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS min2
                      FROM uuid_tbl"""


class aggtst_uuid_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "min1": "ba15a45f-e479-4fa7-a45e-b73e3d03436b",
                "min2": "cd88e41a-6ffb-4aaf-826e-efffdedf3c31",
            },
            {
                "id": 1,
                "min1": "2976a23e-788c-419e-be56-4fb1f1013761",
                "min2": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_min_where_gby AS SELECT
                      id,
                      MIN(c1) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS min1,
                      MIN(c2) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS min2
                      FROM uuid_tbl
                      GROUP BY id"""


# ARG_MIN tests
class aggtst_uuid_arg_min(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "arg_min1": "8bad7a16-7c4d-490a-9f97-50c51e7990ec",
                "arg_min2": "9a6b78db-668a-4507-b5b5-1f164e6f1eba",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_arg_min AS SELECT
                      ARG_MIN(c1, c2) AS arg_min1,
                      ARG_MIN(c2, c1) AS arg_min2
                      FROM uuid_tbl"""


class aggtst_uuid_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arg_min1": "724b11b7-5a71-4d18-b241-299f82d9b403",
                "arg_min2": "859d5430-c3e1-4544-b107-b888582902e2",
            },
            {
                "id": 1,
                "arg_min1": "8bad7a16-7c4d-490a-9f97-50c51e7990ec",
                "arg_min2": "9a6b78db-668a-4507-b5b5-1f164e6f1eba",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_arg_min_gby AS SELECT
                      id,
                      ARG_MIN(c1, c2) AS arg_min1,
                      ARG_MIN(c2, c1) AS arg_min2
                      FROM uuid_tbl
                      GROUP BY id"""


class aggtst_uuid_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "arg_min1": "8bad7a16-7c4d-490a-9f97-50c51e7990ec",
                "arg_min2": "9a6b78db-668a-4507-b5b5-1f164e6f1eba",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_arg_min_where AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS arg_min1,
                      ARG_MIN(c2, c1) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS arg_min2
                      FROM uuid_tbl"""


class aggtst_uuid_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "arg_min1": "ba15a45f-e479-4fa7-a45e-b73e3d03436b",
                "arg_min2": "cd88e41a-6ffb-4aaf-826e-efffdedf3c31",
            },
            {
                "id": 1,
                "arg_min1": "8bad7a16-7c4d-490a-9f97-50c51e7990ec",
                "arg_min2": "9a6b78db-668a-4507-b5b5-1f164e6f1eba",
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW uuid_arg_min_where_gby AS SELECT
                      id,
                      ARG_MIN(c1, c2) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS arg_min1,
                      ARG_MIN(c2, c1) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS arg_min2
                      FROM uuid_tbl
                      GROUP BY id"""
