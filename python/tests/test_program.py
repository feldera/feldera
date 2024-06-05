import unittest

import uuid
import logging
import sys

from tests import TEST_CLIENT
from feldera.rest.program import Program
from feldera.enums import CompilationProfile
from typing import Optional

NAME = str(uuid.uuid4())


class TestProgram(unittest.TestCase):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    root.addHandler(handler)

    def test_compile_program(self, name: str = NAME, config: Optional[dict] = None, delete: bool = True):
        sql = """
    CREATE TABLE test_table (
        id INT,
        name VARCHAR(255)
    );
    CREATE VIEW V AS SELECT * FROM test_table;
    """
        program = Program(name, sql)
        TEST_CLIENT.compile_program(program, config)
        assert program.version == 1

        if delete:
            TEST_CLIENT.delete_program(name)

    def test_delete_program(self, name: str = NAME):
        self.test_compile_program(name, delete=False)

        TEST_CLIENT.delete_program(name)

    def test_list_programs(self):
        self.test_compile_program(NAME, delete=False)
        programs = TEST_CLIENT.programs()
        assert len(programs) > 0
        assert NAME in [p.name for p in programs]
        self.test_delete_program(NAME)

    def test_get_program(self):
        self.test_compile_program(NAME, delete=False)
        program = TEST_CLIENT.get_program(NAME)
        assert program.name == NAME
        assert program.version == 1
        self.test_delete_program(NAME)

    def __test_compilation_profile(self, profile: CompilationProfile = None):
        self.test_compile_program(str(uuid.uuid4()), config={"profile": profile.value}, delete=True)

    def test_compilation_profiles(self):
        for profile in CompilationProfile:
            self.__test_compilation_profile(profile)


if __name__ == '__main__':
    unittest.main()
