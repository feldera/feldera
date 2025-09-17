Thanks to the suggestion from @hoefling, I was able to use `pytest-xdist` hooks and `pytest_generate_tests` to get a working solution.

    # in conftest.py
    def pytest_configure(config):
        if is_master(config):
            # share db_entries across master and worker nodes.
            config.db_samples = get_db_entries()

    def pytest_configure_node(node):
        """xdist hook"""
        node.workerinput['db_entries'] = node.config.db_samples

    def is_master(config):
        """True if the code running the given pytest.config object is running in a xdist master
        node or not running xdist at all.
        """
        return not hasattr(config, 'workerinput')

    def pytest_generate_tests(metafunc):
        if 'sample' in metafunc.fixturenames:
            if is_master(metafunc.config):
                samples = metafunc.config.db_samples
            else:
                samples = metafunc.config.workerinput['db_entries']
            metafunc.parametrize("sample", samples)

    @pytest.fixture
    def sample(request):
        return request.param

    # in test-db.py
    def test_db(sample):
        # test one sample.

@hoefling suggestion to use `pytest_generate_tests` hook was the missing piece. Using this hook to parametrize the test fixture helped in solving this problem.