since version 2.8, `pytest` has a powerful cache mechanism.

### Usage example

    @pytest.fixture(autouse=True)
    def init_cache(request):
        data = request.config.cache.get('my_data', None)
        data = {'spam': 'eggs'}
        request.config.cache.set('my_data', data)

Access the data dict in tests via builtin `request` fixture:

    def test_spam(request):
        data = request.config.cache.get('my_data')
        assert data['spam'] == 'eggs'

### Sharing the data between test runs

The cool thing about `request.cache` is that it is persisted on disk, so it can be even shared between test runs. This comes handy when you running tests distributed (`pytest-xdist`) or have some long-running data generation which does not change once generated:

    @pytest.fixture(autouse=True)
    def generate_data(request):
        data = request.config.cache.get('my_data', None)
        if data is None:
            data = long_running_generation_function()
            request.config.cache.set('my_data', data)

Now the tests won't need to recalculate the value on different test runs unless you clear the cache on disk explicitly. Take a look what's currently in the cache:

    $ pytest --cache-show
    ...
    my_data contains:
      {'spam': 'eggs'}

Rerun the tests with the `--cache-clear` flag to delete the cache and force the data to be recalculated. Or just remove the `.pytest_cache` directory in the project root dir.

### Where to go from here

The related section in `pytest` docs: [Cache: working with cross-testrun state](https://docs.pytest.org/en/latest/cache.html).