# Python bindings for the Database Stream Processor (DBSP) HTTP API

See [project homepage](https://github.com/feldera/dbsp).

## Development

tldr:

```bash
cargo make --cwd crates/pipeline_manager/ python_test
```

To avoid installing the library every time you make updates and run the test
scripts, you can set up a local development environment using a virtual
environment and then install your library in editable mode. This allows you to
make changes to the library code and have those changes immediately reflected
when running your test scripts. Here's how you can do it:

1. Create a virtual environment: Open a terminal/command prompt and navigate to
   your project directory. Then, create a virtual environment by running the
   following command in the dbsp base directory:

```bash
python3 -m venv venv
```


2. Activate the virtual environment: Activate the virtual environment using the
   appropriate command for your operating system:

- On macOS/Linux:

```bash
source venv/bin/activate
```

- On Windows:
```bash
venv\Scripts\activate.bat
```

3. Install your library in editable mode: While the virtual environment is
   active, navigate to the directory where your library's `pyproject.toml` file
   is located. Then, install the library in editable mode using `pip` as
   follows:

```bash
cd python/dbsp-api-client
pip install -e .
cd ..
pip install -e .
```


This will create a symlink from your virtual environment to your library code,
allowing changes to be immediately reflected.

4. Run your test scripts: You can now run your test scripts without having to
   install the library again. The virtual environment will use the locally
   installed version of your library.

```bash
cd demo
bash demo.sh
```

Whenever you make changes to your library code, you don't need to reinstall it.
Simply rerun your test scripts, and the changes will be picked up automatically.

Remember to activate the virtual environment every time you work on your project
or want to run the test scripts.


## Regenerate dbsp-api-client code

In case you make changes to the OpenAPI spec (by modifying
crates/pipeline_manager), you'll need to regenerated the `dbsp-api-client`
library. To do so, run the following command:

```bash
cargo make --cwd crates/pipeline_manager/ openapi_python
```