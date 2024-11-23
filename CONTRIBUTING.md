#### Contributing

The `ndbc-api` is actively maintained, please feel free to open a pull request if you have any suggested improvements, test coverage is strongly preferred.

##### Testing

Tests are prepared and executed using the `pytest` framework, and designed to use cached responses rather than making new HTTP requests to the NDBC Data Service. In order to run tests, you will need to install the additional packages in `requirements_dev.txt` (also encoded in the `dev` group in `pyproject.toml`)

For pip installation, please create a clean virtual environment and run:

```bash
pip install -r requirements.txt
pip install -r requirements_dev.txt
```

For poetry-managed installations, please run:

```bash
poetry install
```

##### Running Tests

All tests can be run from the root directory using `python3 -m pytest --run-slow --run-private`.

The two flags in the command above are optional, but can be useful for running all tests, including those marked as slow or private.  The `--run-slow` flag will run tests marked with the `@pytest.mark.slow` decorator, and the `--run-private` flag will run tests marked with the `@pytest.mark.private` decorator. Tests which take more than 30 seconds are typically marked as slow, while test for internals are marked as private.

##### Pull Requests

In order for a pull request to merge to `main`, all tests must pass, and the code must be reviewed by at least one other contributor.

Breaking changes will be considered, especially in the current `alpha` state of the package on `PyPi`.  As the API further matures, breaking changes will only be considered with new major versions (e.g. `N.0.0`).

Alternatively, if you have an idea for a new capability or improvement, feel free to open a feature request issue outlining your suggestion and the ways in which it will empower the atmospheric research community.
