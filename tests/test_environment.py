import sys

import polars
import requests


def test_python_version_is_312_or_newer():
    assert sys.version_info >= (3, 12)


def test_core_dependencies_import():
    assert polars.__version__
    assert requests.__version__
