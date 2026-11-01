import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """Without this, every call to a @task/@flow-decorated function outside an
    existing flow run (most of our unit tests call stage functions directly)
    spins up its own temporary Prefect server -- ~2.5s overhead per call,
    8x'ing the whole suite's runtime (Lesson 20). This harness gives the whole
    test session one shared, lightweight test environment instead.
    """
    with prefect_test_harness():
        yield
