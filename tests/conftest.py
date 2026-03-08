import pytest

from src.utils.spark import build_spark


@pytest.fixture(scope="session")
def spark():
    session = build_spark("health-insurance-tests")
    yield session
    session.stop()

