import pytest
from fastapi.testclient import TestClient

import main


@pytest.fixture
def client():
    with TestClient(main.app) as test_client:  # runs the lifespan too
        yield test_client
