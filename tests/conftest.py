import pytest
import redis as redis_client

import limitlion

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 1


@pytest.fixture
def redis():
    client = redis_client.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture
def limitlion_fixture(redis):
    limitlion.throttle_configure(redis, True)
