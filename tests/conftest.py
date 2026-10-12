import pytest
import pytest_asyncio
from sqslite.broker import Broker
from sqslite.api import create_app
import tempfile
import os

@pytest_asyncio.fixture
async def temp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)

@pytest_asyncio.fixture
async def broker(temp_db):
    b = Broker(temp_db)
    await b.connect()
    yield b
    await b.close()

@pytest_asyncio.fixture
async def client(aiohttp_client, temp_db):
    app = create_app(temp_db)
    return await aiohttp_client(app)
