from asyncio import get_event_loop_policy

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from sqlalchemy_extras.fastapi import AsyncEngineFactory, EngineFactory


@pytest.fixture(scope="module")
def engine_factory():
    return EngineFactory("sqlite:///:memory:")


@pytest.fixture(scope="module")
def async_engine_factory():
    return AsyncEngineFactory("sqlite:///:memory:")


@pytest.fixture(scope="module")
def event_loop():
    return get_event_loop_policy().new_event_loop()


@pytest.fixture()
def app():
    app = FastAPI()
    return app


@pytest.fixture()
def client(app: FastAPI):
    return TestClient(app)


def test_get_engine(app: FastAPI, client: TestClient, engine_factory: EngineFactory):
    @app.get("/")
    def get_database(  # type: ignore
        engine: Session = Depends(engine_factory.get_session),
    ) -> str:
        assert engine.bind is not None

        if isinstance(engine.bind, Engine):
            return engine.bind.url.database or ""
        else:
            return engine.bind.engine.url.database or ""

    with client:
        database = client.get("/").json()
        assert database == ":memory:"


def test_get_async_engine(
    app: FastAPI, client: TestClient, async_engine_factory: AsyncEngineFactory
):
    @app.get("/")
    def get_database(  # type: ignore
        engine: AsyncSession = Depends(async_engine_factory.get_session),
    ) -> str:
        assert engine.bind is not None

        if isinstance(engine.bind, Engine):
            return engine.bind.url.database or ""
        else:
            return engine.bind.engine.url.database or ""

    with client:
        database = client.get("/").json()
        assert database == ":memory:"
