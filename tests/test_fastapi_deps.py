from asyncio import get_event_loop_policy
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from sqlalchemy_extras.fastapi import (
    engine,
    session_factory,
    setup_engine,
    sqlalchemy_connection,
    sqlalchemy_session,
)


@pytest.fixture()
def event_loop():
    return get_event_loop_policy().new_event_loop()


@pytest.fixture()
def app():
    app = FastAPI()
    setup_engine(app, url="sqlite:///:memory:")
    return app


@pytest.fixture()
def client(app: FastAPI):
    return TestClient(app)


def test_get_engine(app: FastAPI, client: TestClient):
    @app.get("/")
    def get_database(engine: AsyncEngine = Depends(engine)):
        return engine.url.database

    with client:
        database = client.get("/").json()
        assert database == ":memory:"


def test_get_session_factory(app: FastAPI, client: TestClient):
    @app.get("/")
    async def test_func(
        engine: AsyncEngine = Depends(engine), factory=Depends(session_factory)
    ):
        assert factory.bind == engine

    with client:
        client.get("/")


def test_get_session(app: FastAPI, client: TestClient):
    @app.get("/")
    def test_func(
        engine: AsyncEngine = Depends(engine),
        session=Depends(sqlalchemy_session),
    ):
        assert session.bind == engine

    with client:
        client.get("/")


def test_engine_not_initialized(app: FastAPI, client: TestClient):
    @app.get("/engine")
    async def get_engine(engine: AsyncEngine = Depends(engine)):
        assert engine.url.database == ":memory:"

    @app.get("/session")
    async def get_session_factory(engine: AsyncEngine = Depends(session_factory)):
        assert engine.url.database == ":memory:"

    with pytest.raises(RuntimeError):
        client.get("/engine")

    with pytest.raises(RuntimeError):
        client.get("/session")


def test_setup_without_url():
    app = FastAPI()
    with pytest.raises(RuntimeError):
        setup_engine(app)


def test_setup_with_environment_variable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: str
):
    db_file = Path(tmp_path, "db.sqlite")
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_file}")
    app = FastAPI()

    @app.get("/")
    async def get_database(engine: AsyncEngine = Depends(engine)):
        return engine.url.database

    setup_engine(app)
    with TestClient(app) as client:
        result = client.get("/").json()
        assert result == str(db_file)


def test_get_connection(app: FastAPI, client: TestClient):
    @app.get("/")
    async def get_connection(conn: AsyncConnection = Depends(sqlalchemy_connection)):
        return await conn.scalar(func.current_timestamp())

    with client:
        client.get("/")
