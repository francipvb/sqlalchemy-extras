"""
FastAPI dependencies for SQLAlchemy.

Use the content of this module from your FastAPI application to interact
with SQLAlchemy.

For details, see help of individual functions.
"""
from contextlib import asynccontextmanager, contextmanager
from functools import cached_property
from logging import getLogger
from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    ContextManager,
    Generator,
    Optional,
    Union,
)

from fastapi import Request
from sqlalchemy import URL, Connection, make_url, create_engine, Engine
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)
from sqlalchemy.orm import sessionmaker, Session

from .utils import make_async_url

log = getLogger(__name__)


class _WithURL:
    def __init__(self, url: Union[str, URL]) -> None:
        if not isinstance(url, URL):
            self._url = make_url(url)
        else:
            self._url = url

    @property
    def url(self) -> URL:
        return self._url


class EngineFactory(_WithURL):
    def __init__(self, url: Union[str, URL]) -> None:
        super().__init__(url)

        log.info("Initialicing engine.")
        engine = create_engine(self.url, pool_pre_ping=True)
        log.info("Engine initialized successfully.")
        self._engine = engine

    @property
    def engine(self) -> Engine:
        return self._engine

    @cached_property
    def sessionmaker(self) -> sessionmaker[Session]:
        log.debug("Initialicing a session factory.")
        factory = sessionmaker(self.engine)
        log.debug("Session factory initialized.")
        return factory

    @contextmanager
    def session(self, *, in_transaction: bool = False) -> Generator[Session, Any, None]:
        session_contextmanager = None
        if in_transaction:
            log.debug("Starting a transactional session.")
            session_contextmanager = self.sessionmaker.begin()
        else:
            log.debug("Starting a non-transactional session.")
            session_contextmanager = self.sessionmaker()
        try:
            log.debug("Connecting to the database.")
            with session_contextmanager as session:
                yield session
                if session.in_transaction():
                    log.debug(
                        "Changes to the session will be committed as the session is in "
                        "a transaction."
                    )
        finally:
            log.debug("Session terminated.")

    @contextmanager
    def connection(
        self, *, in_transaction: bool = False
    ) -> Generator[Connection, Any, None]:
        connection_contextmanager: Optional[ContextManager[Connection]] = None
        if in_transaction:
            log.debug("Starting a transactional database connection.")
            connection_contextmanager = self.engine.begin()
        else:
            log.debug("Starting a non-transactional database connection.")
            connection_contextmanager = self.engine.connect()

        try:
            log.debug("Connectiong to the database.")
            with connection_contextmanager as connection:
                yield connection
                if connection.in_transaction():
                    log.debug("Commmitting transaction.")
        finally:
            log.debug("Connection has been closed.")

    def get_session(self, request: Request):
        with self.session(
            in_transaction=request.method.lower() in ("post", "put", "patch", "delete")
        ) as session:
            yield session

    def get_connection(self, request: Request):
        with self.connection(
            in_transaction=request.method.lower() in ("post", "put", "patch", "delete")
        ) as connection:
            yield connection


class AsyncEngineFactory(_WithURL):
    def __init__(self, url: Union[str, URL]) -> None:
        changed_url = make_async_url(url)
        super().__init__(changed_url)

        log.info("Setting up an async engine instance.")
        engine = create_async_engine(self.url, pool_pre_ping=True)
        log.info("Async engine connection established.")
        self._engine = engine

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @cached_property
    def sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        log.debug("Initialicing an async session factory.")
        factory = async_sessionmaker(self.engine)
        log.debug("Session factory initialized.")
        return factory

    def get_engine(self) -> AsyncEngine:
        return self.engine

    @asynccontextmanager
    async def session(
        self, *, in_transaction: bool = False
    ) -> AsyncGenerator[AsyncSession, None]:
        session_contextmanager: Optional[AsyncContextManager[AsyncSession]] = None
        if in_transaction:
            log.debug("Starting a transactional async session.")
            session_contextmanager = self.sessionmaker.begin()
        else:
            log.debug("Starting an async session.")
            session_contextmanager = self.sessionmaker()
        try:
            log.debug("Starting async session.")
            async with session_contextmanager as session:
                log.debug("Connected to the database with an async session.")
                yield session
                if session.in_transaction():
                    log.debug(
                        "Changes to the session will be committed because the session "
                        "is in a transaction."
                    )
        finally:
            log.debug("Async session terminated.")

    @asynccontextmanager
    async def connection(
        self, *, in_transaction: bool = False
    ) -> AsyncGenerator[AsyncConnection, None]:
        connection_contextmanager: Optional[AsyncContextManager[AsyncConnection]] = None
        if in_transaction:
            connection_contextmanager = self.engine.begin()
        else:
            connection_contextmanager = self.engine.connect()
        try:
            async with connection_contextmanager as connection:
                yield connection
        finally:
            pass

    async def get_session(self, request: Request):
        async with self.session(
            in_transaction=request.method.lower() in ("post", "put", "patch", "delete")
        ) as session:
            yield session

    async def get_connection(self, request: Request):
        async with self.connection(
            in_transaction=request.method.lower() in ("post", "put", "patch", "delete")
        ) as connection:
            yield connection
