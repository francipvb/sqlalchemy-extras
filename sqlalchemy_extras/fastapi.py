"""
FastAPI dependencies for SQLAlchemy.

Use the content of this module from your FastAPI application to interact
with SQLAlchemy.

For details, see help of individual functions.
"""
import os
from asyncio import current_task
from logging import getLogger
from typing import Any, AsyncGenerator, Optional, Union

from fastapi import Depends, FastAPI, Request
from sqlalchemy import func
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import DatabaseError, ProgrammingError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_scoped_session,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker

from .utils import make_async_url

_ENGINE_APP_STATE_KEY = "__app_engine__"
_SESSION_FACTORY_APP_STATE_KEY = "__session_factory__"
log = getLogger(__name__)


def _get_engine(app: FastAPI) -> AsyncEngine:
    try:
        return getattr(app.state, _ENGINE_APP_STATE_KEY)
    except AttributeError:
        raise RuntimeError("Database not initialized.")


def _set_engine(app: FastAPI, engine: AsyncEngine):
    setattr(app.state, _ENGINE_APP_STATE_KEY, engine)


def _get_session_factory(app: FastAPI) -> async_scoped_session:
    try:
        return getattr(app.state, _SESSION_FACTORY_APP_STATE_KEY)
    except AttributeError:
        raise RuntimeError("Database session factory not provided.")


def _set_session_factory(app: FastAPI, factory: async_scoped_session):
    setattr(app.state, _SESSION_FACTORY_APP_STATE_KEY, factory)


def setup_engine(  # noqa C901
    app: FastAPI,
    *,
    url: Optional[Union[str, URL]] = None,
    **kwargs: Any,
):
    """Setup SQLALchemy async engine.

    This function adds startup and shutdown handlers to the supplied FastAPI instance.

    If the URL is not provided, it is taken from the environment variables.

    Provide it as the 'DATABASE_URL' environment variable.

    Args:
        app (FastAPI): The FastAPI application instance.
        url (Optional[Union[str, URL]], optional): The database URL to be used.

    Raises:
        RuntimeError: If the URL is not provided.
    """
    if url is None:
        log.info("Retrieving database URL from environment variable.")
        try:
            url = os.environ["DATABASE_URL"]
        except KeyError:
            raise RuntimeError("Database URL not provided.")

    log.info("Parsing database URL.")
    url = make_async_url(url)

    @app.on_event("startup")
    async def initialize_sqlalchemy():
        log.info("Connecting to '%s' using '%s'.", url.database, url.get_backend_name())
        engine = create_async_engine(url, **kwargs)
        log.info("Testing connection...")
        try:
            async with engine.connect() as conn:
                scalar = await conn.scalar(func.current_timestamp())
                log.info("Query result is '%s'.", scalar)
        except ProgrammingError:  # pragma nocover
            log.warning("Error trying to run a test query.", exc_info=True)

        session = async_scoped_session(
            sessionmaker(
                bind=engine,
                class_=AsyncSession,  # type: ignore
            ),
            current_task,
        )
        _set_engine(app, engine)
        _set_session_factory(app, session)

    @app.on_event("shutdown")
    async def dispose_engine():
        log.info("Closing all database sessions.")
        await _get_session_factory(app).close()

        log.info("Closing engine.")
        await _get_engine(app).dispose()


def engine(request: Request) -> AsyncEngine:
    """Return the engine from the current request.

    This is meant to be used as a fastapi dependency.

    Args:
        request (Request): The current request.

    Returns:
        AsyncEngine: The async engine.
    """
    return _get_engine(request.app)


def session_factory(request: Request) -> async_scoped_session:
    """Return a session factory for the request.

    Args:
        request (Request): The current request.

    Returns:
        async_scoped_session: A session factory.
    """
    return _get_session_factory(request.app)


async def sqlalchemy_session(
    factory: async_scoped_session = Depends(session_factory),
) -> AsyncGenerator[AsyncSession, None]:
    """Return a sqlalchemy session.

    This is meant to be used as a fastapi dependency.

    Args:
        factory: The session factory.

    Returns:
        AsyncGenerator[AsyncSession, None]: The async session, wrapped in a generator.

    """
    session = factory()

    async with session:
        try:
            yield session
        except DatabaseError:
            raise
        else:
            await session.commit()


async def sqlalchemy_connection(
    engine: AsyncEngine = Depends(engine),
) -> AsyncGenerator[AsyncConnection, None]:
    """Return a sqlalchemy async connection.

    This is meant to be used as a fastapi dependency.

    Args:
        engine (AsyncEngine, optional): The engine to create the session for.

    Returns:
        AsyncGenerator[AsyncConnection, None]: The connecttttttion.

    """
    async with engine.connect() as conn:
        yield conn
