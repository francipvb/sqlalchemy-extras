from typing import Optional, Union

from sqlalchemy.engine import URL, make_url


def pluralize(word: str):
    """Return a plural form of a nown.
    >>> pluralize('object')
    'objects'
    >>> pluralize('entry')
    'entries'
    >>> pluralize('potato')
    'potatoes'
    >>> pluralize('banana')
    'bananas'
    >>>
    """
    x = word.lower()
    if x.endswith("y"):
        return f"{x[:-1]}ies"
    elif x.endswith("s") | x.endswith("o") | x.endswith("u"):
        return f"{x}es"
    else:
        return f"{x}s"


ASYNC_ENGINES = {
    "postgresql": "{engine}+asyncpg",
    "postgres": "postgresql+asyncpg",
    "sqlite": "{engine}+aiosqlite",
}


def make_async_url(url: Optional[Union[str, URL]]):
    """Fix an URL to be async compatible assigning an engine and fixing the scheme.

    Args:
        url (str | URL): The raw URL.

    Raises:
        ValueError: If the URL is None.
        RuntimeError: The database is not supported.

    Returns:
        URL: The new, valid URL.

    >>> make_async_url('sqlite:///:memory:')
    sqlite+aiosqlite:///:memory:
    >>> make_async_url('postgres://user:pass@server:5432/db')
    postgresql+asyncpg://user:***@server:5432/db
    >>> make_async_url('postgresql://user:pass@server:5432/db')
    postgresql+asyncpg://user:***@server:5432/db
    >>> make_async_url('postgresql+asyncpg://user:pass@server:5432/db')
    postgresql+asyncpg://user:***@server:5432/db
    >>> make_async_url(None)
    Traceback (most recent call last):
      ...
    ValueError: ...
    >>> make_async_url('mongodb://testserver:37017/test')
    Traceback (most recent call last):
      ...
    ValueError: ...
    >>>
    """
    if url is None:
        raise ValueError("the `url` parameter is None.")

    try:
        original_url = make_url(url)
        async_url = original_url.set(
            drivername=ASYNC_ENGINES[original_url.get_backend_name()].format(
                engine=original_url.get_backend_name(),
            )
        )
    except KeyError:
        raise ValueError("Database not supported.")
    else:
        return async_url
