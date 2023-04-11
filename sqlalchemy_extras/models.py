# pylint: disable=not-callable
from datetime import datetime
import typing

from sqlalchemy import TIMESTAMP, false, func, orm
from sqlalchemy.types import TypeEngine

from .utils import pluralize

_TId = typing.TypeVar("_TId")


class SoftDeletable:
    """A soft-deletable model mixing.

    This adds a `is_deleted` column to mark the instances as deleted.

    >>> from sqlalchemy.orm.decl_api import declarative_base
    >>> from sqlalchemy import Column, Integer
    >>> Base = declarative_base()
    >>> class Item(SoftDeletable, Base):
    ...     __tablename__ = 'items'
    ...     id = Column(Integer, primary_key=True, autoincrement=True)
    ...
    >>> item = Item()
    >>> item.is_removed

    >>> item.mark_removed()

    >>> item.is_removed
    True
    >>> item.mark_removed()
    Traceback (most recent call last):
      ...
    AssertionError: ...
    >>> item.restore()

    >>> item.is_removed
    False
    >>> item.restore()
    Traceback (most recent call last):
      ...
    AssertionError: ...
    >>>
    """

    __abstract__ = True

    is_removed: orm.Mapped[bool] = orm.mapped_column(
        index=True,
        server_default=false(),
        comment="Soft deletion flag.",
    )

    def mark_removed(self):
        assert not self.is_removed, "Already marked."
        self.is_removed = True

    def restore(self):
        assert self.is_removed, "Not removed."
        self.is_removed = False


class Timestamps:
    """Adds timestamp fields.

    >>> from sqlalchemy import Column, Integer
    >>> from sqlalchemy.orm.decl_api import declarative_base
    >>> Base = declarative_base()
    >>> class Item(Timestamps, Base):
    ...     __tablename__ = 'items'
    ...     id = Column(Integer, primary_key=True, autoincrement=True)
    ...
    >>> item=Item()
    >>> item.created

    >>> item.updated

    >>>
    """

    __abstract__ = True

    created: orm.Mapped[datetime] = orm.mapped_column(
        type_=TIMESTAMP(),
        server_default=func.current_timestamp(),
        comment="When a row were added.",
    )
    updated: orm.Mapped[datetime | None] = orm.mapped_column(
        type_=TIMESTAMP(),
        onupdate=func.current_timestamp(),
        comment="Last row update date and time.",
        default=None,
    )

    def touch(self):
        """Update the `updated` timestamp.

        This just sets the `updated` field to the `current_timestamp` function.

        >>> from sqlalchemy import Column, Integer
        >>> from sqlalchemy.orm.decl_api import declarative_base
        >>> Base = declarative_base()
        >>> class Item(Timestamps, Base):
        ...     __tablename__ = 'items'
        ...     id = Column(Integer, primary_key=True, autoincrement=True)
        ...
        >>> item=Item()
        >>> item.touch()
        >>> item.updated
        <sqlalchemy.sql.functions.current_timestamp at ...>
        >>>
        """
        self.updated = func.current_timestamp()  # type: ignore


class Autonamed:
    """Auto-name table.

    >>> from sqlalchemy.orm import DeclarativeBase
    >>> class Base(DeclarativeBase):
    ...     pass
    >>> class Prop(Autonamed, Base):
    ...     id: orm.Mapped[int]=orm.mapped_column(primary_key=True)
    ...
    >>> Prop.__table__.name
    'props'
    >>>
    """

    __abstract__ = True
    __tablename__: typing.ClassVar[str]

    def __init_subclass__(cls) -> None:
        cls.__tablename__ = pluralize(cls.__name__)
        super().__init_subclass__()


class WithPK(typing.Generic[_TId]):
    """Automatically add a primary key to a subclass.

    Example usage:
    >>> from sqlalchemy import orm
    >>> class Base(orm.DeclarativeBase):
    ...     pass
    ...
    >>>
    >>> class Item(WithPK[int], Base):
    ...     __tablename__='items'
    ...
    >>> Item.id.expression.type
    Integer()
    >>>
    """

    __abstract__ = True

    __pk_type__: typing.ClassVar[typing.Optional[TypeEngine[typing.Any]]] = None
    __pk_kwargs__: typing.ClassVar[
        typing.Optional[typing.Mapping[str, typing.Any]]
    ] = None

    id: orm.Mapped[_TId]

    def __init_subclass__(cls, *args: typing.Any, **kwargs: typing.Any) -> None:
        orig_bases: typing.Tuple[typing.Any, ...] = cls.__orig_bases__  # type: ignore
        entity_name: str = cls.__name__.lower()  # type: ignore

        column_kwargs: typing.Dict[str, typing.Any] = (
            dict(cls.__pk_kwargs__) if cls.__pk_kwargs__ is not None else dict()
        )
        if cls.__pk_type__ is not None:
            column_kwargs.setdefault("type_", cls.__pk_type__)
        if "name" not in column_kwargs:
            column_kwargs.update(
                name=f"{entity_name}_id",
            )

        for item in orig_bases:  # type: ignore
            if typing.get_origin(item) is WithPK:  # type: ignore
                pk_type = typing.get_args(item)[0]
                attr_value = orm.mapped_column(
                    primary_key=True,
                    **column_kwargs,
                )
                cls.__annotations__["id"] = orm.Mapped[pk_type]
                cls.id = attr_value
                break

        else:
            raise TypeError(
                f"Class `{WithPK.__module__}.{WithPK.__qualname__}` not found in the "
                f"class `{cls.__module__}.{cls.__qualname__}` inheritance tree."
            )

        super().__init_subclass__(*args, **kwargs)
