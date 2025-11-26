from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional
import re
from sqlalchemy.engine import Engine

class ObjectType(Enum):
    """Types of database objects"""
    TABLE = auto()
    VIEW = auto()
    MATERIALIZED_VIEW = auto()
    UNKNOWN = auto()

class DBMSType(Enum):
    """Supported database management systems"""
    ORACLE = auto()
    POSTGRESQL = auto()
    CLICKHOUSE = auto()

    @classmethod
    def from_engine(cls, engine: Engine) -> 'DBMSType':
        """Infer DBMS type from SQLAlchemy engine"""
        dialect = engine.dialect.name.lower()
        if dialect == 'oracle':
            return cls.ORACLE
        elif dialect in ('postgresql', 'postgres'):
            return cls.POSTGRESQL
        elif dialect == 'clickhouse':
            return cls.CLICKHOUSE
        raise ValueError(f"Unsupported engine dialect: {dialect}")


@dataclass(frozen=True)
class DataReference:
    """Immutable reference to a database object"""
    name: str
    schema: Optional[str] = None

    def __post_init__(self):
        self._validate()

    def _validate(self):
        """Validate table reference parameters"""
        if not re.match(r'^[a-zA-Z0-9_]+$', self.name):
            raise ValueError(f"Invalid table name: {self.name}")
        if self.schema and not re.match(r'^[a-zA-Z0-9_]+$', self.schema):
            raise ValueError(f"Invalid schema name: {self.schema}")

    @property
    def full_name(self) -> str:
        """Get fully qualified object name"""
        return f"{self.schema}.{self.name}" if self.schema else self.name