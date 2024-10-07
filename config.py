from typing import TypedDict, Final, Any


class AdditionalColumn(TypedDict):
    type: str
    default_value: Any


class Config(TypedDict):
    sqlite_db_path: str
    postgresql_conn_params: dict
    additional_columns: dict[str, AdditionalColumn]


CONFIG: Final[Config] = {
    "sqlite_db_path": "/path/to/db.db",
    "postgresql_conn_params": {
        "dbname": "your_db",
        "user": "username",
        "password": "password",
        "host": "localhost"
    },
    "additional_columns": {
        "test_column": {
            "type": "INTEGER",
            "default_value": 1
        }
    }
}  # example
