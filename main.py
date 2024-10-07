import re
import sqlite3
import psycopg2
import traceback
from tqdm.auto import tqdm
from rich.console import Console
from typing import Dict, List, Tuple

from config import CONFIG


console = Console()
print = console.print


def connect_to_databases(sqlite_db_path: str, postgresql_conn_params: dict) -> Tuple[sqlite3.Connection, psycopg2.extensions.connection]:
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    pg_conn = psycopg2.connect(**postgresql_conn_params)
    return sqlite_conn, pg_conn


def get_tables(cursor: sqlite3.Cursor) -> List[str]:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [table[0] for table in cursor.fetchall()]


def get_table_schema(cursor: sqlite3.Cursor, table_name: str) -> List[Tuple]:
    cursor.execute(f"PRAGMA table_info({table_name});")
    return cursor.fetchall()


def get_unique_constraints(cursor: sqlite3.Cursor, table_name: str) -> List[Tuple[str, List[str]]]:
    cursor.execute(f"PRAGMA index_list({table_name});")
    indexes = cursor.fetchall()
    unique_constraints = []
    for index in indexes:
        if index[2]:  # UNIQUE
            index_name = index[1]
            cursor.execute(f"PRAGMA index_info({index_name});")
            index_info = cursor.fetchall()
            unique_columns = [info[2] for info in index_info]
            unique_constraints.append((index_name, unique_columns))
    return unique_constraints


def determine_column_type(cursor: sqlite3.Cursor, table_name: str, column_name: str, column_type: str) -> str:
    if column_type.upper() != "INTEGER":
        return convert_type(column_type)
    
    cursor.execute(f"SELECT {column_name} FROM {table_name}")
    sample_data = [row[0] for row in cursor.fetchall() if row[0] is not None]
    
    if not sample_data:
        return "INTEGER"
    
    if any(abs(value) > 2**63 - 1 for value in sample_data):
        return "NUMERIC"
    if any(abs(value) > 2**31 - 1 for value in sample_data):
        return "BIGINT"
    return "INTEGER"


def create_table_query(cursor: sqlite3.Cursor, table_name: str, columns: List[Tuple], additional_columns: Dict, unique_constraints: List[Tuple[str, List[str]]]) -> str:
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column in columns:
        col_name, col_type = column[1], column[2]
        pg_col_type = determine_column_type(cursor, table_name, col_name, col_type)
        constraints = []
        if column[5]:  # PRIMARY KEY
            constraints.append("PRIMARY KEY")
        if column[3]:  # NOT NULL
            constraints.append("NOT NULL")
        if column[4]:  # DEFAULT value
            constraints.append(f"DEFAULT {column[4]}")
        constraints_str = " ".join(constraints)
        
        if constraints_str:
            constraints_str = f" {constraints_str}"
        query += f"{col_name} {pg_col_type}{constraints_str}, "
    
    for col_name, col_info in additional_columns.items():
        pg_col_type = convert_type(col_info["type"])
        query += f"{col_name} {pg_col_type}, "
    
    for _, unique_columns in unique_constraints:
        query += f"UNIQUE ({', '.join(unique_columns)}), "
    
    return query.rstrip(', ') + ");"


def insert_data(pg_cursor: psycopg2.extensions.cursor, table_name: str, columns: List[Tuple], rows: List[Tuple], additional_columns: Dict):
    col_names = ", ".join([col[1] for col in columns] + list(additional_columns.keys()))
    placeholders = ", ".join(["%s"] * (len(columns) + len(additional_columns)))
    insert_query = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
    
    for j, row in enumerate(tqdm(rows, desc="Copy rows")):
        row = list(row)
        row = [v if v != "None" else None for v in row]
        try:
            for i, col in enumerate(columns):
                if row[i] is not None:
                    col_type = convert_type(col[2])
                    if col_type == "BOOLEAN":
                        if isinstance(row[i], bool):
                            continue
                        elif isinstance(row[i], int):
                            row[i] = bool(row[i])
                        elif row[i].isdigit():
                            row[i] = bool(int(row[i]))
                        elif row[i].lower() in ["true", "false"]:
                            row[i] = {"true": True, "false": False}[row[i].lower()]
                    elif col_type == "TEXT":
                        row[i] = str(row[i])
                    elif col_type in ["INTEGER", "BIGINT", "NUMERIC"]:
                        row[i] = int(row[i])
                    elif col_type == "DOUBLE PRECISION":
                        row[i] = float(row[i])
            
            for col_info in additional_columns.values():
                row.append(col_info["default_value"])
            
            pg_cursor.execute(insert_query, row)
            if j % 16384 == 0:
                pg_cursor.connection.commit()
        except Exception as e:
            print(f"Error inserting row: {row}")
            print(f"Error: {traceback.format_exc()}")
            pg_cursor.connection.rollback()
            exit(1)

    pg_cursor.connection.commit()


def sqlite_to_postgresql(sqlite_db_path: str, postgresql_conn_params: dict, additional_columns: dict):
    sqlite_conn, pg_conn = connect_to_databases(sqlite_db_path, postgresql_conn_params)
    sqlite_cursor = sqlite_conn.cursor()
    pg_cursor = pg_conn.cursor()

    tables = get_tables(sqlite_cursor)
    print(f"Tables for convert: {' '.join(tables)}")

    create_table_queries = []
    for table_name in tqdm(tables, desc="Tables"):
        columns = get_table_schema(sqlite_cursor, table_name)
        unique_constraints = get_unique_constraints(sqlite_cursor, table_name)
        
        create_table_query_str = create_table_query(sqlite_cursor, table_name, columns, additional_columns, unique_constraints)
        create_table_queries.append(create_table_query_str)

    print("Create table queries:")
    for query in create_table_queries:
        print(query)

    if input("Would you like to continue? (y/n): ") != "y":
        sqlite_conn.close()
        pg_conn.close()
        return

    for table_name, create_table_query_str in zip(tables, create_table_queries):
        pg_cursor.execute(create_table_query_str)

        sqlite_cursor.execute(f"SELECT * FROM {table_name}")
        rows = sqlite_cursor.fetchall()
        if rows:
            insert_data(pg_cursor, table_name, columns, rows, additional_columns)

    sqlite_conn.close()
    pg_conn.close()

def convert_type(sqlite_type: str) -> str:
    type_mapping = {
        r"[A-Z]{0,3}INT[0-9A-Z]*": "INTEGER",
        r"TEXT|STRING": "TEXT",
        r"BLOB": "BYTEA",
        r"REAL": "DOUBLE PRECISION",
        r"NUMERIC": "NUMERIC",
        r"BOOL": "BOOLEAN",
        r"FLOAT": "DOUBLE PRECISION"
    }
    
    for pattern, pg_type in type_mapping.items():
        if re.match(pattern, sqlite_type, re.IGNORECASE):
            return pg_type
    return "TEXT"


if __name__ == "__main__":
    sqlite_to_postgresql(sqlite_db_path=CONFIG["sqlite_db_path"], postgresql_conn_params=CONFIG["postgresql_conn_params"],
                         additional_columns=CONFIG["additional_columns"])
