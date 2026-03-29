from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from .db_manager import DBManager


class SQLSanityChecker:
    """SQLite-backed reference store used to sanity-check B+ Tree data state."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))

    def close(self) -> None:
        self._conn.close()

    def begin(self) -> None:
        self._conn.execute("BEGIN")

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def sync_schema_from_manager(self, manager: DBManager) -> None:
        for table_name in manager.list_tables():
            table = manager.get_table(table_name)
            if table.schema is None:
                raise ValueError(f"Table '{table_name}' requires schema for SQL sanity checks")

            columns = sorted(table.schema)
            col_defs: List[str] = []
            for col in columns:
                if col == table.primary_key:
                    col_defs.append(f'"{col}" INTEGER PRIMARY KEY')
                else:
                    col_defs.append(f'"{col}" BLOB')

            ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)})'
            self._conn.execute(ddl)

        self._conn.commit()

    def replace_table_data(self, manager: DBManager) -> None:
        for table_name in manager.list_tables():
            table = manager.get_table(table_name)
            if table.schema is None:
                raise ValueError(f"Table '{table_name}' requires schema for SQL sanity checks")

            columns = sorted(table.schema)
            quoted_cols = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join("?" for _ in columns)

            self._conn.execute(f'DELETE FROM "{table_name}"')

            for _, row in table.all_rows():
                values = [self._to_sql_value(row.get(col)) for col in columns]
                self._conn.execute(
                    f'INSERT INTO "{table_name}" ({quoted_cols}) VALUES ({placeholders})',
                    values,
                )

        self._conn.commit()

    def upsert_row(self, table_name: str, row: Dict[str, object], primary_key: str) -> None:
        columns = sorted(row.keys())
        quoted_cols = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        values = [self._to_sql_value(row.get(col)) for col in columns]

        update_cols = [c for c in columns if c != primary_key]
        if update_cols:
            assignment = ", ".join(f'"{c}"=excluded."{c}"' for c in update_cols)
            sql = (
                f'INSERT INTO "{table_name}" ({quoted_cols}) VALUES ({placeholders}) '
                f'ON CONFLICT("{primary_key}") DO UPDATE SET {assignment}'
            )
        else:
            sql = (
                f'INSERT INTO "{table_name}" ({quoted_cols}) VALUES ({placeholders}) '
                f'ON CONFLICT("{primary_key}") DO NOTHING'
            )

        self._conn.execute(sql, values)

    def delete_row(self, table_name: str, primary_key: str, key_value: object) -> None:
        self._conn.execute(
            f'DELETE FROM "{table_name}" WHERE "{primary_key}" = ?',
            (key_value,),
        )

    def compare_with_manager(self, manager: DBManager) -> Tuple[bool, List[str]]:
        mismatches: List[str] = []

        for table_name in manager.list_tables():
            table = manager.get_table(table_name)
            if table.schema is None:
                mismatches.append(f"Table '{table_name}' has no schema; cannot compare")
                continue

            columns = sorted(table.schema)
            pk = table.primary_key
            sql_rows = self._fetch_sql_rows(table_name, columns, pk)
            btree_rows = self._fetch_btree_rows(table, columns, pk)

            if sql_rows != btree_rows:
                mismatches.append(f"Mismatch in table '{table_name}'")

        return len(mismatches) == 0, mismatches

    def _fetch_sql_rows(self, table_name: str, columns: Iterable[str], pk: str) -> List[Tuple[int, Dict[str, object]]]:
        cols = list(columns)
        quoted_cols = ", ".join(f'"{c}"' for c in cols)
        rows = self._conn.execute(
            f'SELECT {quoted_cols} FROM "{table_name}" ORDER BY "{pk}" ASC'
        ).fetchall()

        result: List[Tuple[int, Dict[str, object]]] = []
        for raw in rows:
            row = {col: raw[idx] for idx, col in enumerate(cols)}
            row_key = int(row[pk])
            row = {k: self._normalize_sql_value(v) for k, v in row.items()}
            result.append((row_key, row))
        return result

    def _fetch_btree_rows(self, table, columns: Iterable[str], pk: str) -> List[Tuple[int, Dict[str, object]]]:
        cols = list(columns)
        result: List[Tuple[int, Dict[str, object]]] = []
        for key, row in table.all_rows():
            normalized = {col: row.get(col) for col in cols}
            normalized[pk] = key
            result.append((key, normalized))
        return result

    def _to_sql_value(self, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
        return str(value)

    def _normalize_sql_value(self, value: object) -> object:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value
