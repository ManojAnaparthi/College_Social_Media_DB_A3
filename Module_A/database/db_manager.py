from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from .table import Table


class DBManager:
    """Simple in-memory manager for multiple Table instances."""

    def __init__(self) -> None:
        self._tables: Dict[str, Table] = {}

    def create_table(
        self,
        name: str,
        primary_key: str = "id",
        schema: Optional[Iterable[str]] = None,
        bplustree_order: int = 4,
        if_not_exists: bool = False,
    ) -> Table:
        table_name = self._normalize_name(name)

        if table_name in self._tables:
            if if_not_exists:
                return self._tables[table_name]
            raise KeyError(f"Table '{table_name}' already exists")

        table = Table(
            name=table_name,
            primary_key=primary_key,
            schema=schema,
            bplustree_order=bplustree_order,
        )
        self._tables[table_name] = table
        return table

    def get_table(self, name: str) -> Table:
        table_name = self._normalize_name(name)
        if table_name not in self._tables:
            raise KeyError(f"Table '{table_name}' does not exist")
        return self._tables[table_name]

    def drop_table(self, name: str, if_exists: bool = False) -> bool:
        table_name = self._normalize_name(name)

        if table_name not in self._tables:
            if if_exists:
                return False
            raise KeyError(f"Table '{table_name}' does not exist")

        del self._tables[table_name]
        return True

    def list_tables(self) -> List[str]:
        return sorted(self._tables.keys())

    def has_table(self, name: str) -> bool:
        return self._normalize_name(name) in self._tables

    def _normalize_name(self, name: str) -> str:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Table name must be a non-empty string")
        return name.strip()
