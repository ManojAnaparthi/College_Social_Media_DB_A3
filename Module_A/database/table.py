from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .bplustree import BPlusTree


class Table:
    """In-memory table abstraction backed by a B+ Tree primary-key index."""

    def __init__(
        self,
        name: str,
        primary_key: str = "id",
        schema: Optional[Iterable[str]] = None,
        bplustree_order: int = 4,
    ) -> None:
        if not name or not name.strip():
            raise ValueError("Table name must be a non-empty string")

        self.name = name.strip()
        self.primary_key = primary_key
        self.schema = set(schema) if schema else None
        self._index = BPlusTree(order=bplustree_order)

        if self.schema is not None and self.primary_key not in self.schema:
            self.schema.add(self.primary_key)

    def insert(self, row: Dict[str, Any]) -> None:
        """Insert a new row; raises if primary key already exists."""
        key = self._extract_and_validate_key(row)
        if self._index.search(key) is not None:
            raise KeyError(f"Duplicate primary key {key} in table '{self.name}'")

        validated = self._validate_row_shape(row)
        self._index.insert(key, deepcopy(validated))

    def upsert(self, row: Dict[str, Any]) -> None:
        """Insert or replace a row using its primary key."""
        key = self._extract_and_validate_key(row)
        validated = self._validate_row_shape(row)
        self._index.insert(key, deepcopy(validated))

    def get(self, key: int) -> Optional[Dict[str, Any]]:
        row = self._index.search(self._validate_key_type(key))
        return deepcopy(row) if row is not None else None

    def update(self, key: int, updates: Dict[str, Any]) -> bool:
        """Patch an existing row using partial updates."""
        if not isinstance(updates, dict):
            raise TypeError("updates must be a dictionary")

        key = self._validate_key_type(key)
        existing = self._index.search(key)
        if existing is None:
            return False

        if self.primary_key in updates and updates[self.primary_key] != key:
            raise ValueError("Primary key cannot be changed during update")

        merged = deepcopy(existing)
        merged.update(updates)
        merged[self.primary_key] = key
        validated = self._validate_row_shape(merged)
        return self._index.update(key, deepcopy(validated))

    def delete(self, key: int) -> bool:
        return self._index.delete(self._validate_key_type(key))

    def range_query(self, start_key: int, end_key: int) -> List[Tuple[int, Dict[str, Any]]]:
        results = self._index.range_query(self._validate_key_type(start_key), self._validate_key_type(end_key))
        return [(k, deepcopy(v)) for k, v in results]

    def all_rows(self) -> List[Tuple[int, Dict[str, Any]]]:
        rows = self._index.get_all()
        return [(k, deepcopy(v)) for k, v in rows]

    def count(self) -> int:
        return len(self._index.get_all())

    def truncate(self) -> None:
        # Reinitialize the index to clear all rows.
        self._index = BPlusTree(order=self._index.order)

    def _extract_and_validate_key(self, row: Dict[str, Any]) -> int:
        if not isinstance(row, dict):
            raise TypeError("row must be a dictionary")
        if self.primary_key not in row:
            raise KeyError(f"Row must contain primary key '{self.primary_key}'")
        return self._validate_key_type(row[self.primary_key])

    def _validate_key_type(self, key: Any) -> int:
        if not isinstance(key, int):
            raise TypeError("Primary key must be an integer for B+ Tree indexing")
        return key

    def _validate_row_shape(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if self.schema is None:
            return row

        unknown = set(row.keys()) - self.schema
        if unknown:
            raise ValueError(f"Unknown columns for table '{self.name}': {sorted(unknown)}")

        return row
