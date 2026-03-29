from __future__ import annotations

import json
from pathlib import Path
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

    def export_state(self) -> Dict[str, object]:
        """Export full database state across all tables."""
        tables_payload = [self._tables[name].export_state() for name in sorted(self._tables.keys())]
        return {"tables": tables_payload}

    def import_state(self, state: Dict[str, object]) -> None:
        """Replace all current tables using a previously exported state."""
        tables_data = state.get("tables", []) if isinstance(state, dict) else []
        rebuilt: Dict[str, Table] = {}

        for table_state in tables_data:
            if not isinstance(table_state, dict):
                continue

            table = Table(
                name=str(table_state["name"]),
                primary_key=str(table_state.get("primary_key", "id")),
                schema=table_state.get("schema"),
                bplustree_order=int(table_state.get("bplustree_order", 4)),
            )
            table.restore_state(table_state)
            rebuilt[table.name] = table

        self._tables = rebuilt

    def save_snapshot(self, file_path: str | Path) -> Path:
        """Persist full database state as JSON."""
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = self.export_state()
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path

    def load_snapshot(self, file_path: str | Path) -> None:
        """Load full database state from JSON into this manager."""
        path = Path(file_path)
        if not path.exists():
            return

        payload = json.loads(path.read_text(encoding="utf-8"))
        self.import_state(payload)
