from __future__ import annotations

import json
import threading
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional

from .db_manager import DBManager


class TransactionManager:
    """
    Multi-relation transaction coordinator for DBManager.

    Isolation is implemented as serialized execution: at most one active
    write transaction at a time.
    """

    def __init__(self, db: DBManager, snapshot_path: str | Path, log_path: str | Path) -> None:
        self._db = db
        self._snapshot_path = Path(snapshot_path)
        self._log_path = Path(log_path)

        self._lock = threading.RLock()
        self._active_tx_id: Optional[str] = None
        self._pre_tx_state: Optional[Dict[str, Any]] = None

        self._snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        if not self._log_path.exists():
            self._log_path.write_text("", encoding="utf-8")

        self.recover()

    def begin(self) -> str:
        with self._lock:
            if self._active_tx_id is not None:
                raise RuntimeError("Another transaction is already active")

            tx_id = uuid.uuid4().hex
            self._pre_tx_state = deepcopy(self._db.export_state())
            self._active_tx_id = tx_id
            self._append_log({"event": "BEGIN", "tx_id": tx_id})
            return tx_id

    def commit(self) -> None:
        with self._lock:
            tx_id = self._require_active_tx()
            self._db.save_snapshot(self._snapshot_path)
            self._append_log({"event": "COMMIT", "tx_id": tx_id})
            self._active_tx_id = None
            self._pre_tx_state = None

    def rollback(self) -> None:
        with self._lock:
            tx_id = self._require_active_tx()
            if self._pre_tx_state is None:
                raise RuntimeError("No transaction snapshot found for rollback")

            self._db.import_state(self._pre_tx_state)
            self._append_log({"event": "ROLLBACK", "tx_id": tx_id})
            self._active_tx_id = None
            self._pre_tx_state = None

    def recover(self) -> None:
        """
        Recover database state after crash/restart.

        Recovery policy:
        - Load latest committed snapshot if present.
        - Ignore incomplete in-memory transactions that lacked COMMIT.
        """
        with self._lock:
            self._db.load_snapshot(self._snapshot_path)
            self._active_tx_id = None
            self._pre_tx_state = None

    @property
    def has_active_transaction(self) -> bool:
        return self._active_tx_id is not None

    def _require_active_tx(self) -> str:
        if self._active_tx_id is None:
            raise RuntimeError("No active transaction")
        return self._active_tx_id

    def _append_log(self, record: Dict[str, Any]) -> None:
        with self._log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
