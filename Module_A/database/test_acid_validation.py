from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from Module_A.database.table import Table


class TestModuleAAcidValidation(unittest.TestCase):
    def setUp(self) -> None:
        self.table = Table(
            name="Member",
            primary_key="MemberID",
            schema=["MemberID", "Name", "Department", "Reputation"],
            bplustree_order=4,
        )

    def _assert_table_index_consistent(self, table: Table) -> None:
        # `all_rows` and B+ tree traversal should always represent identical data.
        self.assertEqual(table.all_rows(), table._index.get_all())

    def test_atomicity_rollback_on_failure(self) -> None:
        self.table.insert({"MemberID": 1, "Name": "Aarav", "Department": "CSE", "Reputation": 15})
        self.table.insert({"MemberID": 2, "Name": "Diya", "Department": "ECE", "Reputation": 12})
        before = self.table.all_rows()

        def failing_operation() -> None:
            self.table.update(1, {"Reputation": 18})
            raise RuntimeError("simulated mid-operation failure")

        with self.assertRaises(RuntimeError):
            self.table.execute_atomic(failing_operation)

        self.assertEqual(self.table.all_rows(), before)
        self._assert_table_index_consistent(self.table)

    def test_consistency_validation_rejects_invalid_update(self) -> None:
        self.table.insert({"MemberID": 10, "Name": "Ishaan", "Department": "ME", "Reputation": 20})
        before = self.table.get(10)

        with self.assertRaises(ValueError):
            self.table.update(10, {"unknown_column": "x"})

        self.assertEqual(self.table.get(10), before)
        self._assert_table_index_consistent(self.table)

    def test_durability_committed_data_survives_restart(self) -> None:
        self.table.insert({"MemberID": 101, "Name": "Naina", "Department": "CSE", "Reputation": 22})
        self.table.insert({"MemberID": 102, "Name": "Rohan", "Department": "EEE", "Reputation": 19})

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "member_snapshot.json"
            self.table.save_snapshot(snapshot_path)

            # Simulate process restart by recreating table from persisted snapshot.
            restarted_table = Table.load_snapshot(snapshot_path)

        self.assertEqual(
            restarted_table.get(101),
            {"MemberID": 101, "Name": "Naina", "Department": "CSE", "Reputation": 22},
        )
        self.assertEqual(
            restarted_table.get(102),
            {"MemberID": 102, "Name": "Rohan", "Department": "EEE", "Reputation": 19},
        )
        self._assert_table_index_consistent(restarted_table)

    def test_committed_state_recovered_after_crash_simulation(self) -> None:
        self.table.insert({"MemberID": 7, "Name": "Kavya", "Department": "CIVIL", "Reputation": 17})

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = Path(tmp_dir) / "commit_snapshot.json"
            self.table.save_snapshot(snapshot_path)

            # Uncommitted changes after snapshot should be lost after crash/restart.
            self.table.update(7, {"Reputation": 99})
            self.table.insert({"MemberID": 8, "Name": "Meera", "Department": "IT", "Reputation": 9})

            recovered_table = Table.load_snapshot(snapshot_path)

        self.assertEqual(
            recovered_table.get(7),
            {"MemberID": 7, "Name": "Kavya", "Department": "CIVIL", "Reputation": 17},
        )
        self.assertIsNone(recovered_table.get(8))
        self._assert_table_index_consistent(recovered_table)

    def test_fault_injection_mid_operation_rolls_back_every_change(self) -> None:
        self.table.insert({"MemberID": 21, "Name": "Priya", "Department": "CSE", "Reputation": 11})
        self.table.insert({"MemberID": 22, "Name": "Arjun", "Department": "ECE", "Reputation": 13})
        before = self.table.all_rows()

        def operation_with_fault_injection() -> None:
            self.table.update(21, {"Reputation": 99})
            self.table.insert({"MemberID": 23, "Name": "Sana", "Department": "ME", "Reputation": 15})
            # Inject failure after partial writes to verify full rollback.
            raise RuntimeError("fault injected after partial updates")

        with self.assertRaises(RuntimeError):
            self.table.execute_atomic(operation_with_fault_injection)

        self.assertEqual(self.table.all_rows(), before)
        self.assertIsNone(self.table.get(23))
        self._assert_table_index_consistent(self.table)


if __name__ == "__main__":
    unittest.main(verbosity=2)
