from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path

from Module_A.database.db_manager import DBManager
from Module_A.database.sql_sanity import SQLSanityChecker
from Module_A.database.transaction_manager import TransactionManager


class TestModuleAMultiRelationAcid(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir_ctx = tempfile.TemporaryDirectory()
        self.tmp_dir = Path(self.tmp_dir_ctx.name)

        self.snapshot_path = self.tmp_dir / "db_snapshot.json"
        self.log_path = self.tmp_dir / "tx_log.jsonl"
        self.sql_path = self.tmp_dir / "sanity.sqlite"

        self.db = DBManager()
        self.users = self.db.create_table(
            "users",
            primary_key="user_id",
            schema=["user_id", "name", "balance", "city"],
        )
        self.products = self.db.create_table(
            "products",
            primary_key="product_id",
            schema=["product_id", "name", "stock", "price"],
        )
        self.orders = self.db.create_table(
            "orders",
            primary_key="order_id",
            schema=["order_id", "user_id", "product_id", "amount"],
        )

        self.txm = TransactionManager(self.db, self.snapshot_path, self.log_path)
        self.sql = SQLSanityChecker(self.sql_path)
        self.sql.sync_schema_from_manager(self.db)

    def tearDown(self) -> None:
        self.sql.close()
        self.tmp_dir_ctx.cleanup()

    def _assert_cross_relation_consistency(self) -> None:
        user_ids = {k for k, _ in self.users.all_rows()}
        product_ids = {k for k, _ in self.products.all_rows()}
        for _, order in self.orders.all_rows():
            self.assertIn(order["user_id"], user_ids)
            self.assertIn(order["product_id"], product_ids)

            user = self.users.get(order["user_id"])
            product = self.products.get(order["product_id"])
            self.assertIsNotNone(user)
            self.assertIsNotNone(product)
            self.assertGreaterEqual(user["balance"], 0)
            self.assertGreaterEqual(product["stock"], 0)
            self.assertGreater(order["amount"], 0)

    def _assert_sql_matches_btree(self) -> None:
        ok, mismatches = self.sql.compare_with_manager(self.db)
        self.assertTrue(ok, msg=f"SQL/B+Tree mismatch: {mismatches}")

    def _seed_reference_state(self) -> None:
        self.users.insert({"user_id": 1, "name": "Alice", "balance": 100, "city": "Delhi"})
        self.products.insert({"product_id": 10, "name": "Book", "stock": 5, "price": 20})

        self.sql.begin()
        self.sql.upsert_row(
            "users",
            {"user_id": 1, "name": "Alice", "balance": 100, "city": "Delhi"},
            primary_key="user_id",
        )
        self.sql.upsert_row(
            "products",
            {"product_id": 10, "name": "Book", "stock": 5, "price": 20},
            primary_key="product_id",
        )
        self.sql.commit()

    def test_atomicity_multi_relation_rollback_on_failure(self) -> None:
        self._seed_reference_state()
        before_state = self.db.export_state()

        self.txm.begin()
        self.sql.begin()
        try:
            self.users.update(1, {"balance": 80})
            self.products.update(10, {"stock": 4})
            self.orders.insert({"order_id": 1000, "user_id": 1, "product_id": 10, "amount": 20})

            self.sql.upsert_row(
                "users",
                {"user_id": 1, "name": "Alice", "balance": 80, "city": "Delhi"},
                primary_key="user_id",
            )
            self.sql.upsert_row(
                "products",
                {"product_id": 10, "name": "Book", "stock": 4, "price": 20},
                primary_key="product_id",
            )
            self.sql.upsert_row(
                "orders",
                {"order_id": 1000, "user_id": 1, "product_id": 10, "amount": 20},
                primary_key="order_id",
            )
            raise RuntimeError("simulated failure in multi-table tx")
        except RuntimeError:
            self.txm.rollback()
            self.sql.rollback()

        self.assertEqual(self.db.export_state(), before_state)
        self._assert_cross_relation_consistency()
        self._assert_sql_matches_btree()

    def test_consistency_constraints_after_commit(self) -> None:
        self._seed_reference_state()

        self.txm.begin()
        self.sql.begin()
        self.users.update(1, {"balance": 70})
        self.products.update(10, {"stock": 3})
        self.orders.insert({"order_id": 1001, "user_id": 1, "product_id": 10, "amount": 30})

        self.sql.upsert_row(
            "users",
            {"user_id": 1, "name": "Alice", "balance": 70, "city": "Delhi"},
            primary_key="user_id",
        )
        self.sql.upsert_row(
            "products",
            {"product_id": 10, "name": "Book", "stock": 3, "price": 20},
            primary_key="product_id",
        )
        self.sql.upsert_row(
            "orders",
            {"order_id": 1001, "user_id": 1, "product_id": 10, "amount": 30},
            primary_key="order_id",
        )

        self.txm.commit()
        self.sql.commit()
        self._assert_cross_relation_consistency()
        self._assert_sql_matches_btree()

    def test_isolation_serialized_execution(self) -> None:
        self._seed_reference_state()
        events: list[str] = []
        tx1_started = threading.Event()

        def tx1() -> None:
            self.txm.begin()
            events.append("tx1-begin")
            tx1_started.set()
            time.sleep(0.08)
            self.users.update(1, {"balance": 90})
            self.products.update(10, {"stock": 4})
            self.orders.insert({"order_id": 1002, "user_id": 1, "product_id": 10, "amount": 10})
            self.txm.commit()
            events.append("tx1-commit")

        def tx2() -> None:
            tx1_started.wait(timeout=1.0)
            try:
                self.txm.begin()
                events.append("tx2-begin")
                self.txm.rollback()
            except RuntimeError:
                events.append("tx2-blocked")

        t1 = threading.Thread(target=tx1)
        t2 = threading.Thread(target=tx2)

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertIn("tx1-begin", events)
        self.assertIn("tx1-commit", events)
        self.assertIn("tx2-blocked", events)

    def test_durability_and_recovery_across_restart(self) -> None:
        self._seed_reference_state()

        # Committed transaction should persist.
        self.txm.begin()
        self.sql.begin()
        self.users.update(1, {"balance": 60})
        self.products.update(10, {"stock": 2})
        self.orders.insert({"order_id": 1003, "user_id": 1, "product_id": 10, "amount": 40})

        self.sql.upsert_row(
            "users",
            {"user_id": 1, "name": "Alice", "balance": 60, "city": "Delhi"},
            primary_key="user_id",
        )
        self.sql.upsert_row(
            "products",
            {"product_id": 10, "name": "Book", "stock": 2, "price": 20},
            primary_key="product_id",
        )
        self.sql.upsert_row(
            "orders",
            {"order_id": 1003, "user_id": 1, "product_id": 10, "amount": 40},
            primary_key="order_id",
        )

        self.txm.commit()
        self.sql.commit()

        # Start another tx but do not commit (simulated crash).
        self.txm.begin()
        self.users.update(1, {"balance": 0})
        self.products.update(10, {"stock": 0})
        self.orders.insert({"order_id": 1004, "user_id": 1, "product_id": 10, "amount": 60})

        restarted_db = DBManager()
        restarted_txm = TransactionManager(restarted_db, self.snapshot_path, self.log_path)
        _ = restarted_txm

        r_users = restarted_db.get_table("users")
        r_products = restarted_db.get_table("products")
        r_orders = restarted_db.get_table("orders")

        self.assertEqual(r_users.get(1)["balance"], 60)
        self.assertEqual(r_products.get(10)["stock"], 2)
        self.assertIsNotNone(r_orders.get(1003))
        self.assertIsNone(r_orders.get(1004))

        ok, mismatches = self.sql.compare_with_manager(restarted_db)
        self.assertTrue(ok, msg=f"Restart SQL/B+Tree mismatch: {mismatches}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
