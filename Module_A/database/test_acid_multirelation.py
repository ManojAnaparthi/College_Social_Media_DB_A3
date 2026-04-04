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
        self.members = self.db.create_table(
            "Member",
            primary_key="MemberID",
            schema=["MemberID", "Name", "Department", "Reputation"],
        )
        self.posts = self.db.create_table(
            "Post",
            primary_key="PostID",
            schema=["PostID", "MemberID", "Content", "LikeCount"],
        )
        self.comments = self.db.create_table(
            "Comment",
            primary_key="CommentID",
            schema=["CommentID", "PostID", "MemberID", "Content", "LikeCount"],
        )

        self.txm = TransactionManager(self.db, self.snapshot_path, self.log_path)
        self.sql = SQLSanityChecker(self.sql_path)
        self.sql.sync_schema_from_manager(self.db)

    def tearDown(self) -> None:
        self.sql.close()
        self.tmp_dir_ctx.cleanup()

    def _assert_cross_relation_consistency(self) -> None:
        member_ids = {k for k, _ in self.members.all_rows()}
        post_ids = {k for k, _ in self.posts.all_rows()}
        for _, comment in self.comments.all_rows():
            self.assertIn(comment["MemberID"], member_ids)
            self.assertIn(comment["PostID"], post_ids)

            member = self.members.get(comment["MemberID"])
            post = self.posts.get(comment["PostID"])
            self.assertIsNotNone(member)
            self.assertIsNotNone(post)
            self.assertGreaterEqual(member["Reputation"], 0)
            self.assertGreaterEqual(post["LikeCount"], 0)
            self.assertGreaterEqual(comment["LikeCount"], 0)
            self.assertTrue(str(post["Content"]).strip())
            self.assertTrue(str(comment["Content"]).strip())

    def _assert_sql_matches_btree(self) -> None:
        ok, mismatches = self.sql.compare_with_manager(self.db)
        self.assertTrue(ok, msg=f"SQL/B+Tree mismatch: {mismatches}")

    def _seed_reference_state(self) -> None:
        self.members.insert({"MemberID": 1, "Name": "Aarav", "Department": "CSE", "Reputation": 100})
        self.posts.insert({"PostID": 10, "MemberID": 1, "Content": "Welcome to campus!", "LikeCount": 5})

        self.sql.begin()
        self.sql.upsert_row(
            "Member",
            {"MemberID": 1, "Name": "Aarav", "Department": "CSE", "Reputation": 100},
            primary_key="MemberID",
        )
        self.sql.upsert_row(
            "Post",
            {"PostID": 10, "MemberID": 1, "Content": "Welcome to campus!", "LikeCount": 5},
            primary_key="PostID",
        )
        self.sql.commit()

    def test_atomicity_multi_relation_rollback_on_failure(self) -> None:
        self._seed_reference_state()
        before_state = self.db.export_state()

        self.txm.begin()
        self.sql.begin()
        try:
            self.members.update(1, {"Reputation": 80})
            self.posts.update(10, {"LikeCount": 4})
            self.comments.insert(
                {"CommentID": 1000, "PostID": 10, "MemberID": 1, "Content": "Nice post!", "LikeCount": 2}
            )

            self.sql.upsert_row(
                "Member",
                {"MemberID": 1, "Name": "Aarav", "Department": "CSE", "Reputation": 80},
                primary_key="MemberID",
            )
            self.sql.upsert_row(
                "Post",
                {"PostID": 10, "MemberID": 1, "Content": "Welcome to campus!", "LikeCount": 4},
                primary_key="PostID",
            )
            self.sql.upsert_row(
                "Comment",
                {"CommentID": 1000, "PostID": 10, "MemberID": 1, "Content": "Nice post!", "LikeCount": 2},
                primary_key="CommentID",
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
        self.members.update(1, {"Reputation": 70})
        self.posts.update(10, {"LikeCount": 8})
        self.comments.insert(
            {"CommentID": 1001, "PostID": 10, "MemberID": 1, "Content": "Campus life is great", "LikeCount": 3}
        )

        self.sql.upsert_row(
            "Member",
            {"MemberID": 1, "Name": "Aarav", "Department": "CSE", "Reputation": 70},
            primary_key="MemberID",
        )
        self.sql.upsert_row(
            "Post",
            {"PostID": 10, "MemberID": 1, "Content": "Welcome to campus!", "LikeCount": 8},
            primary_key="PostID",
        )
        self.sql.upsert_row(
            "Comment",
            {
                "CommentID": 1001,
                "PostID": 10,
                "MemberID": 1,
                "Content": "Campus life is great",
                "LikeCount": 3,
            },
            primary_key="CommentID",
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
            self.members.update(1, {"Reputation": 90})
            self.posts.update(10, {"LikeCount": 9})
            self.comments.insert(
                {"CommentID": 1002, "PostID": 10, "MemberID": 1, "Content": "See you all", "LikeCount": 1}
            )
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
        self.members.update(1, {"Reputation": 60})
        self.posts.update(10, {"LikeCount": 12})
        self.comments.insert(
            {"CommentID": 1003, "PostID": 10, "MemberID": 1, "Content": "Mid-sem update", "LikeCount": 4}
        )

        self.sql.upsert_row(
            "Member",
            {"MemberID": 1, "Name": "Aarav", "Department": "CSE", "Reputation": 60},
            primary_key="MemberID",
        )
        self.sql.upsert_row(
            "Post",
            {"PostID": 10, "MemberID": 1, "Content": "Welcome to campus!", "LikeCount": 12},
            primary_key="PostID",
        )
        self.sql.upsert_row(
            "Comment",
            {"CommentID": 1003, "PostID": 10, "MemberID": 1, "Content": "Mid-sem update", "LikeCount": 4},
            primary_key="CommentID",
        )

        self.txm.commit()
        self.sql.commit()

        # Start another tx but do not commit (simulated crash).
        self.txm.begin()
        self.members.update(1, {"Reputation": 0})
        self.posts.update(10, {"LikeCount": 0})
        self.comments.insert(
            {"CommentID": 1004, "PostID": 10, "MemberID": 1, "Content": "Uncommitted draft", "LikeCount": 0}
        )

        restarted_db = DBManager()
        restarted_txm = TransactionManager(restarted_db, self.snapshot_path, self.log_path)
        _ = restarted_txm

        r_members = restarted_db.get_table("Member")
        r_posts = restarted_db.get_table("Post")
        r_comments = restarted_db.get_table("Comment")

        self.assertEqual(r_members.get(1)["Reputation"], 60)
        self.assertEqual(r_posts.get(10)["LikeCount"], 12)
        self.assertIsNotNone(r_comments.get(1003))
        self.assertIsNone(r_comments.get(1004))

        ok, mismatches = self.sql.compare_with_manager(restarted_db)
        self.assertTrue(ok, msg=f"Restart SQL/B+Tree mismatch: {mismatches}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
