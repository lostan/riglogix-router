"""
Tests for storage/db.py — schema integrity, insert helpers, queries.
All tests use an in-memory-equivalent temp SQLite (via db_conn fixture).
"""

import json
from datetime import datetime, timezone

import pytest

from storage.db import (
    insert_news_item,
    insert_structured_news,
    insert_enriched_news,
    insert_signal,
    insert_run,
    update_run,
    insert_digest,
    insert_feedback,
    get_unprocessed_news,
    get_top_signals,
)
from tests.conftest import RUN_ID


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── news_items ───────────────────────────────────────────────────────────────

class TestNewsItems:
    def test_insert_and_retrieve(self, db_conn, raw_news_item):
        insert_news_item(db_conn, raw_news_item)
        db_conn.commit()
        row = db_conn.execute(
            "SELECT * FROM news_items WHERE id = ?", (raw_news_item["id"],)
        ).fetchone()
        assert row is not None
        assert row["title"] == raw_news_item["title"]
        assert row["run_id"] == RUN_ID

    def test_insert_or_ignore_on_duplicate(self, db_conn, raw_news_item):
        insert_news_item(db_conn, raw_news_item)
        insert_news_item(db_conn, raw_news_item)  # should not raise
        db_conn.commit()
        count = db_conn.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]
        assert count == 1

    def test_get_unprocessed_news_returns_unstructured(self, db_conn, raw_news_item):
        insert_news_item(db_conn, raw_news_item)
        db_conn.commit()
        rows = get_unprocessed_news(db_conn, RUN_ID)
        assert len(rows) == 1
        assert rows[0]["id"] == raw_news_item["id"]

    def test_get_unprocessed_news_excludes_already_structured(
        self, db_conn, raw_news_item
    ):
        insert_news_item(db_conn, raw_news_item)
        insert_structured_news(
            db_conn,
            {
                "news_id": raw_news_item["id"],
                "client": "Petrobras",
                "geography": None,
                "operation_type": None,
                "wells": "[]",
                "asset": None,
                "phase": None,
                "timing_raw": None,
                "environment": None,
                "depth_m": None,
                "contractor": None,
                "history_notes": None,
                "structured_at": _now(),
            },
        )
        db_conn.commit()
        rows = get_unprocessed_news(db_conn, RUN_ID)
        assert rows == []

    def test_get_unprocessed_news_only_current_run(self, db_conn, raw_news_item):
        insert_news_item(db_conn, raw_news_item)
        other_item = {**raw_news_item, "id": "other0000000000", "run_id": "other_run"}
        insert_news_item(db_conn, other_item)
        db_conn.commit()
        rows = get_unprocessed_news(db_conn, RUN_ID)
        assert len(rows) == 1
        assert rows[0]["id"] == raw_news_item["id"]


# ── structured_news ──────────────────────────────────────────────────────────

class TestStructuredNews:
    def test_insert_structured(self, db_conn, raw_news_item):
        insert_news_item(db_conn, raw_news_item)
        insert_structured_news(
            db_conn,
            {
                "news_id": raw_news_item["id"],
                "client": "Petrobras",
                "geography": "Brazil",
                "operation_type": "drilling",
                "wells": json.dumps(["W-1", "W-2"]),
                "asset": "Buzios",
                "phase": "development",
                "timing_raw": "Q3 2025",
                "environment": "ultra-deepwater",
                "depth_m": 2000.0,
                "contractor": "Transocean",
                "history_notes": "DP3",
                "structured_at": _now(),
            },
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT * FROM structured_news WHERE news_id = ?", (raw_news_item["id"],)
        ).fetchone()
        assert row["client"] == "Petrobras"
        assert row["depth_m"] == 2000.0

    def test_insert_or_replace_structured(self, db_conn, raw_news_item):
        insert_news_item(db_conn, raw_news_item)
        base = {
            "news_id": raw_news_item["id"],
            "client": "Old Client",
            "geography": None,
            "operation_type": None,
            "wells": "[]",
            "asset": None,
            "phase": None,
            "timing_raw": None,
            "environment": None,
            "depth_m": None,
            "contractor": None,
            "history_notes": None,
            "structured_at": _now(),
        }
        insert_structured_news(db_conn, base)
        insert_structured_news(db_conn, {**base, "client": "New Client"})
        db_conn.commit()
        row = db_conn.execute(
            "SELECT client FROM structured_news WHERE news_id = ?",
            (raw_news_item["id"],),
        ).fetchone()
        assert row["client"] == "New Client"


# ── product_signals + get_top_signals ────────────────────────────────────────

class TestProductSignals:
    def _seed_run_and_news(self, db_conn, raw_news_item):
        insert_run(db_conn, RUN_ID, _now())
        insert_news_item(db_conn, raw_news_item)
        db_conn.commit()

    def _make_signal(self, news_id: str, product: str, score: float) -> dict:
        return {
            "news_id": news_id,
            "run_id": RUN_ID,
            "product": product,
            "technical_fit": score,
            "timing_fit": score,
            "commercial_priority": score,
            "composite_score": score,
            "rationale": json.dumps(["test rationale"]),
            "uncertainty": "low",
            "evaluated_at": _now(),
        }

    def test_insert_signal_returns_id(self, db_conn, raw_news_item):
        self._seed_run_and_news(db_conn, raw_news_item)
        sig_id = insert_signal(
            db_conn,
            self._make_signal(raw_news_item["id"], "DynOps", 0.85),
        )
        db_conn.commit()
        assert isinstance(sig_id, int)
        assert sig_id > 0

    def test_get_top_signals_filters_by_min_score(self, db_conn, raw_news_item):
        self._seed_run_and_news(db_conn, raw_news_item)
        insert_signal(db_conn, self._make_signal(raw_news_item["id"], "DynOps", 0.80))
        insert_signal(db_conn, self._make_signal(raw_news_item["id"], "SWIM", 0.20))
        db_conn.commit()
        rows = get_top_signals(db_conn, RUN_ID, min_score=0.5, limit=10)
        products = [r["product"] for r in rows]
        assert "DynOps" in products
        assert "SWIM" not in products

    def test_get_top_signals_ordered_by_score(self, db_conn, raw_news_item):
        self._seed_run_and_news(db_conn, raw_news_item)
        insert_signal(db_conn, self._make_signal(raw_news_item["id"], "SWIM", 0.60))
        insert_signal(db_conn, self._make_signal(raw_news_item["id"], "DynOps", 0.90))
        insert_signal(db_conn, self._make_signal(raw_news_item["id"], "Riser Analysis", 0.75))
        db_conn.commit()
        rows = get_top_signals(db_conn, RUN_ID, min_score=0.0, limit=10)
        scores = [r["composite_score"] for r in rows]
        assert scores == sorted(scores, reverse=True)

    def test_get_top_signals_respects_limit(self, db_conn, raw_news_item):
        self._seed_run_and_news(db_conn, raw_news_item)
        for product in ["DynOps", "SWIM", "Riser Analysis", "Conductor Analysis"]:
            insert_signal(db_conn, self._make_signal(raw_news_item["id"], product, 0.70))
        db_conn.commit()
        rows = get_top_signals(db_conn, RUN_ID, min_score=0.0, limit=2)
        assert len(rows) == 2


# ── opportunity_runs ─────────────────────────────────────────────────────────

class TestOpportunityRuns:
    def test_insert_and_update_run(self, db_conn):
        insert_run(db_conn, RUN_ID, _now())
        db_conn.commit()
        row = db_conn.execute(
            "SELECT status FROM opportunity_runs WHERE run_id = ?", (RUN_ID,)
        ).fetchone()
        assert row["status"] == "running"

        update_run(db_conn, RUN_ID, status="success", news_fetched=5)
        db_conn.commit()
        row = db_conn.execute(
            "SELECT status, news_fetched FROM opportunity_runs WHERE run_id = ?",
            (RUN_ID,),
        ).fetchone()
        assert row["status"] == "success"
        assert row["news_fetched"] == 5


# ── digests + feedback ───────────────────────────────────────────────────────

class TestDigestsAndFeedback:
    def _seed(self, db_conn, raw_news_item):
        insert_run(db_conn, RUN_ID, _now())
        insert_news_item(db_conn, raw_news_item)
        sig_id = insert_signal(
            db_conn,
            {
                "news_id": raw_news_item["id"],
                "run_id": RUN_ID,
                "product": "DynOps",
                "technical_fit": 0.8,
                "timing_fit": 0.8,
                "commercial_priority": 0.8,
                "composite_score": 0.8,
                "rationale": "[]",
                "uncertainty": "low",
                "evaluated_at": _now(),
            },
        )
        db_conn.commit()
        return sig_id

    def test_insert_digest(self, db_conn, raw_news_item):
        self._seed(db_conn, raw_news_item)
        digest_id = insert_digest(
            db_conn,
            {
                "run_id": RUN_ID,
                "sent_at": _now(),
                "recipient": "seller@example.com",
                "subject": "Test digest",
                "body_html": "<p>Test</p>",
                "signal_ids": json.dumps([1]),
            },
        )
        db_conn.commit()
        assert isinstance(digest_id, int)

    def test_insert_feedback_links_to_signal(self, db_conn, raw_news_item):
        sig_id = self._seed(db_conn, raw_news_item)
        digest_id = insert_digest(
            db_conn,
            {
                "run_id": RUN_ID,
                "sent_at": _now(),
                "recipient": "seller@example.com",
                "subject": "Test",
                "body_html": "<p>Test</p>",
                "signal_ids": json.dumps([sig_id]),
            },
        )
        db_conn.commit()
        insert_feedback(
            db_conn,
            {
                "signal_id": sig_id,
                "digest_id": digest_id,
                "rating": 4,
                "comment": "bom fit",
                "received_at": _now(),
            },
        )
        db_conn.commit()
        row = db_conn.execute(
            "SELECT rating, comment FROM feedback WHERE signal_id = ?", (sig_id,)
        ).fetchone()
        assert row["rating"] == 4
        assert row["comment"] == "bom fit"
