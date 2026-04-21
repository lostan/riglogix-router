"""
Tests for pipeline/feedback/feedback_handler.py.

Covers: regex parsing of feedback text, edge cases, rank-to-signal mapping,
ingest_from_text DB writes, and unknown rank skipping.
"""

import json
from datetime import datetime, timezone

import pytest

from pipeline.feedback.feedback_handler import parse_feedback_text, ingest_feedback
from storage.db import insert_run, insert_news_item, insert_signal, insert_digest
from tests.conftest import RUN_ID


def _now():
    return datetime.now(timezone.utc).isoformat()


# ── parse_feedback_text ──────────────────────────────────────────────────────

class TestParseFeedbackText:
    def test_single_line_with_comment(self):
        result = parse_feedback_text("#1: 4 — bom fit, cliente em freeze")
        assert len(result) == 1
        assert result[0] == {"rank": 1, "rating": 4, "comment": "bom fit, cliente em freeze"}

    def test_single_line_without_comment(self):
        result = parse_feedback_text("#2: 3")
        assert len(result) == 1
        assert result[0]["rank"] == 2
        assert result[0]["rating"] == 3
        assert result[0]["comment"] is None

    def test_multiple_lines(self):
        text = "#1: 5 — excelente\n#2: 2\n#3: 4 — relevante"
        result = parse_feedback_text(text)
        assert len(result) == 3
        assert result[0]["rank"] == 1
        assert result[1]["rank"] == 2
        assert result[2]["rank"] == 3

    def test_blank_lines_ignored(self):
        text = "\n\n#1: 4\n\n#2: 3\n\n"
        result = parse_feedback_text(text)
        assert len(result) == 2

    def test_non_feedback_lines_ignored(self):
        text = "Obrigado pelo digest!\n#1: 4 — ok\nAté amanhã"
        result = parse_feedback_text(text)
        assert len(result) == 1

    def test_em_dash_separator_variants(self):
        """Accept —, -, –, and similar dashes as comment separator."""
        for dash in ["—", "-", "–"]:
            result = parse_feedback_text(f"#1: 4 {dash} comentário")
            assert len(result) == 1
            assert result[0]["comment"] == "comentário"

    def test_rating_must_be_1_to_5(self):
        result = parse_feedback_text("#1: 6")
        assert len(result) == 0

    def test_leading_hash_required(self):
        result = parse_feedback_text("1: 4 — sem hash")
        assert len(result) == 0

    def test_empty_text_returns_empty_list(self):
        assert parse_feedback_text("") == []
        assert parse_feedback_text("   \n  \n ") == []

    def test_rating_1_accepted(self):
        result = parse_feedback_text("#1: 1")
        assert result[0]["rating"] == 1

    def test_rating_5_accepted(self):
        result = parse_feedback_text("#3: 5")
        assert result[0]["rating"] == 5


# ── ingest_feedback ───────────────────────────────────────────────────────────

def _seed_signal(db_conn, news_id: str) -> tuple[int, int]:
    """Returns (signal_id, digest_id)."""
    insert_run(db_conn, RUN_ID, _now())
    insert_news_item(db_conn, {
        "id": news_id,
        "source": "daily_logix",
        "title": "Test",
        "body": "body",
        "source_url": "https://x.com",
        "published_at": None,
        "fetched_at": _now(),
        "run_id": RUN_ID,
    })
    sig_id = insert_signal(db_conn, {
        "news_id": news_id,
        "run_id": RUN_ID,
        "product": "DynOps",
        "technical_fit": 0.85,
        "timing_fit": 0.80,
        "commercial_priority": 0.75,
        "composite_score": 0.807,
        "rationale": json.dumps(["test"]),
        "uncertainty": "low",
        "evaluated_at": _now(),
    })
    digest_id = insert_digest(db_conn, {
        "run_id": RUN_ID,
        "sent_at": _now(),
        "recipient": "seller@x.com",
        "subject": "Test digest",
        "body_html": "<p>Test</p>",
        "signal_ids": json.dumps([sig_id]),
    })
    db_conn.commit()
    return sig_id, digest_id


class TestIngestFeedback:
    def test_feedback_persisted_to_db(self, db_conn, raw_news_item):
        sig_id, digest_id = _seed_signal(db_conn, raw_news_item["id"])
        ranked_signal = {**raw_news_item, "db_id": sig_id}

        ingest_feedback(
            db_conn,
            [{"rank": 1, "rating": 5, "comment": "perfeito"}],
            digest_id=digest_id,
            ranked_signals=[ranked_signal],
        )
        db_conn.commit()

        row = db_conn.execute(
            "SELECT * FROM feedback WHERE signal_id = ?", (sig_id,)
        ).fetchone()
        assert row is not None
        assert row["rating"] == 5
        assert row["comment"] == "perfeito"

    def test_unknown_rank_skipped(self, db_conn, raw_news_item):
        sig_id, digest_id = _seed_signal(db_conn, raw_news_item["id"])
        ranked_signal = {**raw_news_item, "db_id": sig_id}

        ingest_feedback(
            db_conn,
            [{"rank": 99, "rating": 3, "comment": None}],
            digest_id=digest_id,
            ranked_signals=[ranked_signal],
        )
        db_conn.commit()

        count = db_conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]
        assert count == 0

    def test_signal_without_db_id_skipped(self, db_conn, raw_news_item):
        _, digest_id = _seed_signal(db_conn, raw_news_item["id"])
        ranked_signal = {**raw_news_item}  # no db_id

        ingest_feedback(
            db_conn,
            [{"rank": 1, "rating": 4, "comment": None}],
            digest_id=digest_id,
            ranked_signals=[ranked_signal],
        )
        db_conn.commit()

        count = db_conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]
        assert count == 0

    def test_multiple_feedback_items(self, db_conn, raw_news_item):
        sig_id, digest_id = _seed_signal(db_conn, raw_news_item["id"])

        # Second signal for rank 2
        news_id_2 = "news2000000000000"
        insert_news_item(db_conn, {
            "id": news_id_2, "source": "daily_logix", "title": "T2",
            "body": "b", "source_url": "https://x.com/2", "published_at": None,
            "fetched_at": _now(), "run_id": RUN_ID,
        })
        sig_id_2 = insert_signal(db_conn, {
            "news_id": news_id_2, "run_id": RUN_ID, "product": "SWIM",
            "technical_fit": 0.6, "timing_fit": 0.6, "commercial_priority": 0.6,
            "composite_score": 0.6, "rationale": "[]", "uncertainty": "medium",
            "evaluated_at": _now(),
        })
        db_conn.commit()

        ranked = [
            {**raw_news_item, "db_id": sig_id},
            {"id": news_id_2, "db_id": sig_id_2},
        ]
        ingest_feedback(
            db_conn,
            [
                {"rank": 1, "rating": 4, "comment": "ok"},
                {"rank": 2, "rating": 2, "comment": "não relevante"},
            ],
            digest_id=digest_id,
            ranked_signals=ranked,
        )
        db_conn.commit()

        count = db_conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]
        assert count == 2
