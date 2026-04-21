"""
Tests for pipeline/distribution/email_composer.py.

SMTP is mocked — tests verify: HTML rendering, score_label logic,
uncertainty flag rendering, digest DB insertion, and graceful SMTP failure.
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipeline.distribution.email_composer import compose_digest, run as run_distribution
from storage.db import insert_run, insert_news_item, insert_signal
from tests.conftest import RUN_ID, COMPOSE_RESPONSE


def _now():
    return datetime.now(timezone.utc).isoformat()


def _seed_signal(db_conn, news_id: str) -> int:
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
        "rationale": json.dumps(["DP3 drillship"]),
        "uncertainty": "low",
        "evaluated_at": _now(),
    })
    db_conn.commit()
    return sig_id


class TestComposeDigest:
    def test_returns_subject_and_body_html(self, mock_llm, evaluated_signal):
        mock_llm.return_value = COMPOSE_RESPONSE
        result = compose_digest([evaluated_signal])
        assert "subject" in result
        assert "body_html" in result
        assert isinstance(result["body_html"], str)

    def test_empty_signals_returns_no_opportunity_message(self):
        result = compose_digest([])
        assert "Sem oportunidades" in result["subject"]
        assert result["signal_ids"] == []

    def test_html_contains_opportunity_headline(self, mock_llm, evaluated_signal):
        mock_llm.return_value = COMPOSE_RESPONSE
        result = compose_digest([evaluated_signal])
        assert "DynOps" in result["body_html"]

    def test_html_contains_alta_prioridade_badge(self, mock_llm, evaluated_signal):
        mock_llm.return_value = COMPOSE_RESPONSE
        result = compose_digest([evaluated_signal])
        assert "Alta prioridade" in result["body_html"]

    def test_html_does_not_contain_uncertainty_flag_when_none(
        self, mock_llm, evaluated_signal
    ):
        response = {
            **COMPOSE_RESPONSE,
            "opportunities": [
                {**COMPOSE_RESPONSE["opportunities"][0], "uncertainty_flag": None}
            ],
        }
        mock_llm.return_value = response
        result = compose_digest([evaluated_signal])
        assert "⚠️" not in result["body_html"]

    def test_html_contains_uncertainty_flag_when_set(self, mock_llm, evaluated_signal):
        response = {
            **COMPOSE_RESPONSE,
            "opportunities": [
                {
                    **COMPOSE_RESPONSE["opportunities"][0],
                    "uncertainty_flag": "Dados inferidos — verificar com cliente",
                }
            ],
        }
        mock_llm.return_value = response
        result = compose_digest([evaluated_signal])
        assert "⚠️" in result["body_html"]

    def test_signal_ids_list_contains_db_ids(self, mock_llm, evaluated_signal):
        sig = {**evaluated_signal, "db_id": 42}
        mock_llm.return_value = COMPOSE_RESPONSE
        result = compose_digest([sig])
        assert 42 in result["signal_ids"]


class TestScoreLabelMapping:
    def _compose_with_score(self, mock_llm, evaluated_signal, score: float) -> str:
        response = {
            **COMPOSE_RESPONSE,
            "opportunities": [
                {**COMPOSE_RESPONSE["opportunities"][0], "composite_score": score}
            ],
        }
        mock_llm.return_value = response
        sig = {**evaluated_signal, "composite_score": score}
        result = compose_digest([sig])
        return result["body_html"]

    def test_score_above_70_shows_alta_prioridade(self, mock_llm, evaluated_signal):
        html = self._compose_with_score(mock_llm, evaluated_signal, 0.80)
        assert "alta" in html.lower()

    def test_score_50_to_69_shows_prioridade_media(self, mock_llm, evaluated_signal):
        response = {
            **COMPOSE_RESPONSE,
            "opportunities": [
                {**COMPOSE_RESPONSE["opportunities"][0], "score_label": "Prioridade média", "composite_score": 0.55}
            ],
        }
        mock_llm.return_value = response
        result = compose_digest([{**evaluated_signal, "composite_score": 0.55}])
        assert "media" in result["body_html"].lower() or "média" in result["body_html"]


class TestRunDistribution:
    def test_run_inserts_digest_row(self, db_conn, evaluated_signal, mock_llm):
        sig_id = _seed_signal(db_conn, evaluated_signal["id"])
        sig = {**evaluated_signal, "db_id": sig_id}
        mock_llm.return_value = COMPOSE_RESPONSE

        with patch("pipeline.distribution.email_composer._send_email"):
            result = run_distribution(db_conn, [sig], RUN_ID)

        db_conn.commit()
        row = db_conn.execute("SELECT * FROM digests WHERE run_id = ?", (RUN_ID,)).fetchone()
        assert row is not None
        assert row["recipient"] == "seller@example.com"

    def test_run_returns_true_on_success(self, db_conn, evaluated_signal, mock_llm):
        sig_id = _seed_signal(db_conn, evaluated_signal["id"])
        sig = {**evaluated_signal, "db_id": sig_id}
        mock_llm.return_value = COMPOSE_RESPONSE

        with patch("pipeline.distribution.email_composer._send_email"):
            result = run_distribution(db_conn, [sig], RUN_ID)

        assert result is True

    def test_run_returns_false_on_smtp_failure(self, db_conn, evaluated_signal, mock_llm):
        sig_id = _seed_signal(db_conn, evaluated_signal["id"])
        sig = {**evaluated_signal, "db_id": sig_id}
        mock_llm.return_value = COMPOSE_RESPONSE

        with patch(
            "pipeline.distribution.email_composer._send_email",
            side_effect=Exception("SMTP connection refused"),
        ):
            result = run_distribution(db_conn, [sig], RUN_ID)

        assert result is False

    def test_run_still_inserts_digest_on_smtp_failure(self, db_conn, evaluated_signal, mock_llm):
        """Digest record must be saved even when email sending fails."""
        sig_id = _seed_signal(db_conn, evaluated_signal["id"])
        sig = {**evaluated_signal, "db_id": sig_id}
        mock_llm.return_value = COMPOSE_RESPONSE

        with patch(
            "pipeline.distribution.email_composer._send_email",
            side_effect=Exception("SMTP error"),
        ):
            run_distribution(db_conn, [sig], RUN_ID)

        db_conn.commit()
        row = db_conn.execute("SELECT * FROM digests WHERE run_id = ?", (RUN_ID,)).fetchone()
        assert row is not None

    def test_run_returns_false_when_email_to_not_set(self, db_conn, evaluated_signal, mock_llm, monkeypatch):
        monkeypatch.delenv("EMAIL_TO", raising=False)
        result = run_distribution(db_conn, [evaluated_signal], RUN_ID)
        assert result is False
