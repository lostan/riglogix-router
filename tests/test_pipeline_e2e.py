"""
End-to-end offline pipeline test.

Runs the complete pipeline from ingestion through distribution using:
  - Local fixture JSON (no HTTP calls)
  - Mocked LLM (no Anthropic API calls)
  - Mocked SMTP (no email sent)
  - Temporary SQLite DB (auto-deleted)

Asserts the full DB state after a successful run:
  - news_items populated
  - structured_news populated
  - enriched_news populated
  - product_signals populated with correct scores
  - opportunity_runs marked success
  - digests row inserted
"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from tests.conftest import (
    STRUCTURE_RESPONSE,
    ENRICH_RESPONSE,
    CLASSIFY_RESPONSE,
    TIMING_RESPONSE,
    COMPOSE_RESPONSE,
)

FIXTURES_PATH = str(Path(__file__).parent / "fixtures")
PRODUCTS_COUNT = 5  # SWIM, DynOps, Conductor Analysis, Riser Analysis, DP Feasibility Study
NEWS_COUNT = 5      # sample_news.json has 5 articles


# ── LLM router: return correct response per prompt name ─────────────────────

def _llm_router(prompt_name: str, user_message: str, **kwargs):
    mapping = {
        "structure_news": {**STRUCTURE_RESPONSE},
        "enrich_news": {**ENRICH_RESPONSE},
        "classify_product": list(CLASSIFY_RESPONSE),
        "evaluate_timing": {**TIMING_RESPONSE},
        "compose_email": {**COMPOSE_RESPONSE},
    }
    response = mapping.get(prompt_name)
    if response is None:
        raise ValueError(f"Unexpected prompt_name in e2e test: {prompt_name}")
    return response


# ── fixtures settings override ───────────────────────────────────────────────

OFFLINE_SETTINGS = {
    "app": {"log_level": "WARNING"},
    "llm": {"model": "claude-sonnet-4-6", "max_tokens": 4096, "cache_system_prompt": True},
    "ingestion": {
        "source": "daily_logix",
        "max_news_per_run": 15,
        "use_fixtures": True,
        "fixtures_path": FIXTURES_PATH,
    },
    "storage": {"db_path": "REPLACED_BY_TMP"},
    "pipeline": {
        "min_opportunity_score": 0.0,  # include everything for assertions
        "max_opportunities_in_digest": 5,
    },
    "email": {
        "subject_template": "RigLogix Router — {date} | {count} oportunidade(s)",
        "send_time": "07:00",
    },
    "scheduler": {"enabled": False, "cron": {}},
}


# ── helpers ──────────────────────────────────────────────────────────────────

def _run_pipeline(tmp_db_path, monkeypatch):
    """
    Execute the full pipeline once in offline mode.
    Returns the run_id used.
    """
    import storage.db as db_module
    monkeypatch.setattr(db_module, "_DB_PATH", tmp_db_path)

    settings_override = {
        **OFFLINE_SETTINGS,
        "storage": {"db_path": str(tmp_db_path)},
    }

    with patch("llm.client.complete", side_effect=_llm_router), \
         patch("pipeline.distribution.email_composer._send_email"), \
         patch("pipeline.ingestion.daily_logix_scraper.settings", settings_override), \
         patch("pipeline.routing.opportunity_router.settings", settings_override), \
         patch("pipeline.distribution.email_composer.settings", settings_override), \
         patch("config.settings", settings_override):

        from main import cmd_run
        # cmd_run uses a fresh run_id each time — capture it via DB
        cmd_run(dry_run=False)

    import sqlite3
    conn = sqlite3.connect(tmp_db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ── tests ────────────────────────────────────────────────────────────────────

class TestPipelineE2E:
    @pytest.fixture()
    def pipeline_conn(self, tmp_db_path, monkeypatch):
        conn = _run_pipeline(tmp_db_path, monkeypatch)
        yield conn
        conn.close()

    # ── run metadata ─────────────────────────────────────────────────────────

    def test_run_completed_with_success_status(self, pipeline_conn):
        row = pipeline_conn.execute(
            "SELECT status FROM opportunity_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        assert row["status"] == "success"

    def test_run_reports_correct_news_fetched(self, pipeline_conn):
        row = pipeline_conn.execute(
            "SELECT news_fetched FROM opportunity_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        assert row["news_fetched"] == NEWS_COUNT

    def test_run_reports_correct_news_processed(self, pipeline_conn):
        row = pipeline_conn.execute(
            "SELECT news_processed FROM opportunity_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        assert row["news_processed"] == NEWS_COUNT

    def test_digest_sent_flag_is_set(self, pipeline_conn):
        row = pipeline_conn.execute(
            "SELECT digest_sent FROM opportunity_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        assert row["digest_sent"] == 1

    # ── news_items ────────────────────────────────────────────────────────────

    def test_all_news_items_inserted(self, pipeline_conn):
        count = pipeline_conn.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]
        assert count == NEWS_COUNT

    def test_news_items_have_required_fields(self, pipeline_conn):
        rows = pipeline_conn.execute("SELECT * FROM news_items").fetchall()
        for row in rows:
            assert row["id"] is not None
            assert row["title"] is not None
            assert len(row["title"]) > 0

    # ── structured_news ───────────────────────────────────────────────────────

    def test_all_news_structured(self, pipeline_conn):
        count = pipeline_conn.execute("SELECT COUNT(*) FROM structured_news").fetchone()[0]
        assert count == NEWS_COUNT

    def test_structured_client_populated(self, pipeline_conn):
        rows = pipeline_conn.execute("SELECT client FROM structured_news").fetchall()
        clients = [r["client"] for r in rows if r["client"]]
        assert len(clients) == NEWS_COUNT

    # ── enriched_news ─────────────────────────────────────────────────────────

    def test_all_news_enriched(self, pipeline_conn):
        count = pipeline_conn.execute("SELECT COUNT(*) FROM enriched_news").fetchone()[0]
        assert count == NEWS_COUNT

    def test_enriched_uncertainty_is_valid(self, pipeline_conn):
        rows = pipeline_conn.execute("SELECT uncertainty FROM enriched_news").fetchall()
        for row in rows:
            assert row["uncertainty"] in ("low", "medium", "high")

    # ── product_signals ───────────────────────────────────────────────────────

    def test_signals_created_for_every_news_x_product(self, pipeline_conn):
        count = pipeline_conn.execute("SELECT COUNT(*) FROM product_signals").fetchone()[0]
        assert count == NEWS_COUNT * PRODUCTS_COUNT

    def test_all_products_represented(self, pipeline_conn):
        products = {
            r[0] for r in pipeline_conn.execute(
                "SELECT DISTINCT product FROM product_signals"
            ).fetchall()
        }
        assert products == {
            "SWIM", "DynOps", "Conductor Analysis", "Riser Analysis", "DP Feasibility Study"
        }

    def test_composite_scores_in_valid_range(self, pipeline_conn):
        rows = pipeline_conn.execute("SELECT composite_score FROM product_signals").fetchall()
        for row in rows:
            assert 0.0 <= row["composite_score"] <= 1.0

    def test_composite_score_formula_applied(self, pipeline_conn):
        """Spot-check that composite = 0.4*tf + 0.35*tif + 0.25*cp."""
        rows = pipeline_conn.execute(
            "SELECT technical_fit, timing_fit, commercial_priority, composite_score FROM product_signals"
        ).fetchall()
        for row in rows:
            expected = round(
                0.4 * row["technical_fit"]
                + 0.35 * row["timing_fit"]
                + 0.25 * row["commercial_priority"],
                3,
            )
            assert abs(row["composite_score"] - expected) < 0.001

    # ── digests ───────────────────────────────────────────────────────────────

    def test_one_digest_inserted(self, pipeline_conn):
        count = pipeline_conn.execute("SELECT COUNT(*) FROM digests").fetchone()[0]
        assert count == 1

    def test_digest_has_html_body(self, pipeline_conn):
        row = pipeline_conn.execute("SELECT body_html FROM digests").fetchone()
        assert "<html" in row["body_html"].lower()

    def test_digest_recipient_matches_env(self, pipeline_conn):
        row = pipeline_conn.execute("SELECT recipient FROM digests").fetchone()
        assert row["recipient"] == "seller@example.com"

    def test_digest_signal_ids_json_array(self, pipeline_conn):
        row = pipeline_conn.execute("SELECT signal_ids FROM digests").fetchone()
        ids = json.loads(row["signal_ids"])
        assert isinstance(ids, list)


class TestPipelineE2ENoOpportunities:
    """Run where all signals score below threshold → no digest sent."""

    def test_no_digest_when_all_scores_below_threshold(self, tmp_db_path, monkeypatch):
        import storage.db as db_module
        monkeypatch.setattr(db_module, "_DB_PATH", tmp_db_path)

        # LLM returns 0.0 for everything
        zero_classify = [
            {**sig, "technical_fit": 0.0} for sig in CLASSIFY_RESPONSE
        ]
        zero_timing = {**TIMING_RESPONSE, "timing_fit": 0.0, "commercial_priority": 0.0}

        def _zero_router(prompt_name, user_message, **kwargs):
            mapping = {
                "structure_news": {**STRUCTURE_RESPONSE},
                "enrich_news": {**ENRICH_RESPONSE},
                "classify_product": list(zero_classify),
                "evaluate_timing": zero_timing,
                "compose_email": {**COMPOSE_RESPONSE},
            }
            return mapping[prompt_name]

        settings_override = {
            **OFFLINE_SETTINGS,
            "storage": {"db_path": str(tmp_db_path)},
            "pipeline": {"min_opportunity_score": 0.5, "max_opportunities_in_digest": 5},
        }

        with patch("llm.client.complete", side_effect=_zero_router), \
             patch("pipeline.distribution.email_composer._send_email") as mock_smtp, \
             patch("pipeline.ingestion.daily_logix_scraper.settings", settings_override), \
             patch("pipeline.routing.opportunity_router.settings", settings_override), \
             patch("pipeline.distribution.email_composer.settings", settings_override), \
             patch("config.settings", settings_override):

            from main import cmd_run
            cmd_run(dry_run=False)

        mock_smtp.assert_not_called()

        import sqlite3
        conn = sqlite3.connect(tmp_db_path)
        count = conn.execute("SELECT COUNT(*) FROM digests").fetchone()[0]
        conn.close()
        assert count == 0


class TestPipelineE2EDryRun:
    """--dry mode: pipeline runs but no email sent and no digest row."""

    def test_dry_run_skips_smtp(self, tmp_db_path, monkeypatch):
        import storage.db as db_module
        monkeypatch.setattr(db_module, "_DB_PATH", tmp_db_path)

        settings_override = {
            **OFFLINE_SETTINGS,
            "storage": {"db_path": str(tmp_db_path)},
        }

        with patch("llm.client.complete", side_effect=_llm_router), \
             patch("pipeline.distribution.email_composer._send_email") as mock_smtp, \
             patch("pipeline.ingestion.daily_logix_scraper.settings", settings_override), \
             patch("pipeline.routing.opportunity_router.settings", settings_override), \
             patch("pipeline.distribution.email_composer.settings", settings_override), \
             patch("config.settings", settings_override):

            from main import cmd_run
            cmd_run(dry_run=True)

        mock_smtp.assert_not_called()

    def test_dry_run_still_processes_all_stages(self, tmp_db_path, monkeypatch):
        import storage.db as db_module
        monkeypatch.setattr(db_module, "_DB_PATH", tmp_db_path)

        settings_override = {
            **OFFLINE_SETTINGS,
            "storage": {"db_path": str(tmp_db_path)},
        }

        with patch("llm.client.complete", side_effect=_llm_router), \
             patch("pipeline.distribution.email_composer._send_email"), \
             patch("pipeline.ingestion.daily_logix_scraper.settings", settings_override), \
             patch("pipeline.routing.opportunity_router.settings", settings_override), \
             patch("pipeline.distribution.email_composer.settings", settings_override), \
             patch("config.settings", settings_override):

            from main import cmd_run
            cmd_run(dry_run=True)

        import sqlite3
        conn = sqlite3.connect(tmp_db_path)
        news_count = conn.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]
        signal_count = conn.execute("SELECT COUNT(*) FROM product_signals").fetchone()[0]
        conn.close()

        assert news_count == NEWS_COUNT
        assert signal_count == NEWS_COUNT * PRODUCTS_COUNT
