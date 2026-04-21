"""
Tests for pipeline/enrichment/enricher.py.

LLM is mocked. Tests cover field mapping, JSON serialisation of lists/dicts,
uncertainty propagation, error fallback (item passed through un-enriched).
"""

import json
from unittest.mock import patch

import pytest

from pipeline.enrichment.enricher import enrich_news_item, run as run_enrichment
from storage.db import insert_news_item, insert_structured_news
from tests.conftest import RUN_ID, ENRICH_RESPONSE


def _structured_db_row(news_id: str) -> dict:
    from datetime import datetime, timezone
    return {
        "news_id": news_id,
        "client": "Petrobras",
        "geography": "Brazil",
        "operation_type": "drilling",
        "wells": json.dumps(["12 wells"]),
        "asset": "Buzios",
        "phase": "development",
        "timing_raw": "Q3 2025",
        "environment": "ultra-deepwater",
        "depth_m": 2000.0,
        "contractor": "Transocean",
        "history_notes": None,
        "structured_at": datetime.now(timezone.utc).isoformat(),
    }


class TestEnrichNewsItem:
    def test_returns_correct_fields(self, mock_llm):
        mock_llm.return_value = {**ENRICH_RESPONSE}
        result = enrich_news_item("news001", {"title": "Test", "body": "Test body"})
        assert result["news_id"] == "news001"
        assert result["rig"] == "Deepwater Titan"
        assert result["uncertainty"] == "low"
        assert "enriched_at" in result

    def test_wells_json_list_is_serialised(self, mock_llm):
        mock_llm.return_value = {**ENRICH_RESPONSE, "wells_json": ["W-1", "W-2"]}
        result = enrich_news_item("news001", {})
        assert isinstance(result["wells_json"], str)
        assert json.loads(result["wells_json"]) == ["W-1", "W-2"]

    def test_wells_json_none_becomes_empty_array(self, mock_llm):
        mock_llm.return_value = {**ENRICH_RESPONSE, "wells_json": None}
        result = enrich_news_item("news001", {})
        assert result["wells_json"] == "[]"

    def test_relationships_json_dict_is_serialised(self, mock_llm):
        rel = {"operator": "Petrobras", "drilling_contractor": "Transocean"}
        mock_llm.return_value = {**ENRICH_RESPONSE, "relationships_json": rel}
        result = enrich_news_item("news001", {})
        parsed = json.loads(result["relationships_json"])
        assert parsed["operator"] == "Petrobras"

    def test_relationships_json_none_becomes_empty_object(self, mock_llm):
        mock_llm.return_value = {**ENRICH_RESPONSE, "relationships_json": None}
        result = enrich_news_item("news001", {})
        assert result["relationships_json"] == "{}"

    def test_enrichment_rules_loaded_as_context(self, mock_llm, tmp_path, monkeypatch):
        rules_file = tmp_path / "enrichment_rules.md"
        rules_file.write_text("## Test Rule\n- Depth inference rule")

        with patch("pipeline.enrichment.enricher.Path") as mock_path_cls:
            mock_path_instance = mock_path_cls.return_value
            mock_path_instance.exists.return_value = True
            mock_path_instance.read_text.return_value = "## Test Rule"
            mock_llm.return_value = {**ENRICH_RESPONSE}
            enrich_news_item("news001", {})

        call_kwargs = mock_llm.call_args[1]
        assert "context" in call_kwargs


class TestRunEnrichment:
    def test_run_persists_to_db(self, db_conn, structured_item, mock_llm):
        mock_llm.return_value = {**ENRICH_RESPONSE}
        insert_news_item(db_conn, structured_item)
        insert_structured_news(db_conn, _structured_db_row(structured_item["id"]))
        db_conn.commit()

        run_enrichment(db_conn, [structured_item])
        db_conn.commit()

        row = db_conn.execute(
            "SELECT * FROM enriched_news WHERE news_id = ?",
            (structured_item["id"],),
        ).fetchone()
        assert row is not None
        assert row["uncertainty"] == "low"

    def test_run_returns_merged_dicts(self, db_conn, structured_item, mock_llm):
        mock_llm.return_value = {**ENRICH_RESPONSE}
        insert_news_item(db_conn, structured_item)

        result = run_enrichment(db_conn, [structured_item])

        assert len(result) == 1
        assert result[0]["id"] == structured_item["id"]
        assert result[0]["rig"] == "Deepwater Titan"
        assert result[0]["client"] == "Petrobras"

    def test_run_falls_back_on_llm_error(self, db_conn, structured_item, mock_llm):
        """Item is kept in output even if enrichment fails; no crash."""
        mock_llm.side_effect = ValueError("LLM error")
        insert_news_item(db_conn, structured_item)

        result = run_enrichment(db_conn, [structured_item])

        assert len(result) == 1
        assert result[0]["id"] == structured_item["id"]
        count = db_conn.execute("SELECT COUNT(*) FROM enriched_news").fetchone()[0]
        assert count == 0

    def test_run_processes_multiple_items(self, db_conn, raw_news_list, mock_llm):
        mock_llm.return_value = {**ENRICH_RESPONSE}
        structured_list = [
            {**item, "client": "Test", "structured_at": "2025-01-01T00:00:00+00:00"}
            for item in raw_news_list
        ]
        for item in structured_list:
            insert_news_item(db_conn, item)
        db_conn.commit()

        result = run_enrichment(db_conn, structured_list)
        assert len(result) == len(structured_list)
