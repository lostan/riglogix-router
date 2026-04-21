"""
Tests for pipeline/structuring/news_parser.py.

LLM is mocked — tests verify field mapping, wells JSON normalisation,
DB persistence, and graceful skip on LLM error.
"""

import json
from unittest.mock import patch

import pytest

from pipeline.structuring.news_parser import structure_news_item, run as run_structuring
from storage.db import insert_news_item
from tests.conftest import RUN_ID, STRUCTURE_RESPONSE


class TestStructureNewsItem:
    def test_returns_correct_fields(self, mock_llm):
        mock_llm.return_value = {**STRUCTURE_RESPONSE}
        result = structure_news_item("news001", "Test Title", "Test body")
        assert result["client"] == "Petrobras"
        assert result["geography"] == "Brazil — Santos Basin"
        assert result["phase"] == "development"
        assert result["news_id"] == "news001"
        assert "structured_at" in result

    def test_wells_list_is_json_serialised(self, mock_llm):
        mock_llm.return_value = {
            **STRUCTURE_RESPONSE,
            "wells": ["W-1", "W-2", "W-3"],
        }
        result = structure_news_item("news001", "Title", "Body")
        assert isinstance(result["wells"], str)
        wells = json.loads(result["wells"])
        assert wells == ["W-1", "W-2", "W-3"]

    def test_wells_none_becomes_empty_json_array(self, mock_llm):
        mock_llm.return_value = {**STRUCTURE_RESPONSE, "wells": None}
        result = structure_news_item("news001", "Title", "Body")
        assert result["wells"] == "[]"

    def test_structured_at_is_iso8601(self, mock_llm):
        mock_llm.return_value = {**STRUCTURE_RESPONSE}
        result = structure_news_item("news001", "Title", "Body")
        from datetime import datetime
        datetime.fromisoformat(result["structured_at"])  # raises if invalid

    def test_llm_called_with_title_and_body(self, mock_llm):
        mock_llm.return_value = {**STRUCTURE_RESPONSE}
        structure_news_item("news001", "My Title", "My Body")
        call_args = mock_llm.call_args
        assert call_args[0][0] == "structure_news"
        assert "My Title" in call_args[0][1]
        assert "My Body" in call_args[0][1]


class TestRunStructuring:
    def test_run_persists_to_db(self, db_conn, raw_news_item, mock_llm):
        mock_llm.return_value = {**STRUCTURE_RESPONSE}
        insert_news_item(db_conn, raw_news_item)
        db_conn.commit()

        result = run_structuring(db_conn, [raw_news_item])
        db_conn.commit()

        row = db_conn.execute(
            "SELECT * FROM structured_news WHERE news_id = ?",
            (raw_news_item["id"],),
        ).fetchone()
        assert row is not None
        assert row["client"] == "Petrobras"

    def test_run_returns_merged_dicts(self, db_conn, raw_news_item, mock_llm):
        mock_llm.return_value = {**STRUCTURE_RESPONSE}
        insert_news_item(db_conn, raw_news_item)

        result = run_structuring(db_conn, [raw_news_item])

        assert len(result) == 1
        assert result[0]["id"] == raw_news_item["id"]
        assert result[0]["client"] == "Petrobras"
        assert result[0]["title"] == raw_news_item["title"]

    def test_run_skips_item_on_llm_error(self, db_conn, raw_news_item, mock_llm):
        mock_llm.side_effect = ValueError("LLM failed")
        insert_news_item(db_conn, raw_news_item)

        result = run_structuring(db_conn, [raw_news_item])

        # Failed item skipped, no crash
        assert result == []
        row = db_conn.execute("SELECT COUNT(*) FROM structured_news").fetchone()[0]
        assert row == 0

    def test_run_processes_multiple_items(self, db_conn, raw_news_list, mock_llm):
        mock_llm.return_value = {**STRUCTURE_RESPONSE}
        for item in raw_news_list:
            insert_news_item(db_conn, item)
        db_conn.commit()

        result = run_structuring(db_conn, raw_news_list)
        assert len(result) == len(raw_news_list)
        count = db_conn.execute("SELECT COUNT(*) FROM structured_news").fetchone()[0]
        assert count == len(raw_news_list)
