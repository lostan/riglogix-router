"""
Tests for pipeline/ingestion/daily_logix_scraper.py.

Uses fixture JSON files so no HTTP calls are made.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ingestion.daily_logix_scraper import fetch_news, _make_id
from tests.conftest import RUN_ID


class TestMakeId:
    def test_deterministic(self):
        id1 = _make_id("https://example.com/article", "Petrobras Signs Contract")
        id2 = _make_id("https://example.com/article", "Petrobras Signs Contract")
        assert id1 == id2

    def test_different_inputs_different_ids(self):
        id1 = _make_id("https://example.com/a", "Title A")
        id2 = _make_id("https://example.com/b", "Title B")
        assert id1 != id2

    def test_returns_16_char_hex(self):
        result = _make_id("url", "title")
        assert len(result) == 16
        assert all(c in "0123456789abcdef" for c in result)


class TestFetchNewsFixtures:
    def test_loads_fixture_json(self, tmp_path):
        fixture_data = [
            {
                "title": "Test Article",
                "body": "Test body content.",
                "source_url": "https://fixture.example.com/1",
                "published_at": "2025-04-15",
            }
        ]
        fixture_file = tmp_path / "test.json"
        fixture_file.write_text(json.dumps(fixture_data))

        with patch("pipeline.ingestion.daily_logix_scraper.settings", {
            "ingestion": {
                "use_fixtures": True,
                "fixtures_path": str(tmp_path),
                "max_news_per_run": 15,
            }
        }):
            items = fetch_news(RUN_ID)

        assert len(items) == 1
        assert items[0]["title"] == "Test Article"
        assert items[0]["source"] == "daily_logix"
        assert items[0]["run_id"] == RUN_ID

    def test_id_injected_into_each_item(self, tmp_path):
        fixture_data = [
            {"title": "Article A", "body": "Body A", "source_url": "https://a.com/1"},
            {"title": "Article B", "body": "Body B", "source_url": "https://b.com/2"},
        ]
        (tmp_path / "news.json").write_text(json.dumps(fixture_data))

        with patch("pipeline.ingestion.daily_logix_scraper.settings", {
            "ingestion": {
                "use_fixtures": True,
                "fixtures_path": str(tmp_path),
                "max_news_per_run": 15,
            }
        }):
            items = fetch_news(RUN_ID)

        ids = [item["id"] for item in items]
        assert len(set(ids)) == 2  # all unique

    def test_max_items_cap_is_respected(self, tmp_path):
        fixture_data = [
            {"title": f"Article {i}", "body": "Body", "source_url": f"https://x.com/{i}"}
            for i in range(20)
        ]
        (tmp_path / "news.json").write_text(json.dumps(fixture_data))

        with patch("pipeline.ingestion.daily_logix_scraper.settings", {
            "ingestion": {
                "use_fixtures": True,
                "fixtures_path": str(tmp_path),
                "max_news_per_run": 5,
            }
        }):
            items = fetch_news(RUN_ID)

        assert len(items) == 5

    def test_sample_fixture_loads_correctly(self):
        fixtures_path = Path(__file__).parent / "fixtures"

        with patch("pipeline.ingestion.daily_logix_scraper.settings", {
            "ingestion": {
                "use_fixtures": True,
                "fixtures_path": str(fixtures_path),
                "max_news_per_run": 15,
            }
        }):
            items = fetch_news(RUN_ID)

        assert len(items) == 5
        for item in items:
            assert "id" in item
            assert "title" in item
            assert "body" in item
            assert item["run_id"] == RUN_ID
            assert item["source"] == "daily_logix"

    def test_dict_fixture_wraps_single_object(self, tmp_path):
        """A fixture file may contain a single dict instead of a list."""
        single = {
            "title": "Single Article",
            "body": "Body text",
            "source_url": "https://x.com/1",
        }
        (tmp_path / "single.json").write_text(json.dumps(single))

        with patch("pipeline.ingestion.daily_logix_scraper.settings", {
            "ingestion": {
                "use_fixtures": True,
                "fixtures_path": str(tmp_path),
                "max_news_per_run": 15,
            }
        }):
            items = fetch_news(RUN_ID)

        assert len(items) == 1
        assert items[0]["title"] == "Single Article"
