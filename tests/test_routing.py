"""
Tests for pipeline/routing/opportunity_router.py.

Covers: min_score filtering, descending rank order, max_opps cap,
DB persistence of all signals, db_id injection into top signals.
"""

import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipeline.routing.opportunity_router import route
from storage.db import insert_run, insert_news_item
from tests.conftest import RUN_ID


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_evaluated_signal(
    news_id: str,
    product: str,
    composite_score: float,
) -> dict:
    return {
        "news_id": news_id,
        "run_id": RUN_ID,
        "product": product,
        "technical_fit": composite_score,
        "timing_fit": composite_score,
        "commercial_priority": composite_score,
        "composite_score": composite_score,
        "rationale": json.dumps(["reason"]),
        "uncertainty": "low",
        "evaluated_at": _now(),
    }


def _seed(db_conn, raw_news_item):
    insert_run(db_conn, RUN_ID, _now())
    insert_news_item(db_conn, raw_news_item)
    db_conn.commit()


class TestRouteFiltering:
    def test_signals_below_threshold_excluded(self, db_conn, raw_news_item):
        _seed(db_conn, raw_news_item)
        signals = [
            _make_evaluated_signal(raw_news_item["id"], "DynOps", 0.80),
            _make_evaluated_signal(raw_news_item["id"], "SWIM", 0.20),
        ]
        with patch(
            "pipeline.routing.opportunity_router.settings",
            {"pipeline": {"min_opportunity_score": 0.5, "max_opportunities_in_digest": 10}},
        ):
            top = route(db_conn, signals)

        products = [s["product"] for s in top]
        assert "DynOps" in products
        assert "SWIM" not in products

    def test_all_signals_above_threshold_included(self, db_conn, raw_news_item):
        _seed(db_conn, raw_news_item)
        signals = [
            _make_evaluated_signal(raw_news_item["id"], "DynOps", 0.80),
            _make_evaluated_signal(raw_news_item["id"], "Riser Analysis", 0.70),
        ]
        with patch(
            "pipeline.routing.opportunity_router.settings",
            {"pipeline": {"min_opportunity_score": 0.5, "max_opportunities_in_digest": 10}},
        ):
            top = route(db_conn, signals)

        assert len(top) == 2

    def test_empty_signals_returns_empty_list(self, db_conn):
        insert_run(db_conn, RUN_ID, _now())
        db_conn.commit()
        with patch(
            "pipeline.routing.opportunity_router.settings",
            {"pipeline": {"min_opportunity_score": 0.5, "max_opportunities_in_digest": 5}},
        ):
            top = route(db_conn, [])
        assert top == []


class TestRouteRanking:
    def test_output_ordered_by_composite_score_descending(self, db_conn, raw_news_item):
        _seed(db_conn, raw_news_item)
        signals = [
            _make_evaluated_signal(raw_news_item["id"], "SWIM", 0.60),
            _make_evaluated_signal(raw_news_item["id"], "DynOps", 0.90),
            _make_evaluated_signal(raw_news_item["id"], "Riser Analysis", 0.75),
        ]
        with patch(
            "pipeline.routing.opportunity_router.settings",
            {"pipeline": {"min_opportunity_score": 0.0, "max_opportunities_in_digest": 10}},
        ):
            top = route(db_conn, signals)

        scores = [s["composite_score"] for s in top]
        assert scores == sorted(scores, reverse=True)

    def test_max_opps_cap_respected(self, db_conn, raw_news_item):
        _seed(db_conn, raw_news_item)
        signals = [
            _make_evaluated_signal(raw_news_item["id"], p, 0.75)
            for p in ["DynOps", "SWIM", "Riser Analysis", "Conductor Analysis", "DP Feasibility Study"]
        ]
        with patch(
            "pipeline.routing.opportunity_router.settings",
            {"pipeline": {"min_opportunity_score": 0.0, "max_opportunities_in_digest": 3}},
        ):
            top = route(db_conn, signals)

        assert len(top) == 3


class TestRouteDbPersistence:
    def test_all_signals_persisted_regardless_of_threshold(self, db_conn, raw_news_item):
        _seed(db_conn, raw_news_item)
        signals = [
            _make_evaluated_signal(raw_news_item["id"], "DynOps", 0.80),
            _make_evaluated_signal(raw_news_item["id"], "SWIM", 0.20),
        ]
        with patch(
            "pipeline.routing.opportunity_router.settings",
            {"pipeline": {"min_opportunity_score": 0.5, "max_opportunities_in_digest": 10}},
        ):
            route(db_conn, signals)
        db_conn.commit()

        count = db_conn.execute("SELECT COUNT(*) FROM product_signals").fetchone()[0]
        assert count == 2

    def test_db_id_injected_into_top_signals(self, db_conn, raw_news_item):
        _seed(db_conn, raw_news_item)
        signals = [_make_evaluated_signal(raw_news_item["id"], "DynOps", 0.80)]
        with patch(
            "pipeline.routing.opportunity_router.settings",
            {"pipeline": {"min_opportunity_score": 0.0, "max_opportunities_in_digest": 5}},
        ):
            top = route(db_conn, signals)
        db_conn.commit()

        assert "db_id" in top[0]
        assert isinstance(top[0]["db_id"], int)

    def test_db_id_matches_actual_row(self, db_conn, raw_news_item):
        _seed(db_conn, raw_news_item)
        signals = [_make_evaluated_signal(raw_news_item["id"], "DynOps", 0.80)]
        with patch(
            "pipeline.routing.opportunity_router.settings",
            {"pipeline": {"min_opportunity_score": 0.0, "max_opportunities_in_digest": 5}},
        ):
            top = route(db_conn, signals)
        db_conn.commit()

        row = db_conn.execute(
            "SELECT id FROM product_signals WHERE id = ?", (top[0]["db_id"],)
        ).fetchone()
        assert row is not None
