"""
Tests for pipeline/classification/product_classifier.py.

LLM is mocked. Tests cover: product list completeness, score bounds,
rationale serialisation, unknown product filtering, error handling.
"""

import json
from unittest.mock import patch

import pytest

from pipeline.classification.product_classifier import (
    classify_news_item,
    run as run_classification,
)
from storage.db import insert_news_item
from tests.conftest import RUN_ID, CLASSIFY_RESPONSE

EXPECTED_PRODUCTS = {
    "SWIM",
    "DynOps",
    "Conductor Analysis",
    "Riser Analysis",
    "DP Feasibility Study",
}


class TestClassifyNewsItem:
    def test_returns_one_signal_per_known_product(self, mock_llm, enriched_item):
        mock_llm.return_value = CLASSIFY_RESPONSE
        signals = classify_news_item(enriched_item["id"], enriched_item)
        products = {s["product"] for s in signals}
        assert products == EXPECTED_PRODUCTS

    def test_technical_fit_is_float_between_0_and_1(self, mock_llm, enriched_item):
        mock_llm.return_value = CLASSIFY_RESPONSE
        signals = classify_news_item(enriched_item["id"], enriched_item)
        for sig in signals:
            assert 0.0 <= sig["technical_fit"] <= 1.0

    def test_rationale_stored_as_json_string(self, mock_llm, enriched_item):
        mock_llm.return_value = CLASSIFY_RESPONSE
        signals = classify_news_item(enriched_item["id"], enriched_item)
        for sig in signals:
            parsed = json.loads(sig["rationale"])
            assert isinstance(parsed, list)

    def test_timing_and_priority_placeholders_are_zero(self, mock_llm, enriched_item):
        mock_llm.return_value = CLASSIFY_RESPONSE
        signals = classify_news_item(enriched_item["id"], enriched_item)
        for sig in signals:
            assert sig["timing_fit"] == 0.0
            assert sig["commercial_priority"] == 0.0
            assert sig["composite_score"] == 0.0

    def test_unknown_product_is_filtered_out(self, mock_llm, enriched_item):
        response_with_unknown = [
            *CLASSIFY_RESPONSE,
            {"product": "UnknownProduct", "technical_fit": 0.9, "rationale": []},
        ]
        mock_llm.return_value = response_with_unknown
        signals = classify_news_item(enriched_item["id"], enriched_item)
        products = {s["product"] for s in signals}
        assert "UnknownProduct" not in products

    def test_non_list_llm_response_raises(self, mock_llm, enriched_item):
        mock_llm.return_value = {"error": "unexpected shape"}
        with pytest.raises(ValueError, match="Unexpected LLM response"):
            classify_news_item(enriched_item["id"], enriched_item)

    def test_news_id_injected_in_all_signals(self, mock_llm, enriched_item):
        mock_llm.return_value = CLASSIFY_RESPONSE
        signals = classify_news_item(enriched_item["id"], enriched_item)
        for sig in signals:
            assert sig["news_id"] == enriched_item["id"]

    def test_uncertainty_propagated_from_enriched_item(self, mock_llm, enriched_item):
        enriched_item_copy = {**enriched_item, "uncertainty": "high"}
        mock_llm.return_value = CLASSIFY_RESPONSE
        signals = classify_news_item(enriched_item_copy["id"], enriched_item_copy)
        for sig in signals:
            assert sig["uncertainty"] == "high"

    def test_product_context_loaded_from_workspace(self, mock_llm, enriched_item):
        """Verify that classify_product prompt is called (product files are loaded)."""
        mock_llm.return_value = CLASSIFY_RESPONSE
        classify_news_item(enriched_item["id"], enriched_item)
        call_args = mock_llm.call_args
        assert call_args[0][0] == "classify_product"


class TestRunClassification:
    def test_run_returns_flat_signal_list(self, db_conn, enriched_item, mock_llm):
        mock_llm.return_value = CLASSIFY_RESPONSE
        insert_news_item(db_conn, enriched_item)
        db_conn.commit()

        signals = run_classification(db_conn, [enriched_item], RUN_ID)

        assert len(signals) == len(EXPECTED_PRODUCTS)
        for sig in signals:
            assert sig["run_id"] == RUN_ID

    def test_run_skips_item_on_error(self, db_conn, enriched_item, mock_llm):
        mock_llm.side_effect = ValueError("LLM error")
        insert_news_item(db_conn, enriched_item)

        signals = run_classification(db_conn, [enriched_item], RUN_ID)

        assert signals == []

    def test_run_multiple_items_produces_correct_signal_count(
        self, db_conn, raw_news_list, mock_llm
    ):
        mock_llm.return_value = CLASSIFY_RESPONSE
        for item in raw_news_list:
            insert_news_item(db_conn, item)
        db_conn.commit()

        enriched_list = [{**item, "uncertainty": "low"} for item in raw_news_list]
        signals = run_classification(db_conn, enriched_list, RUN_ID)

        assert len(signals) == len(raw_news_list) * len(EXPECTED_PRODUCTS)
