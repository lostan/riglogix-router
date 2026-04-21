"""
Tests for pipeline/timing/timing_evaluator.py.

Key invariant: composite_score = 0.4*technical_fit + 0.35*timing_fit + 0.25*commercial_priority
Error fallback: composite_score = 0.4 * technical_fit (partial score, no crash).
"""

import pytest

from pipeline.timing.timing_evaluator import evaluate_timing, run as run_timing
from tests.conftest import RUN_ID, TIMING_RESPONSE


class TestCompositeScoreFormula:
    def test_composite_score_exact_formula(self, mock_llm, signal_base, enriched_item):
        mock_llm.return_value = {**TIMING_RESPONSE, "timing_fit": 0.80, "commercial_priority": 0.75}
        result = run_timing(None, [signal_base], [enriched_item])

        expected = round(0.4 * 0.85 + 0.35 * 0.80 + 0.25 * 0.75, 3)
        assert result[0]["composite_score"] == expected

    def test_composite_score_all_zeros(self, mock_llm, signal_base, enriched_item):
        s = {**signal_base, "technical_fit": 0.0}
        mock_llm.return_value = {**TIMING_RESPONSE, "timing_fit": 0.0, "commercial_priority": 0.0}
        result = run_timing(None, [s], [enriched_item])
        assert result[0]["composite_score"] == 0.0

    def test_composite_score_all_ones(self, mock_llm, signal_base, enriched_item):
        s = {**signal_base, "technical_fit": 1.0}
        mock_llm.return_value = {**TIMING_RESPONSE, "timing_fit": 1.0, "commercial_priority": 1.0}
        result = run_timing(None, [s], [enriched_item])
        assert result[0]["composite_score"] == 1.0

    def test_composite_score_rounded_to_3_decimals(self, mock_llm, signal_base, enriched_item):
        s = {**signal_base, "technical_fit": 0.333}
        mock_llm.return_value = {**TIMING_RESPONSE, "timing_fit": 0.333, "commercial_priority": 0.333}
        result = run_timing(None, [s], [enriched_item])
        score_str = str(result[0]["composite_score"])
        decimals = len(score_str.split(".")[-1]) if "." in score_str else 0
        assert decimals <= 3


class TestEvaluateTiming:
    def test_returns_all_timing_fields(self, mock_llm, signal_base, enriched_item):
        mock_llm.return_value = {**TIMING_RESPONSE}
        result = evaluate_timing(signal_base, enriched_item)
        assert "timing_fit" in result
        assert "commercial_priority" in result
        assert "window_description" in result
        assert "urgency" in result
        assert "recommended_action" in result
        assert "timing_rationale" in result

    def test_timing_fit_is_float(self, mock_llm, signal_base, enriched_item):
        mock_llm.return_value = {**TIMING_RESPONSE}
        result = evaluate_timing(signal_base, enriched_item)
        assert isinstance(result["timing_fit"], float)
        assert isinstance(result["commercial_priority"], float)

    def test_llm_called_with_correct_prompt_name(self, mock_llm, signal_base, enriched_item):
        mock_llm.return_value = {**TIMING_RESPONSE}
        evaluate_timing(signal_base, enriched_item)
        assert mock_llm.call_args[0][0] == "evaluate_timing"

    def test_product_name_in_user_message(self, mock_llm, signal_base, enriched_item):
        mock_llm.return_value = {**TIMING_RESPONSE}
        evaluate_timing(signal_base, enriched_item)
        user_msg = mock_llm.call_args[0][1]
        assert "DynOps" in user_msg

    def test_missing_fields_default_gracefully(self, mock_llm, signal_base, enriched_item):
        mock_llm.return_value = {}  # empty response
        result = evaluate_timing(signal_base, enriched_item)
        assert result["timing_fit"] == 0.0
        assert result["commercial_priority"] == 0.0
        assert result["urgency"] == "unknown"


class TestRunTimingFallback:
    def test_fallback_on_llm_error(self, mock_llm, signal_base, enriched_item):
        """On LLM failure, item is still returned with partial composite score."""
        mock_llm.side_effect = ValueError("LLM error")
        result = run_timing(None, [signal_base], [enriched_item])

        assert len(result) == 1
        expected_fallback = round(signal_base["technical_fit"] * 0.4, 3)
        assert result[0]["composite_score"] == expected_fallback

    def test_enriched_item_lookup_by_news_id(self, mock_llm, signal_base, enriched_item):
        """Timing evaluator looks up enriched_item by news_id."""
        unrelated = {**enriched_item, "id": "unrelated0000000"}
        mock_llm.return_value = {**TIMING_RESPONSE}
        result = run_timing(None, [signal_base], [unrelated])

        # Signal still processed (enriched_item not found → empty dict context)
        assert len(result) == 1

    def test_multiple_signals_all_evaluated(self, mock_llm, signal_base, enriched_item):
        mock_llm.return_value = {**TIMING_RESPONSE}
        signals = [
            {**signal_base, "product": "DynOps"},
            {**signal_base, "product": "Riser Analysis"},
            {**signal_base, "product": "Conductor Analysis"},
        ]
        result = run_timing(None, signals, [enriched_item])
        assert len(result) == 3
        for item in result:
            assert item["composite_score"] > 0
