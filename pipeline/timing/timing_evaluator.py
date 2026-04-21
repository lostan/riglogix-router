"""
Timing stage — evaluate timing fit and commercial priority for each product signal.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from llm.client import complete

logger = logging.getLogger(__name__)


def _load_seller_profile() -> str:
    path = Path("seller_workspace/seller_profile.md")
    return path.read_text() if path.exists() else ""


def evaluate_timing(signal: dict, enriched_item: dict) -> dict:
    """
    Call LLM to assess timing fit and commercial priority for one (news, product) pair.
    Returns a dict with timing fields to merge back into the signal.
    """
    seller_profile = _load_seller_profile()
    context = f"## Seller Profile\n\n{seller_profile}"

    user_message = (
        f"## Product: {signal['product']}\n"
        f"## Technical Fit Score: {signal['technical_fit']}\n\n"
        "## Enriched News Context\n\n"
        + json.dumps(
            {
                k: v
                for k, v in enriched_item.items()
                if k not in ("run_id", "fetched_at", "source", "body")
            },
            ensure_ascii=False,
            indent=2,
        )
        + f"\n\n## News Title\n{enriched_item.get('title', '')}"
        + f"\n\n## News Excerpt\n{enriched_item.get('body', '')[:800]}"
        + f"\n\n## Classification Rationale\n{signal.get('rationale', '[]')}"
    )

    result = complete("evaluate_timing", user_message, context=context)

    return {
        "timing_fit": round(float(result.get("timing_fit", 0.0)), 3),
        "commercial_priority": round(float(result.get("commercial_priority", 0.0)), 3),
        "window_description": result.get("window_description", ""),
        "window_open": result.get("window_open"),
        "window_close": result.get("window_close"),
        "urgency": result.get("urgency", "unknown"),
        "timing_rationale": result.get("timing_rationale", []),
        "recommended_action": result.get("recommended_action", ""),
    }


def run(
    conn: sqlite3.Connection,
    signals: list[dict],
    enriched_items: list[dict],
) -> list[dict]:
    """
    Evaluate timing for all signals. Returns signals with timing fields populated.
    """
    enriched_by_id = {item["id"]: item for item in enriched_items}

    evaluated = []
    for signal in signals:
        news_id = signal["news_id"]
        product = signal["product"]
        logger.info("Evaluating timing for %s / %s", news_id, product)

        enriched_item = enriched_by_id.get(news_id, {})

        try:
            timing = evaluate_timing(signal, enriched_item)
            updated = {**signal, **timing}

            # Composite score: weighted average of the three dimensions
            tf = updated["technical_fit"]
            tif = updated["timing_fit"]
            cp = updated["commercial_priority"]
            updated["composite_score"] = round(0.4 * tf + 0.35 * tif + 0.25 * cp, 3)

            evaluated.append(updated)
        except Exception as e:
            logger.error("Failed to evaluate timing for %s/%s: %s", news_id, product, e)
            signal["composite_score"] = round(signal["technical_fit"] * 0.4, 3)
            evaluated.append(signal)

    return evaluated
