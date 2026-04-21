"""
Classification stage — evaluate technical fit of each product against an enriched news item.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from llm.client import complete
from storage.db import insert_signal

logger = logging.getLogger(__name__)

_PRODUCTS = [
    "SWIM",
    "DynOps",
    "Conductor Analysis",
    "Riser Analysis",
    "DP Feasibility Study",
]


def _load_product_context() -> str:
    parts = []
    products_dir = Path("seller_workspace/products")
    for product_file in sorted(products_dir.glob("*.md")):
        parts.append(product_file.read_text())

    rules = Path("seller_workspace/classification_rules.md")
    if rules.exists():
        parts.append(f"## Classification Rules\n\n{rules.read_text()}")

    return "\n\n---\n\n".join(parts)


def classify_news_item(news_id: str, enriched: dict) -> list[dict]:
    """
    Call LLM to score all products against one enriched news item.
    Returns list of signal dicts ready for DB insertion.
    """
    context = _load_product_context()

    user_message = (
        "## Enriched News Item\n\n"
        + json.dumps(
            {
                k: v
                for k, v in enriched.items()
                if k
                not in (
                    "run_id",
                    "fetched_at",
                    "source",
                    "structured_at",
                    "enriched_at",
                )
            },
            ensure_ascii=False,
            indent=2,
        )
        + f"\n\n## Title\n{enriched.get('title', '')}"
        + f"\n\n## Body\n{enriched.get('body', '')[:1500]}"
    )

    results = complete("classify_product", user_message, context=context)

    if not isinstance(results, list):
        raise ValueError(f"Unexpected LLM response shape for classification: {type(results)}")

    now = datetime.now(timezone.utc).isoformat()
    signals = []

    for item in results:
        product = item.get("product", "")
        if product not in _PRODUCTS:
            logger.warning("Unknown product in classification response: %s", product)
            continue

        technical_fit = float(item.get("technical_fit", 0.0))
        rationale = item.get("rationale", [])
        uncertainty = enriched.get("uncertainty", "medium")

        signals.append(
            {
                "news_id": news_id,
                "product": product,
                "technical_fit": round(technical_fit, 3),
                "rationale": json.dumps(rationale, ensure_ascii=False),
                "uncertainty": uncertainty,
                "evaluated_at": now,
                # timing_fit and commercial_priority filled by timing stage
                "timing_fit": 0.0,
                "commercial_priority": 0.0,
                "composite_score": 0.0,
            }
        )

    return signals


def run(
    conn: sqlite3.Connection, enriched_items: list[dict], run_id: str
) -> list[dict]:
    """
    Classify all enriched news items and persist product signals.
    Returns flat list of signal dicts with run_id.
    """
    all_signals = []

    for item in enriched_items:
        news_id = item["id"]
        logger.info("Classifying news item %s", news_id)

        try:
            signals = classify_news_item(news_id, item)
            for sig in signals:
                sig["run_id"] = run_id
            all_signals.extend(signals)
        except Exception as e:
            logger.error("Failed to classify news %s: %s", news_id, e)

    logger.info("Created %d product signals from %d news items", len(all_signals), len(enriched_items))
    return all_signals
