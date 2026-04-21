"""
Enrichment stage — infer additional technical fields using domain knowledge.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from llm.client import complete
from storage.db import insert_enriched_news

logger = logging.getLogger(__name__)

def _load_rules() -> str:
    path = Path("seller_workspace/enrichment_rules.md")
    return path.read_text() if path.exists() else ""


def enrich_news_item(news_id: str, structured: dict) -> dict:
    """Call LLM to enrich a structured news item."""
    rules = _load_rules()
    context = f"## Enrichment Rules\n\n{rules}"

    user_message = (
        "## Structured News\n\n"
        + json.dumps(
            {k: v for k, v in structured.items() if k not in ("id", "run_id", "fetched_at", "source")},
            ensure_ascii=False,
            indent=2,
        )
    )

    result = complete("enrich_news", user_message, context=context)

    result["news_id"] = news_id
    result["enriched_at"] = datetime.now(timezone.utc).isoformat()

    # Normalize JSON fields
    if isinstance(result.get("wells_json"), list):
        result["wells_json"] = json.dumps(result["wells_json"])
    elif result.get("wells_json") is None:
        result["wells_json"] = json.dumps([])

    if isinstance(result.get("relationships_json"), dict):
        result["relationships_json"] = json.dumps(result["relationships_json"])
    else:
        result["relationships_json"] = json.dumps({})

    return result


def run(conn: sqlite3.Connection, structured_items: list[dict]) -> list[dict]:
    """
    Enrich all structured news items.
    Returns list of enriched dicts (also persisted to DB).
    """
    enriched = []

    for item in structured_items:
        news_id = item["id"]
        logger.info("Enriching news item %s", news_id)

        try:
            data = enrich_news_item(news_id, item)
            insert_enriched_news(conn, data)
            enriched.append({**item, **data})
        except Exception as e:
            logger.error("Failed to enrich news %s: %s", news_id, e)
            # Continue with un-enriched item — enrichment is additive
            enriched.append(item)

    logger.info("Enriched %d/%d news items", len(enriched), len(structured_items))
    return enriched
