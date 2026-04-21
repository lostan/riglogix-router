"""
Structuring stage — extract typed fields from raw news using LLM.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone

from llm.client import complete
from storage.db import insert_structured_news

logger = logging.getLogger(__name__)


def structure_news_item(news_id: str, title: str, body: str) -> dict:
    """Call LLM to extract structured fields from a single news item."""
    user_message = f"## Article Title\n{title}\n\n## Article Body\n{body}"

    result = complete("structure_news", user_message)

    result["news_id"] = news_id
    result["structured_at"] = datetime.now(timezone.utc).isoformat()

    # Normalize wells list to JSON string for storage
    if isinstance(result.get("wells"), list):
        result["wells"] = json.dumps(result["wells"])
    elif result.get("wells") is None:
        result["wells"] = json.dumps([])

    return result


def run(conn: sqlite3.Connection, news_items: list[dict]) -> list[dict]:
    """
    Structure all news items in this run.
    Returns list of structured dicts (also persisted to DB).
    """
    structured = []

    for item in news_items:
        news_id = item["id"]
        logger.info("Structuring news item %s: %s", news_id, item["title"][:60])

        try:
            data = structure_news_item(news_id, item["title"], item["body"])
            insert_structured_news(conn, data)
            structured.append({**item, **data})
        except Exception as e:
            logger.error("Failed to structure news %s: %s", news_id, e)

    logger.info("Structured %d/%d news items", len(structured), len(news_items))
    return structured
