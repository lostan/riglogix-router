"""
Routing stage — rank and filter signals into a final opportunity list for the digest.
"""

import json
import logging
import sqlite3

from config import settings
from storage.db import insert_signal

logger = logging.getLogger(__name__)


def route(
    conn: sqlite3.Connection,
    evaluated_signals: list[dict],
) -> list[dict]:
    """
    Persist evaluated signals, then select and rank the top opportunities.

    Only signals above the configured min_score threshold are included.
    Returns ranked list for digest composition.
    """
    pipeline_cfg = settings["pipeline"]
    min_score = pipeline_cfg.get("min_opportunity_score", 0.4)
    max_opps = pipeline_cfg.get("max_opportunities_in_digest", 5)

    # Persist all signals
    for sig in evaluated_signals:
        db_sig = {
            "news_id": sig["news_id"],
            "run_id": sig["run_id"],
            "product": sig["product"],
            "technical_fit": sig["technical_fit"],
            "timing_fit": sig.get("timing_fit", 0.0),
            "commercial_priority": sig.get("commercial_priority", 0.0),
            "composite_score": sig.get("composite_score", 0.0),
            "rationale": sig.get("rationale", "[]"),
            "uncertainty": sig.get("uncertainty", "medium"),
            "evaluated_at": sig.get("evaluated_at", ""),
        }
        sig["db_id"] = insert_signal(conn, db_sig)

    # Filter and rank
    eligible = [s for s in evaluated_signals if s.get("composite_score", 0.0) >= min_score]
    eligible.sort(key=lambda s: s["composite_score"], reverse=True)

    top = eligible[:max_opps]

    logger.info(
        "Routing: %d total signals, %d above threshold (%.2f), %d selected for digest",
        len(evaluated_signals),
        len(eligible),
        min_score,
        len(top),
    )

    return top
